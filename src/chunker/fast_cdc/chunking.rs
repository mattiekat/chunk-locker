use std::fmt::{Debug, Formatter};
use std::mem;
use std::pin::Pin;

use crate::chunker::fast_cdc::consts::{GEAR, GEAR_LS, MASKS};
use crate::chunker::fast_cdc::StreamCdcConfig;
use crate::chunker::{fast_cdc, ChunkingError};
use crate::memory::{MemoryHandle, MemoryManager};
use async_stream::stream;
use bytes::BufMut;
use futures::Stream;
use tokio::io::{AsyncRead, AsyncReadExt};

/// The level for the normalized chunking used by FastCDC.
///
/// Normalized chunking "generates chunks whose sizes are normalized to a
/// specified region centered at the expected chunk size," as described in
/// section 4.4 of the FastCDC 2016 paper.
///
/// Note that lower levels of normalization will result in a larger range of
/// generated chunk sizes. It may be beneficial to widen the minimum/maximum
/// chunk size values given to the `FastCDC` constructor in that case.
///
/// Note that higher levels of normalization may result in the final chunk of
/// data being smaller than the minimum chunk size, which results in a hash
/// value of zero since no calculations are performed for sub-minimum chunks.
#[repr(u8)]
#[derive(Copy, Clone, Eq, PartialEq)]
#[allow(unused)]
pub enum Normalization {
    /// No chunk size normalization, produces a wide range of chunk sizes.
    Level0 = 0,
    /// Level 1 normalization, in which fewer chunks are outside of the desired range.
    Level1 = 1,
    /// Level 2 normalization, where most chunks are of the desired size.
    Level2 = 2,
    /// Level 3 normalization, nearly all chunks are the desired size.
    Level3 = 3,
}

impl Default for Normalization {
    fn default() -> Self {
        Self::Level1
    }
}

impl Normalization {
    /// Returns `(mask_s, mask_l)`
    pub(super) fn masks(self, avg_size: u32) -> (u64, u64) {
        let bits = fast_cdc::logarithm2(avg_size);
        let normalization = self as u32;
        let mask_s = MASKS[(bits + normalization) as usize];
        let mask_l = MASKS[(bits - normalization) as usize];
        (mask_s, mask_l)
    }
}

/// Represents a chunk returned from the StreamCdc iterator.
pub struct ChunkData {
    /// The gear hash value as of the end of the chunk.
    pub hash: u64,
    /// Starting byte position within the source.
    pub offset: u64,
    /// Source bytes contained in this chunk.
    pub data: MemoryHandle,
}

impl Debug for ChunkData {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ChunkData {{ hash: {}, offset: {}, length: {} }}",
            self.hash,
            self.offset,
            self.data.len()
        )
    }
}

/// The FastCDC chunker implementation from 2020 with streaming support.
///
/// Use `new` to construct an instance, and then iterate over the `ChunkData`s
/// via the `Iterator` trait.
///
/// Note that this struct allocates a `Vec<u8>` of `max_size` bytes to act as a
/// buffer when reading from the source and finding chunk boundaries.
///
/// ```no_run
/// # use std::fs::File;
/// # use fastcdc::v2020::StreamCdc;
/// let source = File::open("test/fixtures/SekienAkashita.jpg").unwrap();
/// let chunker = StreamCdc::new(Box::new(source), 4096, 16384, 65535);
/// for result in chunker {
///     let chunk = result.unwrap();
///     println!("offset={} length={}", chunk.offset, chunk.length);
/// }
/// ```
pub struct StreamCdc<S> {
    cfg: StreamCdcConfig,
    /// Buffer of data from source for finding cut points.
    buffer: MemoryHandle,
    /// Source from which data is read into `buffer`.
    source: S,
    /// Number of bytes read from the source so far.
    processed: u64,
    /// True when the source produces no more data.
    eof: bool,
}

impl<S> StreamCdc<S> {
    pub(super) async fn new(source: S, cfg: StreamCdcConfig) -> Self {
        Self {
            cfg,
            buffer: MemoryManager::new().alloc().await,
            source,
            eof: false,
            processed: 0,
        }
    }

    /// Find the next chunk cut point in the source.
    /// Returns `(hash, chunk_length)`
    #[allow(clippy::too_many_arguments)]
    fn cut(&self) -> (u64, usize) {
        let mut remaining = self.buffer.len();
        if remaining <= self.cfg.min_size {
            return (0, remaining);
        }
        let mut center = self.cfg.avg_size;
        if remaining > self.cfg.max_size {
            remaining = self.cfg.max_size;
        } else if remaining < center {
            center = remaining;
        }

        let mut hash: u64 = 0;
        for index in (self.cfg.min_size / 2)..(center / 2) {
            let a = index * 2;
            hash = (hash << 2).wrapping_add(GEAR_LS[self.buffer[a] as usize]);
            if (hash & self.cfg.mask_s_ls) == 0 {
                return (hash, a);
            }
            hash = hash.wrapping_add(GEAR[self.buffer[a + 1] as usize]);
            if (hash & self.cfg.mask_s) == 0 {
                return (hash, a + 1);
            }
        }
        for index in (center / 2)..(remaining / 2) {
            let a = index * 2;
            hash = (hash << 2).wrapping_add(GEAR_LS[self.buffer[a] as usize]);
            if (hash & self.cfg.mask_l_ls) == 0 {
                return (hash, a);
            }
            hash = hash.wrapping_add(GEAR[self.buffer[a + 1] as usize]);
            if (hash & self.cfg.mask_l) == 0 {
                return (hash, a + 1);
            }
        }
        // If all else fails, return the largest chunk. This will happen with
        // pathological data, such as all zeroes.
        (hash, remaining)
    }

    /// Returns the first `count` bytes and keeps the remaining in the internal buffer.
    ///
    /// Allocates a new internal buffer and returns the old one. It also copies over any data which
    /// was after the end of the chunk to the new buffer and clears it from the one which is
    /// returned. The returned buffer will contain only the chunk itself.
    async fn drain_bytes(&mut self, count: usize) -> MemoryHandle {
        assert!(count <= self.buffer.len());

        // allocate a new empty buffer
        let mut buffer = MemoryManager::new().alloc().await;
        // get the empty buffer into self
        mem::swap(&mut self.buffer, &mut buffer);
        // move over remainder of the old buffer after the chunk into the new buffer
        self.buffer.cursor_mut().put(buffer.cursor_from(count));
        // shrink the old buffer we are returning to just the size of its chunk
        buffer.truncate(count);
        buffer
    }
}

impl<S: AsyncRead + Unpin> StreamCdc<S> {
    pub fn into_stream(mut self) -> Pin<Box<impl Stream<Item = Result<ChunkData, ChunkingError>>>> {
        Box::pin(stream! {
            while let Some(slice) = self.read_chunk().await.transpose() {
                yield slice
            }
        })
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn stream<'a>(
        &'a mut self,
    ) -> Pin<Box<impl Stream<Item = Result<ChunkData, ChunkingError>> + 'a>> {
        Box::pin(stream! {
            while let Some(slice) = self.read_chunk().await.transpose() {
                yield slice
            }
        })
    }

    /// Fill the buffer with data from the source, returning the number of bytes
    /// read (zero if end of source has been reached).
    async fn fill_buffer(&mut self) -> Result<usize, ChunkingError> {
        let initial_size = self.buffer.len();
        let initial_capacity = self.buffer.max_len() - initial_size;
        let mut all_bytes_read = 0;
        let mut cursor = self.buffer.cursor_mut_from(self.buffer.len());
        while !self.eof && all_bytes_read < initial_capacity {
            let bytes_read = self.source.read_buf(&mut cursor).await?;
            self.eof |= bytes_read == 0;
            all_bytes_read += bytes_read;
        }
        debug_assert_eq!(self.buffer.len() - initial_size, all_bytes_read);
        Ok(all_bytes_read)
    }

    /// Find the next chunk in the source. If the end of the source has been
    /// reached, returns `Ok(None)`.
    pub(super) async fn read_chunk(&mut self) -> Result<Option<ChunkData>, ChunkingError> {
        if self.buffer.len() < self.cfg.max_size {
            // we might need more bytes for this chunk
            self.fill_buffer().await?;
        } else {
            // already have enough, save on the read ops
        }

        debug_assert!(
            self.eof || self.buffer.len() > 0,
            "We are not at end of file so we should have read bytes"
        );
        if self.buffer.len() == 0 {
            return Ok(None);
        }

        let (hash, count) = self.cut();
        if count == 0 {
            return Ok(None);
        }

        let offset = self.processed;
        self.processed += count as u64;
        let data = self.drain_bytes(count).await;
        Ok(Some(ChunkData { hash, offset, data }))
    }
}
