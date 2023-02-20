// Modified from https://github.com/nlfiedler/fastcdc-rs/blob/bcf405d6066185fa65d3c6ed8195568842f76a6f/src/v2020/mod.rs

//! This module implements the canonical FastCDC algorithm as described in the
//! [paper](https://ieeexplore.ieee.org/document/9055082) by Wen Xia, et al., in
//! 2020.
//!
//! The algorithm incorporates a simplified hash judgement using the fast Gear
//! hash, sub-minimum chunk cut-point skipping, normalized chunking to produce
//! chunks of a more consistent length, and "rolling two bytes each time".
//! According to the authors, this should be 30-40% faster than the 2016 version
//! while producing the same cut points.
//!
//! There are two ways in which to use the `FastCDC` struct defined in this
//! module. One is to simply invoke `cut()` while managing your own `start` and
//! `remaining` values. The other is to use the struct as an `Iterator` that
//! yields `Chunk` structs which represent the offset and size of the chunks.
//! Note that attempting to use both `cut()` and `Iterator` on the same
//! `FastCDC` instance will yield incorrect results.
//!
//! Note that the `cut()` function returns the 64-bit hash of the chunk, which
//! may be useful in scenarios involving chunk size prediction using historical
//! data, such as in RapidCDC or SuperCDC. This hash value is also given in the
//! `hash` field of the `Chunk` struct. While this value has rather low entropy,
//! it is computationally cost-free and can be put to some use with additional
//! record keeping.
//!
//! The `StreamCdc` implementation is similar to `FastCDC` except that it will
//! read data from a boxed `Read` into an internal buffer of `max_size` and
//! produce `ChunkData` values from the `Iterator`.
use async_stream::stream;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_stream::Stream;

use crate::memory::{MemoryHandle, MemoryManager};
pub use consts::*;

mod consts;
#[cfg(test)]
mod tests;

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

/// The error type returned from the `StreamCdc` iterator.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An I/O error occurred.
    #[error("IO Error: {0:?}")]
    IoError(#[from] std::io::Error),
    #[error("Chunker Error: {0}")]
    Other(String),
}

/// Represents a chunk returned from the StreamCdc iterator.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ChunkData {
    /// The gear hash value as of the end of the chunk.
    pub hash: u64,
    /// Starting byte position within the source.
    pub offset: u64,
    /// Length of the chunk in bytes.
    pub length: usize,
    /// Source bytes contained in this chunk.
    pub data: Vec<u8>,
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
    /// Buffer of data from source for finding cut points.
    buffer: MemoryHandle,
    /// Maximum capacity of the buffer (always `max_size`).
    capacity: usize,
    /// Number of relevant bytes in the `buffer`.
    length: usize,
    /// Source from which data is read into `buffer`.
    source: S,
    /// Number of bytes read from the source so far.
    processed: u64,
    /// True when the source produces no more data.
    eof: bool,
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
}

impl<S> StreamCdc<S> {
    /// Construct a `StreamCdc` that will process bytes from the given source.
    ///
    /// Uses chunk size normalization level 1 by default.
    pub async fn new(source: S, min_size: u32, avg_size: u32, max_size: u32) -> Self {
        StreamCdc::with_level(source, min_size, avg_size, max_size, Normalization::Level1)
    }

    /// Create a new `StreamCdc` with the given normalization level.
    pub async fn with_level(
        source: S,
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        level: Normalization,
    ) -> Self {
        assert!(min_size >= MINIMUM_MIN, "Minimum chunk size is too small");
        assert!(min_size <= MINIMUM_MAX, "Minimum chunk size is too large");
        assert!(avg_size >= AVERAGE_MIN, "Average chunk size is too small");
        assert!(avg_size <= AVERAGE_MAX, "Average chunk size is too large");
        assert!(max_size >= MAXIMUM_MIN, "Maximum chunk size is too small");
        assert!(max_size <= MAXIMUM_MAX, "Maximum chunk size is too large");
        assert!(
            min_size <= avg_size,
            "Average size must be greater than the minimum size"
        );
        assert!(
            avg_size <= max_size,
            "Maximum size must be greater than the average size"
        );

        let buffer = MemoryManager::new().alloc().await;
        assert!(
            max_size as usize <= buffer.max_len(),
            "Maximum chunk size cannot exceed maximum buffer size"
        );

        let bits = logarithm2(avg_size);
        let normalization = level as u32;
        let mask_s = MASKS[(bits + normalization) as usize];
        let mask_l = MASKS[(bits - normalization) as usize];
        Self {
            buffer,
            capacity: max_size as usize,
            length: 0,
            source,
            eof: false,
            processed: 0,
            min_size: min_size as usize,
            avg_size: avg_size as usize,
            max_size: max_size as usize,
            mask_s,
            mask_l,
            mask_s_ls: mask_s << 1,
            mask_l_ls: mask_l << 1,
        }
    }

    /// Find the next chunk cut point in the source.
    #[allow(clippy::too_many_arguments)]
    fn cut(&self) -> (u64, usize) {
        let mut remaining = self.length;
        if remaining <= self.min_size {
            return (0, remaining);
        }
        let mut center = self.avg_size;
        if remaining > self.max_size {
            remaining = self.max_size;
        } else if remaining < center {
            center = remaining;
        }

        let mut hash: u64 = 0;
        for index in (self.min_size / 2)..(center / 2) {
            let a = index * 2;
            hash = (hash << 2).wrapping_add(GEAR_LS[self.buffer[a] as usize]);
            if (hash & self.mask_s_ls) == 0 {
                return (hash, a);
            }
            hash = hash.wrapping_add(GEAR[self.buffer[a + 1] as usize]);
            if (hash & self.mask_s) == 0 {
                return (hash, a + 1);
            }
        }
        for index in (center / 2)..(remaining / 2) {
            let a = index * 2;
            hash = (hash << 2).wrapping_add(GEAR_LS[self.buffer[a] as usize]);
            if (hash & self.mask_l_ls) == 0 {
                return (hash, a);
            }
            hash = hash.wrapping_add(GEAR[self.buffer[a + 1] as usize]);
            if (hash & self.mask_l) == 0 {
                return (hash, a + 1);
            }
        }
        // If all else fails, return the largest chunk. This will happen with
        // pathological data, such as all zeroes.
        (hash, remaining)
    }

    /// Drains a specified number of bytes from the buffer, then resizes the
    /// buffer back to `capacity` size in preparation for further reads.
    fn drain_bytes(&mut self, count: usize) -> Result<Vec<u8>, Error> {
        // this code originally copied from asuran crate
        if count > self.length {
            Err(Error::Other(format!(
                "drain_bytes() called with count larger than length: {} > {}",
                count, self.length
            )))
        } else {
            let data = self.buffer.drain(..count).collect::<Vec<u8>>();
            self.length -= count;
            self.buffer.resize(self.capacity, 0_u8);
            Ok(data)
        }
    }
}

impl<S: AsyncRead + Unpin> StreamCdc<S> {
    pub fn into_stream(mut self) -> Pin<Box<impl Stream<Item = Result<ChunkData, Error>>>> {
        Box::pin(stream! {
            while let Some(slice) = self.read_chunk().await.transpose() {
                yield slice
            }
        })
    }

    pub fn stream<'a>(&'a mut self) -> Pin<Box<impl Stream<Item = Result<ChunkData, Error>> + 'a>> {
        Box::pin(stream! {
            while let Some(slice) = self.read_chunk().await.transpose() {
                yield slice
            }
        })
    }

    /// Fill the buffer with data from the source, returning the number of bytes
    /// read (zero if end of source has been reached).
    async fn fill_buffer(&mut self) -> Result<usize, Error> {
        // this code originally copied from asuran crate
        if self.eof {
            Ok(0)
        } else {
            let mut all_bytes_read = 0;
            while !self.eof && self.length < self.capacity {
                let bytes_read = self.source.read(&mut self.buffer[self.length..]).await?;
                if bytes_read == 0 {
                    self.eof = true;
                } else {
                    self.length += bytes_read;
                    all_bytes_read += bytes_read;
                }
            }
            Ok(all_bytes_read)
        }
    }

    /// Find the next chunk in the source. If the end of the source has been
    /// reached, returns `Error::Empty` as the error.
    async fn read_chunk(&mut self) -> Result<Option<ChunkData>, Error> {
        self.fill_buffer().await?;
        if self.length == 0 {
            Ok(None)
        } else {
            let (hash, count) = self.cut();
            if count == 0 {
                Ok(None)
            } else {
                let offset = self.processed;
                self.processed += count as u64;
                let data = self.drain_bytes(count)?;
                Ok(Some(ChunkData {
                    hash,
                    offset,
                    length: count,
                    data,
                }))
            }
        }
    }
}

/// Rounded base-2 logarithm function for unsigned 32-bit integers.
fn logarithm2(value: u32) -> u32 {
    (value as f32).log2().round() as u32
}
