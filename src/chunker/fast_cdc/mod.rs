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
//! The `StreamCDC` implementation is similar to `FastCDC` except that it will
//! read data from a boxed `Read` into an internal buffer of `max_size` and
//! produce `ChunkData` values from the `Iterator`.
use std::fmt;
use std::io::Read;

pub use consts::*;

mod consts;
#[cfg(test)]
mod tests;

/// Find the next chunk cut point in the source.
#[allow(clippy::too_many_arguments)]
fn cut(
    source: &[u8],
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
) -> (u64, usize) {
    let mut remaining = source.len();
    if remaining <= min_size {
        return (0, remaining);
    }
    let mut center = avg_size;
    if remaining > max_size {
        remaining = max_size;
    } else if remaining < center {
        center = remaining;
    }
    let mut index = min_size / 2;
    let mut hash: u64 = 0;
    while index < center / 2 {
        let a = index * 2;
        hash = (hash << 2).wrapping_add(GEAR_LS[source[a] as usize]);
        if (hash & mask_s_ls) == 0 {
            return (hash, a);
        }
        hash = hash.wrapping_add(GEAR[source[a + 1] as usize]);
        if (hash & mask_s) == 0 {
            return (hash, a + 1);
        }
        index += 1;
    }
    while index < remaining / 2 {
        let a = index * 2;
        hash = (hash << 2).wrapping_add(GEAR_LS[source[a] as usize]);
        if (hash & mask_l_ls) == 0 {
            return (hash, a);
        }
        hash = hash.wrapping_add(GEAR[source[a + 1] as usize]);
        if (hash & mask_l) == 0 {
            return (hash, a + 1);
        }
        index += 1;
    }
    // If all else fails, return the largest chunk. This will happen with
    // pathological data, such as all zeroes.
    (hash, remaining)
}

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
pub enum Normalization {
    /// No chunk size normalization, produces a wide range of chunk sizes.
    Level0,
    /// Level 1 normalization, in which fewer chunks are outside of the desired range.
    Level1,
    /// Level 2 normalization, where most chunks are of the desired size.
    Level2,
    /// Level 3 normalization, nearly all chunks are the desired size.
    Level3,
}

impl Normalization {
    fn bits(&self) -> u32 {
        match self {
            Normalization::Level0 => 0,
            Normalization::Level1 => 1,
            Normalization::Level2 => 2,
            Normalization::Level3 => 3,
        }
    }
}

/// Represents a chunk returned from the FastCDC iterator.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct Chunk {
    /// The gear hash value as of the end of the chunk.
    pub hash: u64,
    /// Starting byte position within the source.
    pub offset: usize,
    /// Length of the chunk in bytes.
    pub length: usize,
}

/// The FastCDC chunker implementation from 2020.
///
/// Use `new` to construct an instance, and then iterate over the `Chunk`s via
/// the `Iterator` trait.
///
/// This example reads a file into memory and splits it into chunks that are
/// roughly 16 KB in size. The minimum and maximum sizes are the absolute limit
/// on the returned chunk sizes. With this algorithm, it is helpful to be more
/// lenient on the maximum chunk size as the results are highly dependent on the
/// input data. Changing the minimum chunk size will affect the results as the
/// algorithm may find different cut points given it uses the minimum as a
/// starting point (cut-point skipping).
///
/// ```no_run
/// use std::fs;
/// use fastcdc::v2020;
/// let contents = fs::read("test/fixtures/SekienAkashita.jpg").unwrap();
/// let chunker = v2020::FastCDC::new(&contents, 8192, 16384, 65535);
/// for entry in chunker {
///     println!("offset={} size={}", entry.offset, entry.length);
/// }
/// ```
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FastCDC<'a> {
    source: &'a [u8],
    processed: usize,
    remaining: usize,
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
}

impl<'a> FastCDC<'a> {
    /// Construct a `FastCDC` that will process the given slice of bytes.
    ///
    /// Uses chunk size normalization level 1 by default.
    pub fn new(source: &'a [u8], min_size: u32, avg_size: u32, max_size: u32) -> Self {
        FastCDC::with_level(source, min_size, avg_size, max_size, Normalization::Level1)
    }

    /// Create a new `FastCDC` with the given normalization level.
    pub fn with_level(
        source: &'a [u8],
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        level: Normalization,
    ) -> Self {
        assert!(min_size >= MINIMUM_MIN);
        assert!(min_size <= MINIMUM_MAX);
        assert!(avg_size >= AVERAGE_MIN);
        assert!(avg_size <= AVERAGE_MAX);
        assert!(max_size >= MAXIMUM_MIN);
        assert!(max_size <= MAXIMUM_MAX);
        let bits = logarithm2(avg_size);
        let normalization = level.bits();
        let mask_s = MASKS[(bits + normalization) as usize];
        let mask_l = MASKS[(bits - normalization) as usize];
        Self {
            source,
            processed: 0,
            remaining: source.len(),
            min_size: min_size as usize,
            avg_size: avg_size as usize,
            max_size: max_size as usize,
            mask_s,
            mask_l,
            mask_s_ls: mask_s << 1,
            mask_l_ls: mask_l << 1,
        }
    }

    /// Find the next cut point in the data, where `start` is the position from
    /// which to start processing the source data, and `remaining` are the
    /// number of bytes left to be processed.
    ///
    /// The returned 2-tuple consists of the 64-bit hash (fingerprint) and the
    /// byte offset of the end of the chunk. Note that the hash values may
    /// differ from those produced by the v2016 chunker.
    ///
    /// There is a special case in which the remaining bytes are less than the
    /// minimum chunk size, at which point this function returns a hash of 0 and
    /// the cut point is the end of the source data.
    pub fn cut(&self, start: usize, remaining: usize) -> (u64, usize) {
        let end = start + remaining;
        let (hash, count) = cut(
            &self.source[start..end],
            self.min_size,
            self.avg_size,
            self.max_size,
            self.mask_s,
            self.mask_l,
            self.mask_s_ls,
            self.mask_l_ls,
        );
        (hash, start + count)
    }
}

impl<'a> Iterator for FastCDC<'a> {
    type Item = Chunk;

    fn next(&mut self) -> Option<Chunk> {
        if self.remaining == 0 {
            None
        } else {
            let (hash, cutpoint) = self.cut(self.processed, self.remaining);
            if cutpoint == 0 {
                None
            } else {
                let offset = self.processed;
                let length = cutpoint - offset;
                self.processed += length;
                self.remaining -= length;
                Some(Chunk {
                    hash,
                    offset,
                    length,
                })
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // NOTE: This intentionally returns the upper bound for both `size_hint`
        // values, as the upper bound doesn't actually seem to get used by `std`
        // and using the actual lower bound is practically guaranteed to require
        // a second capacity growth.
        let upper_bound = self.source.len() / self.min_size;
        (upper_bound, Some(upper_bound))
    }
}

/// The error type returned from the `StreamCDC` iterator.
#[derive(Debug)]
pub enum Error {
    /// End of source data reached.
    Empty,
    /// An I/O error occurred.
    IoError(std::io::Error),
    /// Something unexpected happened.
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "chunker error: {self:?}")
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IoError(error)
    }
}

/// Represents a chunk returned from the StreamCDC iterator.
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
/// # use fastcdc::v2020::StreamCDC;
/// let source = File::open("test/fixtures/SekienAkashita.jpg").unwrap();
/// let chunker = StreamCDC::new(Box::new(source), 4096, 16384, 65535);
/// for result in chunker {
///     let chunk = result.unwrap();
///     println!("offset={} length={}", chunk.offset, chunk.length);
/// }
/// ```
pub struct StreamCDC {
    /// Buffer of data from source for finding cut points.
    buffer: Vec<u8>,
    /// Maximum capacity of the buffer (always `max_size`).
    capacity: usize,
    /// Number of relevant bytes in the `buffer`.
    length: usize,
    /// Source from which data is read into `buffer`.
    source: Box<dyn Read>,
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

impl StreamCDC {
    /// Construct a `StreamCDC` that will process bytes from the given source.
    ///
    /// Uses chunk size normalization level 1 by default.
    pub fn new(source: Box<dyn Read>, min_size: u32, avg_size: u32, max_size: u32) -> Self {
        StreamCDC::with_level(source, min_size, avg_size, max_size, Normalization::Level1)
    }

    /// Create a new `StreamCDC` with the given normalization level.
    pub fn with_level(
        source: Box<dyn Read>,
        min_size: u32,
        avg_size: u32,
        max_size: u32,
        level: Normalization,
    ) -> Self {
        assert!(min_size >= MINIMUM_MIN);
        assert!(min_size <= MINIMUM_MAX);
        assert!(avg_size >= AVERAGE_MIN);
        assert!(avg_size <= AVERAGE_MAX);
        assert!(max_size >= MAXIMUM_MIN);
        assert!(max_size <= MAXIMUM_MAX);
        let bits = logarithm2(avg_size);
        let normalization = level.bits();
        let mask_s = MASKS[(bits + normalization) as usize];
        let mask_l = MASKS[(bits - normalization) as usize];
        Self {
            buffer: vec![0_u8; max_size as usize],
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

    /// Fill the buffer with data from the source, returning the number of bytes
    /// read (zero if end of source has been reached).
    fn fill_buffer(&mut self) -> Result<usize, Error> {
        // this code originally copied from asuran crate
        if self.eof {
            Ok(0)
        } else {
            let mut all_bytes_read = 0;
            while !self.eof && self.length < self.capacity {
                let bytes_read = self.source.read(&mut self.buffer[self.length..])?;
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

    /// Find the next chunk in the source. If the end of the source has been
    /// reached, returns `Error::Empty` as the error.
    fn read_chunk(&mut self) -> Result<ChunkData, Error> {
        self.fill_buffer()?;
        if self.length == 0 {
            Err(Error::Empty)
        } else {
            let (hash, count) = cut(
                &self.buffer[..self.length],
                self.min_size,
                self.avg_size,
                self.max_size,
                self.mask_s,
                self.mask_l,
                self.mask_s_ls,
                self.mask_l_ls,
            );
            if count == 0 {
                Err(Error::Empty)
            } else {
                let offset = self.processed;
                self.processed += count as u64;
                let data = self.drain_bytes(count)?;
                Ok(ChunkData {
                    hash,
                    offset,
                    length: count,
                    data,
                })
            }
        }
    }
}

impl Iterator for StreamCDC {
    type Item = Result<ChunkData, Error>;

    fn next(&mut self) -> Option<Result<ChunkData, Error>> {
        let slice = self.read_chunk();
        if let Err(Error::Empty) = slice {
            None
        } else {
            Some(slice)
        }
    }
}

/// Base-2 logarithm function for unsigned 32-bit integers.
fn logarithm2(value: u32) -> u32 {
    (value as f32).log2().round() as u32
}
