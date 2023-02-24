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

use std::future::Future;

use futures::{FutureExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::mpsc;

use chunking::Normalization;
pub use chunking::*;
pub use consts::*;

use crate::chunker::{Chunker, ChunkingError};
use crate::memory::{MemoryHandle, MemoryManager};

mod chunking;
mod consts;
#[cfg(test)]
mod tests;

#[derive(Clone)]
struct StreamCdcConfig {
    min_size: usize,
    avg_size: usize,
    max_size: usize,
    mask_s: u64,
    mask_l: u64,
    mask_s_ls: u64,
    mask_l_ls: u64,
}

pub struct StreamCdcFactory(StreamCdcConfig);

impl StreamCdcFactory {
    /// Construct a `StreamCdc` that will process bytes from the given source.
    ///
    /// Uses chunk size normalization level 1 by default.
    pub fn new(min_size: u32, avg_size: u32, max_size: u32) -> Self {
        StreamCdcFactory::with_level(min_size, avg_size, max_size, Normalization::Level1)
    }

    /// Create a new `StreamCdc` with the given normalization level.
    pub fn with_level(min_size: u32, avg_size: u32, max_size: u32, level: Normalization) -> Self {
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

        assert!(
            max_size as usize <= MemoryManager::new().buffer_size(),
            "Maximum chunk size cannot exceed maximum buffer size"
        );

        let (mask_s, mask_l) = level.masks(avg_size);
        Self(StreamCdcConfig {
            min_size: min_size as usize,
            avg_size: avg_size as usize,
            max_size: max_size as usize,
            mask_s,
            mask_l,
            mask_s_ls: mask_s << 1,
            mask_l_ls: mask_l << 1,
        })
    }

    pub async fn make<S>(&self, stream: S) -> StreamCdc<S>
    where
        S: AsyncRead + Send + Unpin,
    {
        StreamCdc::new(stream, self.0.clone()).await
    }
}

impl Chunker for StreamCdcFactory {
    fn chunk<R>(&self, stream: R) -> mpsc::Receiver<Result<MemoryHandle, ChunkingError>>
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        let fut = StreamCdc::new(stream, self.0.clone());
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(stream_chunks(fut, tx));
        rx
    }
}

async fn stream_chunks<R>(
    fut: impl Future<Output = StreamCdc<R>>,
    tx: mpsc::Sender<Result<MemoryHandle, ChunkingError>>,
) where
    R: AsyncRead + Unpin,
{
    let mut stream = fut.await.into_stream();
    while let Some(chunk_res) = stream.next().await {
        match chunk_res {
            Ok(chunk) => {
                if let Err(e) = tx.send(Ok(chunk.data)).await {
                    // channel was closed
                    break;
                }
            }
            Err(chunk_error) => {
                if let Err(e) = tx.send(Err(chunk_error)).await {
                    // channel was closed
                    break;
                }
            }
        }
    }
}

/// Rounded base-2 logarithm function for unsigned 32-bit integers.
fn logarithm2(value: u32) -> u32 {
    (value as f32).log2().round() as u32
}
