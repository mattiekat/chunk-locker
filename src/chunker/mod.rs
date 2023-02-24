use crate::memory::MemoryHandle;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

mod fast_cdc;

pub trait Chunker {
    fn chunk<R>(&self, stream: R) -> mpsc::Receiver<Result<MemoryHandle, ChunkingError>>
    where
        R: AsyncRead + Send + Unpin + 'static;
}

#[derive(Debug, thiserror::Error)]
pub enum ChunkingError {
    /// An I/O error occurred.
    #[error("IO Error: {0:?}")]
    IoError(#[from] std::io::Error),
}
