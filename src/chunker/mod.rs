use crate::memory::MemoryHandle;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

mod fast_cdc;

trait Chunker {
    fn chunk<R: AsyncRead>(&self, stream: R) -> mpsc::Sender<MemoryHandle>;
}
