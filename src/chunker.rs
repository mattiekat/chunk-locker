use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use crate::memory::MemoryHandle;

trait Chunker {
    fn chunk<R: AsyncRead>(&self, stream: R) -> mpsc::Sender<MemoryHandle>;
}
