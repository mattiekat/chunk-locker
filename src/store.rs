use async_trait::async_trait;
use crate::memory::MemoryHandle;

#[async_trait]
trait StoreInit {
    async fn lock() -> eyre::Result<Box<dyn Store>>;
}

#[async_trait]
trait Store {
    async fn put(&self, mem: MemoryHandle);
    async fn get(&self) -> MemoryHandle;

    async fn get_db(&self) -> eyre::Result<()>;
    async fn put_db(&self) -> eyre::Result<()>;
}
