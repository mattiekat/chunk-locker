pub mod s3;

use std::ops::Deref;
use std::path::PathBuf;

use crate::memory::{MemoryHandle, MemoryManager};
use crate::Hash;
use async_trait::async_trait;
use eyre::Result;
use serde::Deserialize;

use self::s3::S3Config;

#[async_trait]
pub trait StoreInit {
    async fn lock() -> eyre::Result<Box<dyn Store>>;
}

#[async_trait]
pub trait Store {
    async fn put(&self, hash: Hash, mem: MemoryHandle) -> Result<()>;
    async fn get(&self, hash: Hash) -> Result<MemoryHandle>;

    async fn get_db(&self) -> Result<()>;
    async fn put_db(&self) -> Result<()>;
}

#[derive(Deserialize, Debug)]
#[allow(unused)]
pub struct StoreConfig {
    s3: Option<S3Config>,
    fs_root_path: Option<String>,
}

pub struct FSStore {
    root: PathBuf,
}

impl FSStore {}

#[async_trait]
impl Store for FSStore {
    async fn put(&self, hash: Hash, mem: MemoryHandle) -> Result<()> {
        let mut path = self.root.clone();
        path.push(format!("{hash}"));

        let bytes = mem.deref();
        tokio::fs::write(&path, bytes).await?;
        Ok(())
    }

    async fn get(&self, hash: Hash) -> Result<MemoryHandle> {
        let mut path = self.root.clone();
        path.push(format!("{hash}"));

        let bytes = tokio::fs::read(path).await?;
        let mut memory = MemoryManager::new().alloc().await;

        memory.update_len(bytes.len());

        for (i, byte) in bytes.into_iter().enumerate() {
            memory[i] = byte;
        }

        Ok(memory)
    }

    async fn get_db(&self) -> Result<()> {
        todo!()
    }

    async fn put_db(&self) -> Result<()> {
        todo!()
    }
}
