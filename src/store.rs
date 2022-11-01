use std::ops::Deref;
use std::path::PathBuf;

use crate::memory::BUFFER_SIZE;
use crate::memory::{MemoryHandle, MemoryManager};
use crate::Hash;
use async_trait::async_trait;
use aws_sdk_s3 as s3;
use bytes::Bytes;
use eyre::Result;
use s3::model::CompletedPart;
use s3::output::CreateMultipartUploadOutput;
use s3::types::ByteStream;

#[async_trait]
trait StoreInit {
    async fn lock() -> eyre::Result<Box<dyn Store>>;
}

#[async_trait]
trait Store {
    async fn put(&self, hash: Hash, mem: MemoryHandle) -> Result<()>;
    async fn get(&self, hash: Hash) -> Result<MemoryHandle>;

    async fn get_db(&self) -> Result<()>;
    async fn put_db(&self) -> Result<()>;
}

struct FSStore {
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

        assert!(bytes.len() < BUFFER_SIZE);
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

struct S3Store {
    client: s3::Client,
    base_url: String,
    bucket_name: String,
}

impl S3Store {
    pub(crate) fn new() -> Self {
        Self {
            client: todo!(),
            base_url: todo!(),
            bucket_name: todo!(),
        }
    }
}

#[async_trait]
impl Store for S3Store {
    async fn put(&self, hash: Hash, mem: MemoryHandle) -> Result<()> {
        let key = format!("{hash}");
        let multipart_upload_res: CreateMultipartUploadOutput = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&key)
            .send()
            .await
            .unwrap();
        let upload_id = multipart_upload_res
            .upload_id()
            .expect("Failed to start multipart upload");

        let mut upload_parts: Vec<CompletedPart> = Vec::new();

        let file_size = mem.len() as usize;
        const CHUNK_SIZE: usize = 5 * 1024 * 1024;

        let mut chunk_count = (file_size / CHUNK_SIZE) + 1;
        let mut size_of_last_chunk = file_size % CHUNK_SIZE;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = CHUNK_SIZE;
            chunk_count -= 1;
        }

        let bytes = mem.deref();

        for chunk_index in 0..chunk_count {
            let this_chunk = if chunk_count - 1 == chunk_index {
                size_of_last_chunk
            } else {
                CHUNK_SIZE
            };

            let bytes = &bytes[chunk_index * CHUNK_SIZE..this_chunk];
            let stream = ByteStream::from(Bytes::from(bytes));

            // Chunk index needs to start at 0, but part numbers start at 1.
            let part_number = (chunk_index as i32) + 1;
            let upload_part_res = self
                .client
                .upload_part()
                .key(&key)
                .bucket(&self.bucket_name)
                .upload_id(upload_id)
                .body(stream)
                .part_number(part_number)
                .send()
                .await?;
            upload_parts.push(
                CompletedPart::builder()
                    .e_tag(upload_part_res.e_tag.unwrap_or_default())
                    .part_number(part_number)
                    .build(),
            );
        }

        Ok(())
    }
    async fn get(&self, hash: Hash) -> Result<MemoryHandle> {
        let bytes = reqwest::get(format!("{}/{}", self.base_url, hash))
            .await?
            .bytes()
            .await?;

        let mut handle = MemoryManager::new().alloc().await;
        let len = bytes.len();
        assert!(BUFFER_SIZE > len);
        for (i, byte) in bytes.into_iter().enumerate() {
            handle[i] = byte;
        }

        handle.len = len;
        Ok(handle)
    }

    async fn get_db(&self) -> eyre::Result<()> {
        todo!()
    }
    async fn put_db(&self) -> eyre::Result<()> {
        todo!()
    }
}
