use std::ops::Deref;
use std::path::PathBuf;

use crate::config::Config;
use crate::memory::BUFFER_SIZE;
use crate::memory::{MemoryHandle, MemoryManager};
use crate::Hash;
use async_trait::async_trait;
use aws_sdk_s3 as s3;
use bytes::Bytes;
use eyre::Result;
use s3::model::{CompletedMultipartUpload, CompletedPart};
use s3::output::CreateMultipartUploadOutput;
use s3::types::ByteStream;
use s3::Region;

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

        memory.len = bytes.len();
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

pub struct S3Store {
    client: s3::Client,
    base_url: String,
    bucket_name: String,
    upload_chunk_size: usize,
}

impl S3Store {
    pub(crate) async fn new() -> Self {
        let config = Config::load();

        let creds = s3::Credentials::new(
            &config.store.s3_key_id,
            &config.store.s3_application_key,
            None,
            None,
            "chunk-locker",
        );
        let endpoint = s3::Endpoint::immutable(config.store.s3_endpoint.parse().unwrap());
        let region = config.store.s3_region.clone();

        let client = s3::Client::new(
            &aws_config::from_env()
                .credentials_provider(creds)
                .endpoint_resolver(endpoint)
                .region(Region::new(region))
                .load()
                .await,
        );

        Self {
            client,
            base_url: config.store.s3_download_url.clone(),
            bucket_name: config.store.s3_bucket.clone(),
            upload_chunk_size: config.store.s3_upload_chunk_size,
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
        let chunk_size = self.upload_chunk_size;

        let mut chunk_count = (file_size / chunk_size) + 1;
        let mut size_of_last_chunk = file_size % chunk_size;
        if size_of_last_chunk == 0 {
            size_of_last_chunk = chunk_size;
            chunk_count -= 1;
        }

        println!("Uploading {chunk_count} chunks....");

        let bytes = mem.deref();
        let total_len = mem.len();

        for chunk_index in 0..chunk_count {
            let this_chunk_size = if chunk_count - 1 == chunk_index {
                size_of_last_chunk
            } else {
                chunk_size
            };

            // TODO(emily): Figure out how best to not copy this memory around
            // MemoryHandle probably wants to point to a bytes::Bytes structure
            let bytes = bytes
                [chunk_index * chunk_size..(chunk_index * chunk_size) + this_chunk_size]
                .to_vec();
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

            println!(
                "Uploaded {chunk_index} of {chunk_count} ({} of {})",
                (chunk_index + 1) * chunk_size,
                total_len
            );
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let _complete_multipart_upload_res = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&key)
            .multipart_upload(completed_multipart_upload)
            .upload_id(upload_id)
            .send()
            .await
            .unwrap();

        println!("Completed upload");

        Ok(())
    }
    async fn get(&self, hash: Hash) -> Result<MemoryHandle> {
        let bytes = reqwest::get(format!("{}{}", self.base_url, hash))
            .await?
            .bytes()
            .await?;

        let mut handle = MemoryManager::new().alloc().await;
        let len = bytes.len();
        assert!(BUFFER_SIZE > len);
        handle.len = len;
        handle.copy_from_slice(&bytes);
        Ok(handle)
    }

    async fn get_db(&self) -> eyre::Result<()> {
        todo!()
    }
    async fn put_db(&self) -> eyre::Result<()> {
        todo!()
    }
}
