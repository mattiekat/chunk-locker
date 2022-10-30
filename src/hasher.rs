use async_trait::async_trait;
use std::io::Read;
use tokio::io::AsyncRead;
use crate::Hash;

#[async_trait]
trait Hasher {
    fn hash_stream<R: Read>(&self, stream: R) -> Hash;
    async fn hash_async_stream<R: AsyncRead>(&self, stream: R) -> Hash;
}
