use eyre::Result;
use std::ops::Deref;

use crate::{
    memory::{MemoryManager, BUFFER_SIZE},
    store::{S3Store, Store},
};

mod chunker;
mod compressor;
mod config;
mod db;
mod encryptor;
mod hasher;
mod memory;
mod signer;
mod store;

type Hash = u128;

async fn example_s3store() {
    let store = S3Store::new().await;

    let manager = MemoryManager::new();
    let mut handle = manager.alloc().await;

    // Write some garbage into the handle
    let text = b"Hello chunk-locker!";
    let text_len = text.len();

    for i in 0..(BUFFER_SIZE.checked_div(text_len).unwrap()) {
        handle.len += text_len;
        let bytes = &mut *handle;
        let range = bytes.len() - text_len..bytes.len();
        bytes[range].copy_from_slice(text);
    }

    println!("Uploading to s3");
    let hash = Hash::default();
    store.put(hash, handle).await.unwrap();

    println!("Downloading from s3");
    // try get it back down
    let handle = store.get(hash).await.unwrap();
    let bytes = &*handle;
    println!("{}", String::from_utf8(bytes[..100].to_vec()).unwrap())
}

#[tokio::main]
async fn main() {
    // TODO: CLI
    // TODO: TUI
    println!("Hello, world!");

    // example_s3store()
}
