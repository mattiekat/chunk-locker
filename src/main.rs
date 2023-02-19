extern crate core;

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

#[tokio::main]
async fn main() {
    // TODO: CLI
    // TODO: TUI
    println!("Hello, world!");

    // store::s3::example_s3store().await;
}
