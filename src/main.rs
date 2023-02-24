// TODO: remove this once we start hooking things up
#![allow(unused)]

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

/// A mutex to prevent tests which use static variables from conflicting with each other.
#[cfg(test)]
static STATIC_TEST_MUTEX: parking_lot::ReentrantMutex<()> = parking_lot::const_reentrant_mutex(());
