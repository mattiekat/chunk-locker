//! This module is for reducing the overhead of allocating buffers for our data pipeline.
//! Using this also prevents a bottleneck in the pipeline from leading to increasingly more memory
//! allocations.

use itertools::Itertools;
use std::ops::{Deref, DerefMut};
use tokio::sync::{Mutex, Notify};

/// Number of buffers to allocate
const BUFFER_COUNT: usize = 16;
/// Size of the buffers to allocate
pub const BUFFER_SIZE: usize = 1024 * 1024 * 16;

/// Stored statically since the application should only ever have one set of buffers.
static BUFFERS: [[u8; BUFFER_SIZE]; BUFFER_COUNT] = [[0; BUFFER_SIZE]; BUFFER_COUNT];
/// True means a buffer is in use, false means it is not.
static ALLOCATIONS: Mutex<[bool; BUFFER_COUNT]> = Mutex::const_new([false; BUFFER_COUNT]);
static NOTIFY: Notify = Notify::const_new();

#[derive(Copy, Clone)]
pub struct MemoryManager;

impl MemoryManager {
    /// Create a new handle to the memory manager.
    pub const fn new() -> Self {
        Self
    }

    /// Allocate
    pub async fn alloc(&self) -> MemoryHandle {
        loop {
            let mut allocs = ALLOCATIONS.lock().await;
            if let Some((idx, _)) = allocs.iter().find_position(|i| !(**i)) {
                // we found an unallocated buffer, claim it and make a handle to it
                allocs[idx] = true;
                break MemoryHandle {
                    len: 0,
                    allocation_idx: idx,
                    mem: BUFFERS[idx].as_ptr() as *mut [u8; BUFFER_SIZE],
                };
            } else {
                // no buffer was available, unlock and then wait until we get notified that one is
                drop(allocs);
                NOTIFY.notified().await
            }
        }
    }
}

pub struct MemoryHandle {
    pub(crate) len: usize,
    allocation_idx: usize,
    mem: *mut [u8; BUFFER_SIZE],
}

impl Drop for MemoryHandle {
    fn drop(&mut self) {
        let idx = self.allocation_idx;
        tokio::spawn(async move {
            // mark as unallocated and then notify there is now a buffer available
            ALLOCATIONS.lock().await[idx] = false;
            NOTIFY.notify_one();
        });
    }
}

impl Deref for MemoryHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { &(*self.mem)[0..self.len] }
    }
}

impl DerefMut for MemoryHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { &mut ((*self.mem)[0..self.len]) }
    }
}

unsafe impl Send for MemoryHandle {}
