//! This module is for reducing the overhead of allocating buffers for our data pipeline.
//! Using this also prevents a bottleneck in the pipeline from leading to increasingly more memory
//! allocations.

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::mem::{size_of, swap};
use std::ops::{Deref, DerefMut};
use std::ptr::null_mut;
use std::slice;

use bitvec::prelude::BitSlice;
use once_cell::sync::OnceCell;
use serde::Deserialize;
use tokio::sync::{Mutex, Notify};

use crate::config::Config;

#[derive(Deserialize, Debug)]
pub struct MemoryConfig {
    /// Number of buffers to allocate
    buffer_count: usize,
    /// Size of the buffers to allocate
    buffer_size: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            buffer_count: 16,
            buffer_size: 1024 * 1024 * 16,
        }
    }
}

// static NOTIFY: Notify = Notify::const_new();
static MANAGER: OnceCell<MemoryManagerData> = OnceCell::new();

struct MemoryManagerData {
    cfg: &'static MemoryConfig,
    /// Stored statically since the application should only ever have one set of buffers.
    buffers: *mut u8,
    /// True means a buffer is in use, false means it is not.
    allocations: Mutex<&'static mut BitSlice<usize>>,
    notify: Notify,
    buffers_layout: Layout,
    allocations_layout: Layout,
}

impl MemoryManagerData {
    fn new() -> Self {
        let cfg = &Config::load().memory;
        assert!(cfg.buffer_size >= 1024, "Buffers must be at least 1KiB");
        assert!(cfg.buffer_count >= 2, "There must be at least two buffers");

        const S: usize = size_of::<usize>();
        let buffers_layout = Layout::from_size_align(cfg.buffer_size * cfg.buffer_count, S)
            .expect("Invalid buffer size or count");
        let buffers = unsafe { alloc(buffers_layout) };

        let integers_needed = cfg.buffer_count / S + if cfg.buffer_size % S == 0 { 0 } else { 1 };
        let allocations_layout = Layout::from_size_align(integers_needed * S, S).unwrap();
        let raw_allocations = unsafe { alloc_zeroed(allocations_layout) } as *mut usize;
        let allocations = Mutex::const_new(BitSlice::from_slice_mut(unsafe {
            slice::from_raw_parts_mut(raw_allocations, integers_needed)
        }));

        Self {
            cfg,
            buffers,
            allocations,
            notify: Notify::const_new(),
            buffers_layout,
            allocations_layout,
        }
    }
}

impl Drop for MemoryManagerData {
    fn drop(&mut self) {
        unsafe {
            let mut t = null_mut();
            swap(&mut t, &mut self.buffers);
            dealloc(t, self.buffers_layout);

            let mut t = BitSlice::empty_mut();
            swap(&mut t, self.allocations.get_mut());
            dealloc(
                t.as_mut_bitptr().pointer() as *mut u8,
                self.allocations_layout,
            );
        }
    }
}

unsafe impl Send for MemoryManagerData {}
unsafe impl Sync for MemoryManagerData {}

#[derive(Copy, Clone)]
pub struct MemoryManager(&'static MemoryManagerData);

impl MemoryManager {
    /// Create a new handle to the memory manager.
    pub fn new() -> Self {
        // ensure it is initialized if a handle exists
        Self(MANAGER.get_or_init(MemoryManagerData::new))
    }

    /// Allocate
    pub async fn alloc(&self) -> MemoryHandle {
        loop {
            let mut allocs = self.0.allocations.lock().await;

            let first_zero = allocs.first_zero();
            if let Some(idx) = first_zero {
                // we found an unallocated buffer, claim it and make a handle to it
                unsafe {
                    // we know the size of the allocations must contain idx
                    allocs.set_unchecked(idx, true);
                }
                break MemoryHandle {
                    max_len: self.0.cfg.buffer_size,
                    len: 0,
                    allocation_idx: idx,
                    mem: unsafe {
                        // we defined the buffer as buffer_count * buffer_size and can just offset by a
                        // buffer size each time to get the next buffer
                        self.0.buffers.add(idx * self.0.cfg.buffer_size)
                    },
                };
            } else {
                // no buffer was available, unlock and then wait until we get notified that one is
                drop(allocs);
                self.0.notify.notified().await
            }
        }
    }

    async fn dealloc(&self, idx: usize) {
        // mark as unallocated and then notify there is now a buffer available
        let mut allocs = self.0.allocations.lock().await;
        unsafe { allocs.set_unchecked(idx, false) }
        self.0.notify.notify_one();
    }

    #[allow(unused)]
    pub fn buffer_size(&self) -> usize {
        self.0.cfg.buffer_size
    }
}

pub struct MemoryHandle {
    max_len: usize,
    len: usize,
    allocation_idx: usize,
    mem: *mut u8,
}

impl MemoryHandle {
    #[allow(unused)]
    pub(crate) fn max_len(&self) -> usize {
        self.max_len
    }

    pub(crate) fn update_len(&mut self, len: usize) {
        assert!(
            len <= self.max_len,
            "New length is too large for the configured buffer size"
        );
        unsafe { self.update_len_unchecked(len) }
    }

    pub(crate) unsafe fn update_len_unchecked(&mut self, len: usize) {
        self.len = len;
    }
}

impl Drop for MemoryHandle {
    fn drop(&mut self) {
        // make sure we free up this buffer
        let idx = self.allocation_idx;
        tokio::spawn(async move {
            MemoryManager::new().dealloc(idx).await;
        });
    }
}

impl Deref for MemoryHandle {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { slice::from_raw_parts(self.mem, self.len) }
    }
}

impl DerefMut for MemoryHandle {
    fn deref_mut(&mut self) -> &mut Self::Target {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { slice::from_raw_parts_mut(self.mem, self.len) }
    }
}

unsafe impl Send for MemoryHandle {}
