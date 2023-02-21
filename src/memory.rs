//! This module is for reducing the overhead of allocating buffers for our data pipeline.
//! Using this also prevents a bottleneck in the pipeline from leading to increasingly more memory
//! allocations.

use std::alloc::{alloc, alloc_zeroed, dealloc, Layout};
use std::mem::{size_of, swap};
use std::ptr::null_mut;
use std::{cmp, slice};
use std::ops::{Index, IndexMut};

use bitvec::prelude::BitSlice;
use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut};
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

        let integers_needed = cfg.buffer_count / S + if cfg.buffer_count % S == 0 { 0 } else { 1 };
        let allocations_layout = Layout::from_size_align(integers_needed * S, S).unwrap();
        let allocations = unsafe {
            let raw_allocations = alloc_zeroed(allocations_layout) as *mut usize;
            let slice_allocations = slice::from_raw_parts_mut(raw_allocations, integers_needed);
            let allocations = BitSlice::from_slice_mut(slice_allocations);
            Mutex::const_new(allocations)
        };

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

            let idx = unsafe {
                let first_zero = allocs.first_zero();
                if first_zero.is_none() || first_zero.unwrap_unchecked() >= self.0.cfg.buffer_count
                {
                    // no buffer was available, unlock and then wait until we get notified that one is
                    drop(allocs);
                    self.0.notify.notified().await;
                    continue;
                };
                first_zero.unwrap_unchecked()
            };

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
        }
    }

    #[allow(unused)]
    pub async fn allocations(&self) -> usize {
        let allocs = self.0.allocations.lock().await;
        allocs.count_ones()
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
    pub const fn max_len(&self) -> usize {
        self.max_len
    }

    pub const fn len(&self) -> usize {
        self.len
    }

    /// Unsafe because it may be going into uninitialized memory.
    pub(crate) unsafe fn update_len(&mut self, len: usize) {
        assert!(
            len <= self.max_len,
            "New length is too large for the configured buffer size"
        );
        unsafe { self.update_len_unchecked(len) }
    }

    /// Unsafe because it may be going into uninitialized memory and does not validate the bounds.
    pub(crate) unsafe fn update_len_unchecked(&mut self, len: usize) {
        self.len = len;
    }

    /// Shrink this memory handle to a new length. Will panic if `len` is greater than the current
    /// length of the buffer.
    pub fn truncate(&mut self, len: usize) {
        assert!(len <= self.len);
        unsafe { self.update_len_unchecked(len) }
    }

    pub const fn as_slice(&self) -> &[u8] {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { slice::from_raw_parts(self.mem, self.len) }
    }

    pub fn as_slice_mut(&mut self) -> &mut [u8] {
        // we can safely do this because we know that we are the only handle referencing this buffer
        unsafe { slice::from_raw_parts_mut(self.mem, self.len) }
    }

    /// Get access to the full buffer space which may not be fully initialized
    pub fn uninit_slice_mut(&mut self) -> &mut UninitSlice {
        unsafe { UninitSlice::from_raw_parts_mut(self.mem, self.max_len) }
    }

    pub const fn cursor(&self) -> MemoryCursor {
        self.cursor_from(0)
    }

    pub const fn cursor_from(&self, offset: usize) -> MemoryCursor {
        assert!(offset < self.len);
        MemoryCursor {
            handle: self,
            offset,
        }
    }

    pub fn cursor_mut(&mut self) -> MutMemoryCursor {
        self.cursor_mut_from(0)
    }

    pub fn cursor_mut_from(&mut self, offset: usize) -> MutMemoryCursor {
        assert!(offset <= self.len);
        MutMemoryCursor {
            handle: self,
            offset,
        }
    }
}

impl Index<usize> for MemoryHandle {
    type Output = u8;

    fn index(&self, index: usize) -> &Self::Output {
        assert!(index < self.len);
        unsafe { &*(self.mem.add(index) as *const _) }
    }
}

impl IndexMut<usize> for MemoryHandle {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        assert!(index < self.len);
        unsafe { &mut *(self.mem.add(index)) }
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

impl AsRef<[u8]> for MemoryHandle {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsMut<[u8]> for MemoryHandle {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_slice_mut()
    }
}

impl<'buf> From<&'buf MemoryHandle> for MemoryCursor<'buf> {
    fn from(value: &'buf MemoryHandle) -> Self {
        value.cursor()
    }
}

impl<'buf> From<&'buf mut MemoryHandle> for MutMemoryCursor<'buf> {
    fn from(value: &'buf mut MemoryHandle) -> Self {
        value.cursor_mut()
    }
}

unsafe impl Send for MemoryHandle {}

pub struct MemoryCursor<'buf> {
    handle: &'buf MemoryHandle,
    offset: usize,
}

pub struct MutMemoryCursor<'buf> {
    handle: &'buf mut MemoryHandle,
    offset: usize,
}

impl<'buf> Buf for MemoryCursor<'buf> {
    fn remaining(&self) -> usize {
        self.handle.len - self.offset
    }

    fn chunk(&self) -> &[u8] {
        &self.handle.as_slice()[self.offset..]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining(),
            "Cannot advance beyond the end of memory"
        );
        self.offset += cnt;
    }
}

unsafe impl<'buf> BufMut for MutMemoryCursor<'buf> {
    fn remaining_mut(&self) -> usize {
        self.handle.max_len - self.offset
    }

    unsafe fn advance_mut(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining_mut(),
            "Cannot advance beyond the end of memory"
        );
        self.offset += cnt;
        self.handle
            .update_len(cmp::max(self.handle.len, self.offset))
    }

    fn chunk_mut(&mut self) -> &mut UninitSlice {
        &mut self.handle.uninit_slice_mut()[self.offset..]
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BufMut};
    use futures::FutureExt;
    use tokio::task::yield_now;

    // NOTE: in these tests you do need to do the drop and yield or else it won't fully clean up
    // between tests.
    use crate::memory::MemoryManager;
    use crate::STATIC_TEST_MUTEX;

    #[test]
    fn get_handle() {
        assert_eq!(MemoryManager::new().buffer_size(), 1024 * 1024 * 8)
    }

    #[tokio::test]
    async fn allocations() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();
        let h = manager.0.allocations.lock().await;
        assert!(h.len() >= manager.0.cfg.buffer_count);
    }

    #[tokio::test]
    async fn alloc() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let h = manager.alloc().await;
        assert_eq!(manager.allocations().await, 1);

        assert_eq!(h.max_len, 1024 * 1024 * 8);
        assert_eq!(h.allocation_idx, 0);
        assert_eq!(h.len, 0);

        drop(h);
        yield_now().await;
        assert_eq!(manager.allocations().await, 0);
    }

    #[tokio::test]
    async fn multiple_alloc() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        assert_eq!(manager.allocations().await, 0);

        let a = manager.alloc().await;
        let b = manager.alloc().await;
        let c = manager.alloc().await;
        let d = manager
            .alloc()
            .now_or_never()
            .expect("Should work with opening since we have static test mutex locked");

        assert_eq!(manager.allocations().await, 4);

        assert_ne!(a.allocation_idx, b.allocation_idx);
        assert_ne!(a.allocation_idx, c.allocation_idx);
        assert_ne!(a.allocation_idx, d.allocation_idx);
        assert_ne!(b.allocation_idx, c.allocation_idx);
        assert_ne!(b.allocation_idx, d.allocation_idx);
        assert_ne!(c.allocation_idx, d.allocation_idx);

        assert!(manager.alloc().now_or_never().is_none());

        drop([a, b, c, d]);
        yield_now().await;
        assert_eq!(manager.allocations().await, 0);

        assert!(manager.alloc().now_or_never().is_some());
        yield_now().await;
        assert_eq!(manager.allocations().await, 0);
    }

    #[tokio::test]
    async fn increase_bounds() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let mut m = manager.alloc().await;
        unsafe { m.update_len(m.max_len) };
        assert_eq!(m.len(), m.max_len());
        assert_eq!(m.len(), m.len);
        assert_eq!(m.len(), 1024 * 1024 * 8);

        drop(m);
        yield_now().await;
    }

    #[tokio::test]
    async fn write_basic() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let mut m = manager.alloc().await;
        unsafe { m.update_len(4) };
        assert_eq!(m.len(), 4);
        m[0] = 1;
        m[1] = 2;
        m[2] = 4;
        m[3] = 6;

        assert_eq!(m[0], 1);
        assert_eq!(m[1], 2);
        assert_eq!(m[2], 4);
        assert_eq!(m[3], 6);

        drop(m);
        yield_now().await;
    }

    #[tokio::test]
    async fn write_slice() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let mut m = manager.alloc().await;
        unsafe { m.update_len(4) };
        let s = m.as_slice_mut();
        assert_eq!(s.len(), 4);
        s[0] = 1;
        s[1] = 2;
        s[2] = 4;
        s[3] = 6;

        assert_eq!(s[0], 1);
        assert_eq!(s[1], 2);
        assert_eq!(s[2], 4);
        assert_eq!(s[3], 6);

        drop(m);
        yield_now().await;
    }

    #[tokio::test]
    async fn bytes_read() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let mut m = manager.alloc().await;
        unsafe {
            m.update_len(4);
            m.as_slice_mut().put_slice(&[1, 2, 4, 6]);
        }

        assert_eq!(m.cursor().chunk(), &[1, 2, 4, 6]);

        let mut c = m.cursor();
        assert_eq!(c.remaining(), 4);
        c.advance(2);
        let mut b = [0u8; 2];
        c.copy_to_slice(&mut b);
        assert_eq!(b, [4, 6]);
        assert_eq!(c.remaining(), 0);

        let mut c = m.cursor_from(1);
        assert_eq!(c.remaining(), 3);
        let mut b = [0u8; 3];
        c.copy_to_slice(&mut b);
        assert_eq!(b, [2, 4, 6]);
        assert_eq!(c.remaining(), 0);

        drop(m);
        yield_now().await;
    }

    #[tokio::test]
    async fn bytes_write() {
        let _guard = STATIC_TEST_MUTEX.lock();
        let manager = MemoryManager::new();

        let mut m = manager.alloc().await;
        assert_eq!(m.len(), 0);
        m.cursor_mut().put_slice(&[6, 8, 12, 72, 53]);
        assert_eq!(m.len(), 5);
        assert_eq!(m.as_slice(), &[6, 8, 12, 72, 53]);

        let mut c = m.cursor_mut_from(1);
        c.put_slice(&[2, 3, 5]);
        assert_eq!(m.len(), 5);
        assert_eq!(m.as_slice(), &[6, 2, 3, 5, 53]);

        let mut c = m.cursor_mut_from(5);
        c.put_slice(&[1, 2, 3]);
        assert_eq!(m.len(), 8);
        assert_eq!(m.as_slice(), &[6, 2, 3, 5, 53, 1, 2, 3]);

        drop(m);
        yield_now().await;
    }
}
