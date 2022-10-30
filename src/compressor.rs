use crate::memory::MemoryHandle;

trait Compressor {
    fn compress(&self, mem: &mut MemoryHandle);
}
