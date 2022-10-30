use crate::memory::MemoryHandle;

trait Encryptor {
    fn encrypt(&self, mem: &mut MemoryHandle);
}
