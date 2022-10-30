trait Signer {
    fn sign(&self, hash: u128) -> Vec<u8>;
    fn verify(&self, signature: &[u8]) -> bool;
}
