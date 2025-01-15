pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]>;
    fn from_bytes(bytes: &[u8]) -> Self;
}
