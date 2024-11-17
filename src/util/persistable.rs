pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]>;
}
