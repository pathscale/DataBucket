pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]>;
}

impl Persistable for String {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.as_bytes()
    }
}