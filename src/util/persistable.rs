use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::de::Pool;
use crate::SizeMeasurable;

pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]>;
    fn from_bytes(bytes: &[u8]) -> Self;
}

impl<T> Persistable for Vec<T>
where
    T: Archive
    + for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    > + Default + SizeMeasurable + Clone,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(&bytes[..]) };
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid")
    }
}

impl Persistable for u8 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(&bytes[..]) };
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid")
    }
}

impl Persistable for String {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(&bytes[..]) };
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid")
    }
}
