use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::Serialize;

pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]>;
}

impl<T> Persistable for Vec<T>
where
    T: Persistable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

impl Persistable for u8 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

impl Persistable for String {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}
