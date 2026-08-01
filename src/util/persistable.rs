use crate::SizeMeasurable;

use rkyv::api::high::HighValidator;
use rkyv::bytecheck::CheckBytes;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

pub trait Persistable {
    fn as_bytes(&self) -> impl AsRef<[u8]> + Send;
    fn from_bytes(bytes: &[u8], version: u32) -> Self;
}

/*
 * Validated access, not `access_unchecked`. These bytes come off disk, and a
 * process that died mid-write (crash, SIGKILL, an undrained exit) leaves torn
 * pages behind: unchecked access reads a torn page as an archived value whose
 * relative pointers dangle anywhere, and the process dies of SIGBUS in
 * whatever touches them next — usually mid-write, tearing the store further.
 * Validation turns the same bytes into a named panic at the parse site,
 * while the store on disk stays exactly as readable as it was.
 */
pub(crate) fn checked<T>(bytes: &[u8]) -> T
where
    T: Archive,
    <T as Archive>::Archived: rkyv::Portable
        + for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>
        + Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    let archived = rkyv::access::<<T as Archive>::Archived, rkyv::rancor::Error>(bytes)
        .expect("torn or corrupt page: the archived bytes fail validation");
    rkyv::deserialize::<_, rkyv::rancor::Error>(archived)
        .expect("validated archive failed to deserialize")
}

impl<T> Persistable for Vec<T>
where
    T: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Default
        + SizeMeasurable
        + Clone,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>
        + for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8], _version: u32) -> Self {
        checked::<Self>(bytes)
    }
}

impl Persistable for u8 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8], _version: u32) -> Self {
        checked::<Self>(bytes)
    }
}

impl Persistable for String {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8], _version: u32) -> Self {
        checked::<Self>(bytes)
    }
}
