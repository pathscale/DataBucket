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
 * The one switch every disk read goes through. These bytes come off disk,
 * and a process that died mid-write (crash, SIGKILL, an undrained exit)
 * leaves torn pages behind: unchecked access reads a torn page as an
 * archived value whose relative pointers dangle anywhere, and the process
 * dies of SIGBUS in whatever touches them next, usually mid-write, tearing
 * the store further. With the default `validate-reads` feature the same
 * bytes become a named error at the parse site instead, while the store on
 * disk stays exactly as readable as it was.
 *
 * `validate-reads` is a default feature rather than unconditional because
 * this crate also runs at nanosecond scale, where even background-task CPU
 * is budgeted: `default-features = false` compiles every read back to the
 * exact `access_unchecked` it was before, zero cost, caveat emptor. The
 * CheckBytes bounds stay unconditional either way so the API surface does
 * not shift under a feature flag; derived Archive types satisfy them for
 * free.
 */
#[inline]
pub fn access_archived<A>(bytes: &[u8]) -> Result<&A, rkyv::rancor::Error>
where
    A: rkyv::Portable + for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>,
{
    #[cfg(feature = "validate-reads")]
    {
        rkyv::access::<A, rkyv::rancor::Error>(bytes)
    }
    #[cfg(not(feature = "validate-reads"))]
    {
        Ok(unsafe { rkyv::access_unchecked::<A>(bytes) })
    }
}

pub(crate) fn checked<T>(bytes: &[u8]) -> T
where
    T: Archive,
    <T as Archive>::Archived: rkyv::Portable
        + for<'a> CheckBytes<HighValidator<'a, rkyv::rancor::Error>>
        + Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    let archived = access_archived::<<T as Archive>::Archived>(bytes)
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
