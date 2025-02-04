//! [`IndexPage`] definition.

use std::fmt::Debug;

use indexset::concurrent::map::BTreeMap;
use indexset::concurrent::multimap::BTreeMultiMap;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::api::high::HighDeserializer;

use crate::link::Link;
use crate::page::IndexValue;
use crate::util::Persistable;

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexPage<T> {
    //pub node_id: T,
    pub index_values: Vec<IndexValue<T>>,
}

// Manual `Default` implementation to avoid `T: Default`
impl<'a, T> Default for IndexPage<T> {
    fn default() -> Self {
        Self {
            index_values: vec![],
        }
    }
}

impl<T> IndexPage<T>
where
    T: Clone + Ord + Debug + Send + 'static,
{
    pub fn append_to_unique_tree_index(self, index: &BTreeMap<T, Link>) {
        for val in self.index_values {
            index.insert(val.key, val.link);
        }
    }

    pub fn append_to_tree_index(self, index: &BTreeMultiMap<T, Link>) {
        for val in self.index_values {
            index.insert(val.key, val.link);
        }
    }
}

impl<T> Persistable for IndexPage<T>
where
    T: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as rkyv::Archive>::Archived:
    rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(&bytes[..]) };
        rkyv::deserialize(archived).expect("data should be valid")
    }
}
