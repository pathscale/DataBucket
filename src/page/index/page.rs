//! [`IndexPage`] definition.

use std::fmt::Debug;
use std::sync::Arc;

use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use rkyv::api::high::HighDeserializer;
use scc::ebr::Guard;
use scc::TreeIndex;

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
    T: Clone + Ord + Debug + 'static,
{
    pub fn append_to_unique_tree_index(self, index: &TreeIndex<T, Link>) {
        for val in self.index_values {
            // Errors only if key is already exists.
            index.insert(val.key, val.link).expect("index is unique");
        }
    }

    pub fn append_to_tree_index(self, index: &TreeIndex<T, Arc<lockfree::set::Set<Link>>>) {
        for val in self.index_values {
            let guard = Guard::new();
            if let Some(set) = index.peek(&val.key, &guard) {
                set.insert(val.link).expect("Link should be unique");
            } else {
                let set = lockfree::set::Set::new();
                set
                    .insert(val.link)
                    .expect("Link should be unique as first inserted value");
                index
                    .insert(val.key, Arc::new(set))
                    .expect("index is unique");
            }
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
