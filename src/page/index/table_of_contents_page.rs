use std::collections::HashMap;
use std::hash::Hash;

use rkyv::{Archive, Deserialize, Serialize};
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::Serializer;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;
use rkyv::with::Skip;

use crate::page::PageId;
use crate::{align, Persistable, SizeMeasurable};

#[derive(Archive, Clone, Deserialize, Debug, Serialize)]
pub struct TableOfContentsPage<T> {
    records: HashMap<T, PageId>,
    #[rkyv(with = Skip)]
    estimated_size: usize,
    is_last: bool,
}

impl<T> Default for TableOfContentsPage<T> {
    fn default() -> Self {
        Self {
            records: HashMap::new(),
            estimated_size: 0,
            is_last: false,
        }
    }
}

impl<T> TableOfContentsPage<T>
{

    pub fn is_last(&self) -> bool {
        self.is_last
    }

    pub fn mark_not_last(&mut self) {
        self.is_last = false;
    }

    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    pub fn insert(&mut self, val: T, page_id: PageId)
    where T: Hash + Eq + SizeMeasurable
    {
        self.estimated_size += align(val.aligned_size() + page_id.0.aligned_size());
        let _ = self.records.insert(val, page_id);
    }

    pub fn remove(&mut self, val: &T)
    where T: Hash + Eq + SizeMeasurable
    {
        self.estimated_size -= align(val.aligned_size() + PageId::default().0.aligned_size());
        let _ = self.records.remove(val);
    }

    pub fn contains(&self, val: &T) -> bool
    where T: Hash + Eq
    {
        self.records.contains_key(val)
    }
}

impl<T> IntoIterator for TableOfContentsPage<T> {
    type Item = (T, PageId);
    type IntoIter = <HashMap<T, PageId> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}

impl<T> Persistable for TableOfContentsPage<T>
where
    T: Archive
    + for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    > + Hash + Eq,
    <T as Archive>::Archived: Hash + Eq
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

