use std::collections::HashMap;
use std::hash::Hash;

use rkyv::{Archive, Deserialize, Serialize};
use rkyv::api::high::HighDeserializer;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::Serializer;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;

use crate::page::PageId;
use crate::{align, Persistable, SizeMeasurable};

#[derive(Archive, Clone, Deserialize, Debug, Serialize)]
pub struct TableOfContentsPage<T> {
    records: HashMap<T, PageId>,

    empty_pages: Vec<PageId>,
    estimated_size: usize,
    next_page: Option<PageId>,
}

impl<T> Default for TableOfContentsPage<T>
where T: SizeMeasurable
{
    fn default() -> Self {
        Self {
            records: HashMap::new(),
            empty_pages: vec![],
            estimated_size: usize::default().aligned_size() + Option::<PageId>::default().aligned_size(),
            next_page: None,
        }
    }
}

impl<T> TableOfContentsPage<T>
{

    pub fn is_last(&self) -> bool {
        self.next_page.is_none()
    }

    pub fn mark_not_last(&mut self, page_id: PageId) {
        self.next_page = Some(page_id)
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

    pub fn pop_empty_page(&mut self) -> Option<PageId>
    where T: SizeMeasurable
    {
        if self.empty_pages.is_empty() {
            return None
        }

        let val = self.empty_pages.pop().expect("should not be empty as checked before");
        self.estimated_size -= val.aligned_size();
        Some(val)
    }

    pub fn get(&self, val: &T) -> Option<PageId>
    where T: Hash + Eq
    {
        self.records.get(val).copied()
    }

    pub fn remove(&mut self, val: &T)
    where T: Hash + Eq + SizeMeasurable
    {
        self.estimated_size -= align(val.aligned_size() + PageId::default().0.aligned_size());
        self.estimated_size += PageId::default().0.aligned_size();
        
        let id = self.records.remove(val).expect("value should be available if remove is called");
        self.empty_pages.push(id);
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
    <T as rkyv::Archive>::Archived:
    rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>> + Hash + Eq,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<Self as Archive>::Archived>(&bytes[..]) };
        rkyv::deserialize(archived).expect("data should be valid")
    }
}

