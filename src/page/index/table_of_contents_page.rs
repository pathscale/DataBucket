use std::collections::BTreeMap;

use data_bucket_codegen::Persistable;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::PageId;
use crate::{align, Persistable, SizeMeasurable};

#[derive(Archive, Clone, Deserialize, Debug, Serialize, Persistable)]
pub struct TableOfContentsPage<T: Ord + Eq> {
    records: BTreeMap<T, PageId>,

    empty_pages: Vec<PageId>,
    estimated_size: usize,
}

impl<T> Default for TableOfContentsPage<T>
where
    T: SizeMeasurable + Ord + Eq,
{
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            empty_pages: vec![],
            estimated_size: usize::default().aligned_size()
                + Option::<PageId>::default().aligned_size(),
        }
    }
}

impl<T> TableOfContentsPage<T>
where
    T: Ord + Eq,
{
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    pub fn insert(&mut self, val: T, page_id: PageId)
    where
        T: SizeMeasurable,
    {
        self.estimated_size += align(val.aligned_size() + page_id.0.aligned_size());
        let _ = self.records.insert(val, page_id);
    }

    pub fn pop_empty_page(&mut self) -> Option<PageId>
    where
        T: SizeMeasurable,
    {
        if self.empty_pages.is_empty() {
            return None;
        }

        let val = self
            .empty_pages
            .pop()
            .expect("should not be empty as checked before");
        self.estimated_size -= val.aligned_size();
        Some(val)
    }

    pub fn get(&self, val: &T) -> Option<PageId> {
        self.records.get(val).copied()
    }

    pub fn remove(&mut self, val: &T) -> PageId
    where
        T: SizeMeasurable,
    {
        let id = self.remove_without_record(val);
        self.empty_pages.push(id);
        id
    }

    pub fn remove_without_record(&mut self, val: &T) -> PageId
    where
        T: SizeMeasurable,
    {
        self.estimated_size -= align(val.aligned_size() + PageId::default().0.aligned_size());
        self.estimated_size += PageId::default().0.aligned_size();

        self.records
            .remove(val)
            .expect("value should be available if remove is called")
    }

    pub fn update_key(&mut self, old_key: &T, new_key: T) {
        let id = self
            .records
            .remove(old_key)
            .expect("value should be available if update is called");
        self.records.insert(new_key, id);
    }

    pub fn contains(&self, val: &T) -> bool {
        self.records.contains_key(val)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&T, &PageId)> {
        self.records.iter()
    }
}

impl<T> IntoIterator for TableOfContentsPage<T>
where
    T: Ord + Eq,
{
    type Item = (T, PageId);
    type IntoIter = <BTreeMap<T, PageId> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.records.into_iter()
    }
}
