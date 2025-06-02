use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::page::PageId;
use crate::{align, Persistable, SizeMeasurable};

#[derive(Archive, Clone, Deserialize, Debug, Serialize)]
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
            estimated_size: usize::default().aligned_size() + 12,
        }
    }
}

#[derive(Archive, Clone, Deserialize, Debug, Serialize)]
struct TableOfContentsPagePersisted<T: Ord + Eq> {
    records: Vec<(T, PageId)>,
    empty_pages: Vec<PageId>,
    estimated_size: usize,
}

impl<T: Ord + Eq> Persistable for TableOfContentsPage<T>
where
    T: Clone
        + rkyv::Archive
        + for<'a> rkyv::Serialize<
            rkyv::rancor::Strategy<
                rkyv::ser::Serializer<
                    rkyv::util::AlignedVec,
                    rkyv::ser::allocator::ArenaHandle<'a>,
                    rkyv::ser::sharing::Share,
                >,
                rkyv::rancor::Error,
            >,
        >,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>> + Ord,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        let records = self
            .records
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect::<Vec<_>>();
        let model = TableOfContentsPagePersisted {
            records,
            empty_pages: self.empty_pages.clone(),
            estimated_size: self.estimated_size,
        };
        rkyv::to_bytes::<rkyv::rancor::Error>(&model).unwrap()
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe {
            rkyv::access_unchecked::<<TableOfContentsPagePersisted<T> as Archive>::Archived>(bytes)
        };
        let model: TableOfContentsPagePersisted<T> =
            rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");
        let records = BTreeMap::from_iter(model.records);
        Self {
            records,
            estimated_size: model.estimated_size,
            empty_pages: model.empty_pages,
        }
    }
}

impl<T> TableOfContentsPage<T>
where
    T: Debug + Ord + Eq,
{
    pub fn estimated_size(&self) -> usize {
        self.estimated_size
    }

    pub fn insert(&mut self, val: T, page_id: PageId)
    where
        T: SizeMeasurable + Clone,
    {
        self.estimated_size += (val.clone(), page_id).aligned_size();
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

    pub fn update_key(&mut self, old_key: &T, new_key: T) -> Option<()> {
        if let Some(id) = self.records.remove(old_key) {
            self.records.insert(new_key, id);
            return Some(());
        }
        None
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

#[cfg(test)]
mod test {
    use crate::{Link, Persistable, TableOfContentsPage};

    #[test]
    fn test_sizes() {
        let mut toc_page = TableOfContentsPage::<(u64, Link)>::default();
        assert_eq!(toc_page.as_bytes().as_ref().len(), toc_page.estimated_size);
        toc_page.insert(
            (
                128,
                Link {
                    page_id: 1.into(),
                    offset: 40,
                    length: 80,
                },
            ),
            6.into(),
        );
        assert_eq!(toc_page.as_bytes().as_ref().len(), toc_page.estimated_size);
    }
}
