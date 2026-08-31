use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::page::PageId;
use crate::{align, align8, Persistable, SizeMeasurable};

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
            estimated_size: <usize as SizeMeasurable>::default_aligned_size() + 12,
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
    <TableOfContentsPagePersisted<T> as rkyv::Archive>::Archived:
        for<'a> rkyv::bytecheck::CheckBytes<
            rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>,
        >,
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
    fn from_bytes(bytes: &[u8], _version: u32) -> Self {
        // Validated: the table of contents is the map every other read
        // trusts, so a torn one must fail loudly here.
        let archived =
            crate::access_archived::<<TableOfContentsPagePersisted<T> as Archive>::Archived>(bytes)
                .expect("torn or corrupt table of contents page: the bytes fail validation");
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

    /// Serialized size of one `(key, PageId)` record.
    ///
    /// Mirrors `<(T, PageId) as SizeMeasurable>::aligned_size` without
    /// needing to clone the key, so insertion and removal account records
    /// with the same formula.
    fn record_size(key: &T) -> usize
    where
        T: SizeMeasurable,
    {
        let len = key.aligned_size() + PageId::default().0.aligned_size();
        if let Some(key_align) = T::align() {
            if key_align % 8 == 0 {
                return align8(len);
            }
        }
        align(len)
    }

    pub fn insert(&mut self, val: T, page_id: PageId)
    where
        T: SizeMeasurable + Clone,
    {
        self.estimated_size += Self::record_size(&val);
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
        // The removed page is recorded as empty, which grows the serialized
        // `empty_pages` list by one `PageId`.
        self.estimated_size += id.aligned_size();
        self.empty_pages.push(id);
        id
    }

    pub fn remove_without_record(&mut self, val: &T) -> PageId
    where
        T: SizeMeasurable,
    {
        self.estimated_size -= Self::record_size(val);

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

    fn link(offset: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length: 32,
        }
    }

    #[test]
    fn test_remove_without_record_keeps_estimated_size_exact() {
        let mut toc_page = TableOfContentsPage::<(u32, Link)>::default();
        let empty_size = toc_page.as_bytes().as_ref().len();
        assert_eq!(empty_size, toc_page.estimated_size());

        toc_page.insert((1, link(0)), 6.into());
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );

        // No empty-page record is produced, so the size must return exactly
        // to the empty-page baseline. It used to stay one PageId too big.
        toc_page.remove_without_record(&(1, link(0)));
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );
        assert_eq!(toc_page.estimated_size(), empty_size);
    }

    #[test]
    fn test_remove_keeps_estimated_size_exact() {
        let mut toc_page = TableOfContentsPage::<(u32, Link)>::default();
        toc_page.insert((1, link(0)), 6.into());
        toc_page.insert((2, link(64)), 7.into());

        // `remove` records the freed page in `empty_pages`, which itself
        // takes serialized space.
        toc_page.remove(&(1, link(0)));
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );

        // Reusing the empty page gives that space back.
        assert_eq!(toc_page.pop_empty_page(), Some(6.into()));
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );
    }

    #[test]
    fn test_remove_accounts_8_aligned_keys_like_insert() {
        // (u64, Link) records are 8-aligned, so their serialized record size
        // is align8-rounded. Removal used to subtract the 4-aligned size,
        // leaving estimated_size drifting upward on every insert/remove
        // cycle.
        let mut toc_page = TableOfContentsPage::<(u64, Link)>::default();
        let empty_size = toc_page.as_bytes().as_ref().len();

        toc_page.insert((128, link(0)), 6.into());
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );

        toc_page.remove_without_record(&(128, link(0)));
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );
        assert_eq!(toc_page.estimated_size(), empty_size);
    }

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
