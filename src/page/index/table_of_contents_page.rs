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
        let record_size = Self::record_size(&val);
        // Inserting over an existing key replaces its PageId in place, so
        // the serialized page does not grow.
        if self.records.insert(val, page_id).is_none() {
            self.estimated_size += record_size;
        }
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

    /// Re-keys the record at `old_key` to `new_key`, maintaining
    /// `estimated_size` for the size difference between the two keys.
    ///
    /// Returns [`None`] (leaving the page untouched) when `old_key` is not
    /// present.
    ///
    /// This variant performs no capacity check: a key that grows past the
    /// page slot is accepted and only reported through `estimated_size`.
    /// Use [`Self::try_update_key`] when the caller can relocate instead.
    pub fn update_key(&mut self, old_key: &T, new_key: T) -> Option<()>
    where
        T: SizeMeasurable,
    {
        let id = self.records.remove(old_key)?;
        self.estimated_size -= Self::record_size(old_key);
        let new_record_size = Self::record_size(&new_key);
        // If `new_key` was already present, its record is replaced in place
        // and the serialized page does not grow.
        if self.records.insert(new_key, id).is_none() {
            self.estimated_size += new_record_size;
        }
        Some(())
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
    fn test_insert_over_existing_key_keeps_estimated_size_exact() {
        let mut toc_page = TableOfContentsPage::<(u32, Link)>::default();
        toc_page.insert((1, link(0)), 6.into());
        let size_after_first = toc_page.estimated_size();

        // Re-pointing the same key to another page replaces the record in
        // place; it used to be counted as a second record.
        toc_page.insert((1, link(0)), 7.into());
        assert_eq!(toc_page.get(&(1, link(0))), Some(7.into()));
        assert_eq!(toc_page.estimated_size(), size_after_first);
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
    fn test_update_key_keeps_estimated_size_exact() {
        fn assert_exact(toc_page: &TableOfContentsPage<(String, Link)>) {
            assert_eq!(
                toc_page.as_bytes().as_ref().len(),
                toc_page.estimated_size()
            );
        }

        let mut toc_page = TableOfContentsPage::<(String, Link)>::default();
        toc_page.insert(("key_0001".to_string(), link(0)), 6.into());
        assert_exact(&toc_page);

        // Growing the key must grow estimated_size with it; it used not to
        // be accounted at all.
        let grown = "key_0001_grown_by_a_long_suffix_0001".to_string();
        toc_page
            .update_key(&("key_0001".to_string(), link(0)), (grown.clone(), link(0)))
            .unwrap();
        assert_exact(&toc_page);

        // Shrinking it must give the space back.
        toc_page
            .update_key(&(grown, link(0)), ("key_0001".to_string(), link(0)))
            .unwrap();
        assert_exact(&toc_page);

        // Updating onto an already existing key replaces that record in
        // place.
        toc_page.insert(("key_0002".to_string(), link(64)), 7.into());
        toc_page
            .update_key(
                &("key_0001".to_string(), link(0)),
                ("key_0002".to_string(), link(64)),
            )
            .unwrap();
        assert_exact(&toc_page);
        assert_eq!(
            toc_page.get(&("key_0002".to_string(), link(64))),
            Some(6.into())
        );

        // A missing old key leaves the page untouched.
        let before = toc_page.estimated_size();
        assert!(toc_page
            .update_key(
                &("missing".to_string(), link(0)),
                ("whatever".to_string(), link(0)),
            )
            .is_none());
        assert_eq!(toc_page.estimated_size(), before);
        assert_exact(&toc_page);
    }

    #[test]
    // Key strings are chosen with len % 4 == 0 (or <= 8): the String size
    // model in SizeMeasurable rounds each string to 4 bytes, which is the
    // documented accuracy bound of estimated_size for other lengths.
    fn test_estimated_size_stays_exact_across_churn() {
        fn assert_exact(toc_page: &TableOfContentsPage<(String, Link)>) {
            assert_eq!(
                toc_page.as_bytes().as_ref().len(),
                toc_page.estimated_size()
            );
        }

        let mut toc_page = TableOfContentsPage::<(String, Link)>::default();

        for i in 0..32u32 {
            toc_page.insert((format!("key_{i:04}"), link(i)), (i + 1).into());
            assert_exact(&toc_page);
        }

        // Grow half of the keys through updates.
        for i in 0..16u32 {
            toc_page
                .update_key(
                    &(format!("key_{i:04}"), link(i)),
                    (format!("key_{i:04}_grown_by_a_long_suffix_"), link(i)),
                )
                .unwrap();
            assert_exact(&toc_page);
        }

        // Shrink them back.
        for i in 0..16u32 {
            toc_page
                .update_key(
                    &(format!("key_{i:04}_grown_by_a_long_suffix_"), link(i)),
                    (format!("key_{i:04}"), link(i)),
                )
                .unwrap();
            assert_exact(&toc_page);
        }

        // Remove a mix, with and without empty-page records.
        for i in 0..8u32 {
            toc_page.remove(&(format!("key_{i:04}"), link(i)));
            assert_exact(&toc_page);
        }
        for i in 8..16u32 {
            toc_page.remove_without_record(&(format!("key_{i:04}"), link(i)));
            assert_exact(&toc_page);
        }
        while toc_page.pop_empty_page().is_some() {
            assert_exact(&toc_page);
        }
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
