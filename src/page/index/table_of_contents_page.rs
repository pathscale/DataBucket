use rkyv::{Archive, Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;

use crate::page::PageId;
use crate::{align, align_to, Persistable, SizeMeasurable, INNER_PAGE_SIZE};

/// Serialized size of a [`TableOfContentsPage`] with no records and no
/// empty pages: the `estimated_size` field itself plus the two empty
/// vectors.
pub const EMPTY_TABLE_OF_CONTENTS_PAGE_SIZE: usize = std::mem::size_of::<usize>() + 12;

/// Error returned by the capacity-checked mutators of
/// [`TableOfContentsPage`] when adding a record would push the page's
/// serialized form past its page slot.
///
/// The rejected key is handed back in [`Self::into_key`] so the caller can
/// place it on another page; [`Self::fits_empty_page`] tells whether such a
/// relocation can ever succeed.
#[derive(Debug)]
pub struct TableOfContentsOverflowError<T> {
    /// The key that was not added.
    pub key: T,
    /// Serialized size of the rejected record.
    pub record_size: usize,
    /// The page's estimated serialized size at the time of rejection.
    pub estimated_size: usize,
    /// The serialized-size budget of the page slot.
    pub capacity: usize,
}

impl<T> TableOfContentsOverflowError<T> {
    /// Returns the rejected key so it can be placed on another page.
    pub fn into_key(self) -> T {
        self.key
    }

    /// `true` when the record fits an empty page, so the caller can
    /// relocate it to a fresh table-of-contents page. `false` means the
    /// record can never fit any page slot and must be rejected upstream.
    pub fn fits_empty_page(&self) -> bool {
        EMPTY_TABLE_OF_CONTENTS_PAGE_SIZE + self.record_size <= self.capacity
    }
}

impl<T> std::fmt::Display for TableOfContentsOverflowError<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "table of contents record of {} bytes does not fit the page \
             (estimated size {} of {} bytes)",
            self.record_size, self.estimated_size, self.capacity
        )
    }
}

impl<T: Debug> std::error::Error for TableOfContentsOverflowError<T> {}

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
            estimated_size: EMPTY_TABLE_OF_CONTENTS_PAGE_SIZE,
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
        + SizeMeasurable
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
        // Recompute the accounting from the actual contents instead of
        // trusting the stored field: pages written by versions with
        // accounting bugs (0.5.2 and earlier) would otherwise carry their
        // wrong estimated_size across the upgrade and mislead the capacity
        // checks.
        let estimated_size = Self::recompute_estimated_size(&records, &model.empty_pages);
        Self {
            records,
            estimated_size,
            empty_pages: model.empty_pages,
        }
    }
}

impl<T: Ord + Eq> TableOfContentsPage<T> {
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
                // rkyv pads the archived record out to the key's real
                // alignment (16 for u128-likes), so round to it, not to 8.
                return align_to(len, key_align);
            }
        }
        align(len)
    }

    /// Size accounting derived from the actual contents, used when loading
    /// a persisted page: the stored `estimated_size` may carry accounting
    /// bugs of the version that wrote it, and trusting it would let such an
    /// error survive upgrades.
    fn recompute_estimated_size(records: &BTreeMap<T, PageId>, empty_pages: &[PageId]) -> usize
    where
        T: SizeMeasurable,
    {
        let mut estimated_size = EMPTY_TABLE_OF_CONTENTS_PAGE_SIZE;
        for key in records.keys() {
            estimated_size += Self::record_size(key);
        }
        estimated_size += empty_pages.len() * PageId::default().0.aligned_size();
        estimated_size
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
        let record_size = Self::record_size(&val);
        // Inserting over an existing key replaces its PageId in place, so
        // the serialized page does not grow.
        if self.records.insert(val, page_id).is_none() {
            self.estimated_size += record_size;
        }
    }

    /// Capacity-checked [`Self::insert`].
    ///
    /// Adds the record only when the page's serialized form stays within
    /// its page slot ([`INNER_PAGE_SIZE`]). On overflow the page is left
    /// untouched and the key is handed back in the error, so the caller can
    /// place it on another page; [`TableOfContentsOverflowError::fits_empty_page`]
    /// tells whether a fresh page can ever hold it.
    pub fn try_insert(
        &mut self,
        val: T,
        page_id: PageId,
    ) -> Result<(), TableOfContentsOverflowError<T>>
    where
        T: SizeMeasurable + Clone,
    {
        let record_size = Self::record_size(&val);
        // Replacing an existing key does not grow the page, so it always
        // fits.
        if !self.records.contains_key(&val) && self.estimated_size + record_size > INNER_PAGE_SIZE {
            return Err(TableOfContentsOverflowError {
                key: val,
                record_size,
                estimated_size: self.estimated_size,
                capacity: INNER_PAGE_SIZE,
            });
        }
        self.insert(val, page_id);
        Ok(())
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

    /// Capacity-checked [`Self::update_key`].
    ///
    /// Applies the re-keying only when the page's serialized form stays
    /// within its page slot ([`INNER_PAGE_SIZE`]); on overflow the page is
    /// left untouched and `new_key` is handed back in the error, so the
    /// caller can relocate the record instead.
    ///
    /// Returns `Ok(true)` when the key was updated and `Ok(false)` when
    /// `old_key` is not present (the page stays untouched).
    pub fn try_update_key(
        &mut self,
        old_key: &T,
        new_key: T,
    ) -> Result<bool, TableOfContentsOverflowError<T>>
    where
        T: SizeMeasurable,
    {
        if !self.records.contains_key(old_key) {
            return Ok(false);
        }

        let new_record_size = Self::record_size(&new_key);
        // Re-keying onto an already existing key replaces that record in
        // place, so only the old record's size is released.
        let replaces_existing = new_key != *old_key && self.records.contains_key(&new_key);
        let projected = if replaces_existing {
            self.estimated_size - Self::record_size(old_key)
        } else {
            self.estimated_size - Self::record_size(old_key) + new_record_size
        };
        if projected > INNER_PAGE_SIZE {
            return Err(TableOfContentsOverflowError {
                key: new_key,
                record_size: new_record_size,
                estimated_size: self.estimated_size,
                capacity: INNER_PAGE_SIZE,
            });
        }

        self.update_key(old_key, new_key)
            .expect("old_key presence checked above");
        Ok(true)
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
    use crate::{Link, Persistable, TableOfContentsPage, INNER_PAGE_SIZE};

    fn link(offset: u32) -> Link {
        Link {
            page_id: 1.into(),
            offset,
            length: 32,
        }
    }

    #[test]
    fn test_from_bytes_recomputes_stale_persisted_estimated_size() {
        // Simulate a page persisted by 0.5.2, whose accounting bugs stored
        // a wrong estimated_size: the load must recompute it from the
        // actual records instead of trusting the stored field.
        for bogus_estimated in [0usize, 3, 100_000] {
            let stale = super::TableOfContentsPagePersisted {
                records: vec![
                    ((1u64, link(0)), crate::page::PageId::from(6)),
                    ((2u64, link(64)), crate::page::PageId::from(7)),
                ],
                empty_pages: vec![9.into()],
                estimated_size: bogus_estimated,
            };
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&stale).unwrap();

            let loaded = TableOfContentsPage::<(u64, Link)>::from_bytes(&bytes, 0);
            assert_ne!(loaded.estimated_size(), bogus_estimated);
            // The recomputed accounting matches what the page really
            // serializes to.
            assert_eq!(loaded.estimated_size(), loaded.as_bytes().as_ref().len());

            // And it matches what fresh inserts would have produced.
            let mut fresh = TableOfContentsPage::<(u64, Link)>::default();
            fresh.insert((1, link(0)), 6.into());
            fresh.insert((2, link(64)), 7.into());
            fresh.insert((3, link(128)), 9.into());
            fresh.remove(&(3, link(128)));
            assert_eq!(loaded.estimated_size(), fresh.estimated_size());
        }
    }

    #[test]
    fn test_try_insert_bounds_real_serialized_size_for_16_aligned_keys() {
        // Regression for the review blocker: with u128-family keys the size
        // model under-counted every record by 8 bytes, so try_insert kept
        // accepting records until the real archive was hundreds of bytes
        // past the page slot.
        let mut toc_page = TableOfContentsPage::<u128>::default();
        let mut i = 0u128;
        while toc_page.try_insert(i, 1.into()).is_ok() {
            i += 1;
            assert!(i < 4096, "the page must eventually report itself full");
        }
        let serialized = toc_page.as_bytes().as_ref().len();
        assert_eq!(serialized, toc_page.estimated_size());
        assert!(serialized <= INNER_PAGE_SIZE);

        let mut toc_page = TableOfContentsPage::<(u128, Link)>::default();
        let mut i = 0u128;
        while toc_page.try_insert((i, link(i as u32)), 1.into()).is_ok() {
            i += 1;
            assert!(i < 4096, "the page must eventually report itself full");
        }
        let serialized = toc_page.as_bytes().as_ref().len();
        assert_eq!(serialized, toc_page.estimated_size());
        assert!(serialized <= INNER_PAGE_SIZE);
    }

    #[test]
    fn test_estimated_size_is_an_upper_bound_under_adversarial_churn() {
        // Property-style sweep with a deterministic LCG: string keys of
        // adversarial lengths (the String model rounds to 4 bytes, so it may
        // over-estimate, never under), mixed inserts, removals and key
        // updates. The safety invariant of the capacity checks is
        // serialized <= estimated at every step; the approximation slack
        // stays below 4 bytes per record.
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) as u32
        };

        let mut toc_page = TableOfContentsPage::<(String, Link)>::default();
        let mut keys: Vec<(String, Link)> = vec![];
        let mut record_count = 0usize;
        for step in 0..600u32 {
            let op = next() % 4;
            if op < 2 || keys.is_empty() {
                let len = (next() % 48) as usize;
                let key = (format!("{step:0>len$}"), link(step));
                if toc_page.try_insert(key.clone(), (step + 1).into()).is_ok() {
                    keys.push(key);
                    record_count += 1;
                }
            } else if op == 2 {
                let key = keys.swap_remove((next() as usize) % keys.len());
                toc_page.remove(&key);
                record_count -= 1;
            } else {
                let old = keys.swap_remove((next() as usize) % keys.len());
                let len = (next() % 48) as usize;
                let new_key = (format!("{step:0>len$}"), link(step));
                match toc_page.try_update_key(&old, new_key.clone()) {
                    Ok(true) => keys.push(new_key),
                    Ok(false) => unreachable!("old key is always present"),
                    Err(_) => keys.push(old),
                }
            }

            let serialized = toc_page.as_bytes().as_ref().len();
            let estimated = toc_page.estimated_size();
            assert!(
                serialized <= estimated,
                "under-estimate at step {step}: serialized {serialized} > estimated {estimated}"
            );
            assert!(
                estimated - serialized <= 4 * (record_count + 2),
                "slack blew past the documented bound at step {step}: \
                 serialized {serialized}, estimated {estimated}, records {record_count}"
            );
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
    fn test_try_insert_never_reports_success_past_the_page_slot() {
        let mut toc_page = TableOfContentsPage::<(u32, Link)>::default();

        // Fill until the page reports itself full.
        let mut rejected_at = None;
        for i in 0..2048u32 {
            match toc_page.try_insert((i, link(i)), (i + 1).into()) {
                Ok(()) => {
                    assert!(toc_page.estimated_size() <= INNER_PAGE_SIZE);
                }
                Err(err) => {
                    rejected_at = Some((i, err));
                    break;
                }
            }
        }
        let (i, err) = rejected_at.expect("the page must eventually report itself full");

        // The serialized page still fits its slot exactly at the point of
        // rejection.
        let serialized = toc_page.as_bytes().as_ref().len();
        assert_eq!(serialized, toc_page.estimated_size());
        assert!(serialized <= INNER_PAGE_SIZE);

        // The page was left untouched by the rejected insert, and the key
        // came back for relocation.
        assert!(!toc_page.contains(&(i, link(i))));
        assert!(err.fits_empty_page());
        assert_eq!(err.into_key(), (i, link(i)));

        // Replacing an existing key does not grow the page, so it is still
        // accepted on a full page.
        let size_before = toc_page.estimated_size();
        toc_page
            .try_insert((0, link(0)), 999.into())
            .expect("replacement must fit");
        assert_eq!(toc_page.estimated_size(), size_before);
        assert_eq!(toc_page.get(&(0, link(0))), Some(999.into()));
    }

    #[test]
    fn test_try_insert_rejects_record_that_can_never_fit() {
        let mut toc_page = TableOfContentsPage::<(String, Link)>::default();
        let oversized = "x".repeat(INNER_PAGE_SIZE);

        let err = toc_page
            .try_insert((oversized.clone(), link(0)), 6.into())
            .expect_err("a record larger than the page slot must be rejected");
        // No fresh page can hold it either: the caller must fail upstream
        // instead of relocating forever.
        assert!(!err.fits_empty_page());
        assert!(!toc_page.contains(&(oversized, link(0))));
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );
    }

    #[test]
    fn test_try_update_key_rejects_growth_past_the_page_slot() {
        let mut toc_page = TableOfContentsPage::<(String, Link)>::default();
        toc_page.insert(("key_0001".to_string(), link(0)), 6.into());

        let grown = "x".repeat(INNER_PAGE_SIZE);
        let err = toc_page
            .try_update_key(&("key_0001".to_string(), link(0)), (grown.clone(), link(0)))
            .expect_err("growth past the page slot must be rejected");
        assert!(!err.fits_empty_page());
        assert_eq!(err.into_key(), (grown, link(0)));

        // The page is untouched: the old record is still there and the
        // accounting is still exact.
        assert_eq!(
            toc_page.get(&("key_0001".to_string(), link(0))),
            Some(6.into())
        );
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );

        // A fitting update through the checked variant still works.
        assert!(toc_page
            .try_update_key(
                &("key_0001".to_string(), link(0)),
                ("key_0002".to_string(), link(0)),
            )
            .unwrap());
        assert_eq!(
            toc_page.as_bytes().as_ref().len(),
            toc_page.estimated_size()
        );

        // A missing old key reports Ok(false) and changes nothing.
        assert!(!toc_page
            .try_update_key(
                &("missing".to_string(), link(0)),
                ("whatever".to_string(), link(0)),
            )
            .unwrap());
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
