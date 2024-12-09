use crate::link::Link;
use crate::page;
use crate::page::{IndexPage, PAGE_SIZE};
use crate::util::SizeMeasurable;
use scc::TreeIndex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A wrapper around TreeIndex that provides size measurement capabilities
pub struct MeasuredTreeIndex<T> {
    inner: TreeIndex<T, Link>,
    size: AtomicUsize,
}

/// A wrapper around TreeIndex that provides size measurement capabilities for non-unique indexes
pub struct MeasuredMultiTreeIndex<T> {
    inner: TreeIndex<T, Arc<lockfree::set::Set<Link>>>,
    size: AtomicUsize,
}

impl<T> MeasuredTreeIndex<T>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: TreeIndex::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, key: T, link: Link) -> Result<(), (T, Link)> {
        let value_size = key.aligned_size() + link.aligned_size();
        let new_size = self.size.load(Ordering::Relaxed) + value_size;

        match self.inner.insert(key, link) {
            Ok(()) => {
                self.size.store(new_size, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn page_count(&self) -> usize {
        let size = self.size.load(Ordering::Relaxed);
        if size == 0 {
            0
        } else {
            (size + PAGE_SIZE - 1) / PAGE_SIZE
        }
    }

    pub fn inner(&self) -> &TreeIndex<T, Link> {
        &self.inner
    }

    pub fn to_pages(&self) -> Vec<IndexPage<T>> {
        page::map_unique_tree_index::<T, PAGE_SIZE>(&self.inner)
    }
}

impl<T> MeasuredMultiTreeIndex<T>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    pub fn new() -> Self {
        Self {
            inner: TreeIndex::new(),
            size: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, key: T, link: Link) -> Result<(), (T, Arc<lockfree::set::Set<Link>>)> {
        let value_size = key.aligned_size() + link.aligned_size();
        let new_size = self.size.load(Ordering::Relaxed) + value_size;

        let set = lockfree::set::Set::new();
        set.insert(link).expect("is ok");

        match self.inner.insert(key, Arc::new(set)) {
            Ok(()) => {
                self.size.store(new_size, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn page_count(&self) -> usize {
        let size = self.size.load(Ordering::Relaxed);
        if size == 0 {
            0
        } else {
            (size + PAGE_SIZE - 1) / PAGE_SIZE
        }
    }

    pub fn inner(&self) -> &TreeIndex<T, Arc<lockfree::set::Set<Link>>> {
        &self.inner
    }

    pub fn to_pages(&self) -> Vec<IndexPage<T>> {
        page::map_tree_index::<T, PAGE_SIZE>(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::link::Link;

    #[test]
    fn test_measured_tree_index() {
        let index = MeasuredTreeIndex::new();
        assert_eq!(index.page_count(), 0);

        for i in 0..1024 {
            let link = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, link).expect("insert should succeed");
        }

        assert_eq!(index.page_count(), 1);

        let link = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1024, link).expect("insert should succeed");

        assert_eq!(index.page_count(), 2);

        let pages = index.to_pages();
        assert_eq!(pages.len(), index.page_count());
    }

    #[test]
    fn test_measured_multi_tree_index() {
        let index = MeasuredMultiTreeIndex::new();
        assert_eq!(index.page_count(), 0);

        for i in 0..256 {
            let link = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, link).expect("insert should succeed");
        }

        assert!(index.page_count() > 0);

        let pages = index.to_pages();
        assert_eq!(pages.len(), index.page_count());
    }
}
