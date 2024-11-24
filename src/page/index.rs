//! [`IndexPage`] definition.

use std::fmt::Debug;
use std::sync::Arc;

use rkyv::ser::serializers::AllocSerializer;
use rkyv::{Archive, Deserialize, Serialize};
use scc::ebr::Guard;
use scc::TreeIndex;

use crate::link::Link;
use crate::page::INNER_PAGE_SIZE;
use crate::util::{Persistable, SizeMeasurable};

/// Represents `key/value` pair of B-Tree index, where value is always
/// [`data::Link`], as it is represented in primary and secondary indexes.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexValue<T> {
    pub key: T,
    pub link: Link,
}

impl<T> SizeMeasurable for IndexValue<T>
where
    T: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        self.key.aligned_size() + self.link.aligned_size()
    }
}

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexPage<T> {
    pub index_values: Vec<IndexValue<T>>,
}

// Manual `Default` implementation to avoid `T: Default`
impl<T> Default for IndexPage<T> {
    fn default() -> Self {
        Self {
            index_values: vec![],
        }
    }
}

impl<T> IndexPage<T>
where T: Clone + Ord + Debug + 'static
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
                set.insert(val.link).expect("is ok");
            } else {
                let set = lockfree::set::Set::new();
                set.insert(val.link).expect("is ok");
                index
                    .insert(val.key, Arc::new(set))
                    .expect("index is unique");
            }
        }
    }
}

pub fn map_unique_tree_index<T, const PAGE_SIZE: usize>(
    index: &TreeIndex<T, Link>,
) -> Vec<IndexPage<T>>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    let guard = Guard::new();
    let mut pages = vec![];
    let mut current_page = IndexPage::default();
    let mut current_size = 8;

    for (key, &link) in index.iter(&guard) {
        let index_value = IndexValue {
            key: key.clone(),
            link,
        };
        current_size += index_value.aligned_size();
        if current_size > PAGE_SIZE {
            pages.push(current_page.clone());
            current_page.index_values.clear();
            current_size = 8 + index_value.aligned_size()
        }
        current_page.index_values.push(index_value)
    }
    pages.push(current_page);

    pages
}

pub fn map_tree_index<T, const PAGE_SIZE: usize>(
    index: &TreeIndex<T, Arc<lockfree::set::Set<Link>>>,
) -> Vec<IndexPage<T>>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    let guard = Guard::new();
    let mut pages = vec![];
    let mut current_page = IndexPage::default();
    let mut current_size = 8;

    for (key, links) in index.iter(&guard) {
        for link in links.iter() {
            let index_value = IndexValue {
                key: key.clone(),
                link: *link,
            };
            current_size += index_value.aligned_size();
            if current_size > PAGE_SIZE {
                pages.push(current_page.clone());
                current_page.index_values.clear();
                current_size = 8 + index_value.aligned_size()
            }
            current_page.index_values.push(index_value)
        }
    }
    pages.push(current_page);

    pages
}

impl<T> Persistable for IndexPage<T>
where
    T: Archive + Serialize<AllocSerializer<{ INNER_PAGE_SIZE }>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<_, { INNER_PAGE_SIZE }>(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use scc::ebr::Guard;
    use scc::TreeIndex;

    use crate::page::index::map_unique_tree_index;
    use crate::page::{INNER_PAGE_SIZE, PAGE_SIZE};
    use crate::util::{Persistable, SizeMeasurable};
    use crate::{map_tree_index, Link};

    #[test]
    fn map_single_value() {
        let index = TreeIndex::new();
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1u32, l).expect("is ok");

        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(&index);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, 1);
        assert_eq!(v.link, l);
        assert_eq!(
            rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(),
            1u32.aligned_size() + l.aligned_size() + 8
        )
    }

    #[test]
    fn map_page_border() {
        let index = TreeIndex::new();
        for i in 0..1023 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l).expect("is ok");
        }

        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(&index);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1023);
        // As 1023 * 16 + 8
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(), 16_376);

        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1024, l).expect("is ok");
        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(&index);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].index_values.len(), 1023);
        assert_eq!(res[1].index_values.len(), 1);
        // As 16 + 8
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(), 16_376);
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[1]).unwrap().len(), 24);
    }

    #[test]
    fn map_unique_and_back() {
        let index = TreeIndex::new();
        for i in 0..1023 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l).expect("is ok");
        }

        let pages = map_unique_tree_index::<_, { PAGE_SIZE }>(&index);
        let res_index = TreeIndex::new();

        for page in pages {
            page.append_to_unique_tree_index(&res_index)
        }

        assert_eq!(index, res_index)
    }

    #[test]
    fn map_and_back() {
        let index = TreeIndex::new();
        for i in 0..256 {
            let set = lockfree::set::Set::new();
            for j in 0..4 {
                let l = Link {
                    page_id: j.into(),
                    offset: 0,
                    length: 32,
                };
                set.insert(l).unwrap();
            }

            index.insert(i, Arc::new(set)).expect("is ok");
        }

        let pages = map_tree_index::<_, { PAGE_SIZE }>(&index);
        let res_index = TreeIndex::new();

        for page in pages {
            page.append_to_tree_index(&res_index)
        }

        let guard = Guard::new();
        for (k, set) in index.iter(&guard) {
            let res_guard = Guard::new();
            let res_set = res_index.peek(k, &res_guard).expect("exists");

            for v in set.iter() {
                assert!(res_set.contains(&v))
            }
        }
    }

    #[test]
    fn map_single_string() {
        let index = TreeIndex::new();
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        let s = "some string example".to_string();
        index.insert(s.clone(), l).expect("is ok");

        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(&index);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, s);
        assert_eq!(v.link, l);
        assert_eq!(
            rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(),
            s.aligned_size() + l.aligned_size() + 8
        )
    }

    #[test]
    fn test_as_bytes() {
        let index = TreeIndex::new();
        for i in 0..1022 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l).expect("is ok");
        }
        let pages = map_unique_tree_index::<_, { INNER_PAGE_SIZE }>(&index);
        let page = pages.get(0).unwrap();

        let bytes = page.as_bytes();
        assert!(bytes.as_ref().len() <= INNER_PAGE_SIZE)
    }
}
