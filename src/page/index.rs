//! [`IndexPage`] definition.

use std::sync::Arc;

use rkyv::{Archive, Deserialize, Serialize};
use scc::ebr::Guard;
use scc::TreeIndex;
use crate::link::Link;
use crate::page::PAGE_SIZE;
use crate::util::SizeMeasurable;

/// Represents `key/value` pair of B-Tree index, where value is always
/// [`data::Link`], as it is represented in primary and secondary indexes.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct IndexValue<T> {
    pub key: T,
    pub link: Link
}

impl<T> SizeMeasurable for IndexValue<T>
where T: SizeMeasurable
{
    fn approx_size(&self) -> usize {
        self.key.approx_size() + self.link.approx_size()
    }
}

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct IndexPage<T> {
    pub index_values: Vec<IndexValue<T>>
}

// Manual `Default` implementation to avoid `T: Default`
impl<T> Default for IndexPage<T> {
    fn default() -> Self {
        Self {
            index_values: vec![]
        }
    }
}
pub fn map_tree_index<T, const PAGE_SIZE: usize>(index: Arc<TreeIndex<T, Link>>) -> Vec<IndexPage<T>>
where T: Clone + Ord + SizeMeasurable + 'static
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
        current_size += index_value.approx_size();
        if current_size > PAGE_SIZE {
            pages.push(current_page.clone());
            current_page.index_values.clear();
            current_size = 8 + index_value.approx_size()
        }
        current_page.index_values.push(index_value)
    }
    pages.push(current_page);

    pages
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use scc::TreeIndex;

    use crate::page::index::map_tree_index;
    use crate::page::PAGE_SIZE;
    use crate::Link;
    use crate::util::SizeMeasurable;

    #[test]
    fn map_single_value() {
        let index = Arc::new(TreeIndex::new());
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1u32, l).expect("is ok");

        let res = map_tree_index::<_, { PAGE_SIZE }>(index);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, 1);
        assert_eq!(v.link, l);
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(), 1u32.approx_size() + l.approx_size() + 8)
    }

    #[test]
    fn map_page_border() {
        let index = Arc::new(TreeIndex::new());
        for i in 0..1023 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l).expect("is ok");
        }

        let res = map_tree_index::<_, { PAGE_SIZE }>(index.clone());
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
        let res = map_tree_index::<_, { PAGE_SIZE }>(index.clone());
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].index_values.len(), 1023);
        assert_eq!(res[1].index_values.len(), 1);
        // As 16 + 8
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(), 16_376);
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[1]).unwrap().len(), 24);
    }

    #[test]
    fn map_single_string() {
        let index = Arc::new(TreeIndex::new());
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        let s = "some string example".to_string();
        index.insert(s.clone(), l).expect("is ok");

        let res = map_tree_index::<_, { PAGE_SIZE }>(index);
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, s);
        assert_eq!(v.link, l);
        assert_eq!(rkyv::to_bytes::<_, 0>(&res[0]).unwrap().len(), s.approx_size() + l.approx_size() + 8)
    }
}