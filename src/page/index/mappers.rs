use std::sync::Arc;

use crate::{Link, SizeMeasurable};
use crate::page::{IndexPage, IndexValue};

pub fn map_unique_tree_index<'a, T, const PAGE_SIZE: usize>(
    index: impl Iterator<Item = (&'a T, &'a Link)>,
) -> Vec<IndexPage<T>>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    let mut pages = vec![];
    let mut current_page = IndexPage::default();
    let mut current_size = 8;

    for (key, link) in index {
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
    pages.push(current_page);

    pages
}

pub fn map_tree_index<'a, T, const PAGE_SIZE: usize>(
    index: impl Iterator<Item = (&'a T, &'a Arc<lockfree::set::Set<Link>>)>,
) -> Vec<IndexPage<T>>
where
    T: Clone + Ord + SizeMeasurable + 'static,
{
    let mut pages = vec![];
    let mut current_page = IndexPage::default();
    let mut current_size = 8;

    for (key, links) in index {
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

#[cfg(test)]
mod test {
    use scc::ebr::Guard;
    use scc::TreeIndex;
    use std::sync::Arc;

    use crate::page::{INNER_PAGE_SIZE, PAGE_SIZE};
    use crate::util::{Persistable, SizeMeasurable};
    use crate::Link;
    use crate::page::index::mappers::{map_tree_index, map_unique_tree_index};

    #[test]
    fn map_single_value() {
        let index = TreeIndex::new();
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1u32, l).expect("is ok");

        let guard = Guard::new();
        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, 1);
        assert_eq!(v.link, l);
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&res[0])
                .unwrap()
                .len(),
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

        let guard = Guard::new();
        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1023);
        // As 1023 * 16 + 8
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&res[0])
                .unwrap()
                .len(),
            16_376
        );

        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1024, l).expect("is ok");
        let guard = Guard::new();
        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].index_values.len(), 1023);
        assert_eq!(res[1].index_values.len(), 1);
        // As 16 + 8
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&res[0])
                .unwrap()
                .len(),
            16_376
        );
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&res[1])
                .unwrap()
                .len(),
            24
        );
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

        let guard = Guard::new();
        let pages = map_unique_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
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

        let guard = Guard::new();
        let pages = map_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
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

        let guard = Guard::new();
        let res = map_unique_tree_index::<_, { PAGE_SIZE }>(index.iter(&guard));
        assert_eq!(res.len(), 1);
        assert_eq!(res[0].index_values.len(), 1);
        let v = &res[0].index_values[0];
        assert_eq!(v.key, s);
        assert_eq!(v.link, l);
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&res[0])
                .unwrap()
                .len(),
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
        let guard = Guard::new();
        let pages = map_unique_tree_index::<_, { INNER_PAGE_SIZE }>(index.iter(&guard));
        let page = pages.get(0).unwrap();

        let bytes = page.as_bytes();
        assert!(bytes.as_ref().len() <= INNER_PAGE_SIZE)
    }
}