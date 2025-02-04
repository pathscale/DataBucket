use crate::{Link, SizeMeasurable};
use crate::page::{IndexPage, IndexValue};

pub fn map_tree_index<'a, T, const PAGE_SIZE: usize>(
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

#[cfg(test)]
mod test {
    use indexset::concurrent::map::BTreeMap;
    use indexset::concurrent::multimap::BTreeMultiMap;

    use crate::page::{INNER_PAGE_SIZE, PAGE_SIZE};
    use crate::util::{Persistable, SizeMeasurable};
    use crate::Link;
    use crate::page::index::mappers::map_tree_index;

    #[test]
    fn map_single_value() {
        let index = BTreeMap::new();
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        index.insert(1u32, l);

        let res = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
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
        let index = BTreeMap::new();
        for i in 0..1023 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l);
        }

        let res = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
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
        index.insert(1024, l);
        let res = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
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
        let index = BTreeMap::new();
        for i in 0..1023 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l);
        }

        let pages = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
        let res_index = BTreeMap::new();

        for page in pages {
            page.append_to_unique_tree_index(&res_index)
        }

        assert_eq!(index.iter().collect::<Vec<_>>(), res_index.iter().collect::<Vec<_>>())
    }

    #[test]
    fn map_and_back() {
        let index = BTreeMultiMap::new();
        for i in 0..256 {
            for j in 0..4 {
                let l = Link {
                    page_id: j.into(),
                    offset: 0,
                    length: 32,
                };
                index.insert(i, l);
            }
        }

        let pages = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
        let res_index = BTreeMultiMap::new();

        for page in pages {
            page.append_to_tree_index(&res_index)
        }

        let mut vals = index.iter().collect::<Vec<_>>();

        for v in res_index.iter() {
            assert!(vals.contains(&v));
            let i = vals.iter().position(|n| n == &v).unwrap();
            vals.remove(i);
        }

        assert!(vals.is_empty())
    }

    #[test]
    fn map_single_string() {
        let index = BTreeMap::new();
        let l = Link {
            page_id: 1.into(),
            offset: 0,
            length: 32,
        };
        let s = "some string example".to_string();
        index.insert(s.clone(), l);

        let res = map_tree_index::<_, { PAGE_SIZE }>(index.iter());
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
        let index = BTreeMap::new();
        for i in 0..1022 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l);
        }
        let pages = map_tree_index::<_, { INNER_PAGE_SIZE }>(index.iter());
        let page = pages.get(0).unwrap();

        let bytes = page.as_bytes();
        assert!(bytes.as_ref().len() <= INNER_PAGE_SIZE)
    }
}