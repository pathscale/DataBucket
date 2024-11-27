//! [`SpaceInfo`] declaration.
use std::collections::HashMap;

use rkyv::ser::serializers::AllocSerializer;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::INNER_PAGE_SIZE;
use crate::util::Persistable;
use crate::{space, Link};

use super::PrimaryKeyGeneratorState;

pub type SpaceName = String;

// TODO: Minor. Add some schema description in `SpaceInfo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct SpaceInfo<Pk = ()>
where Pk: PrimaryKeyGeneratorState
{
    pub id: space::Id,
    pub page_count: u32,
    pub name: SpaceName,
    pub primary_key_intervals: Vec<Interval>,
    pub secondary_index_intervals: HashMap<String, Vec<Interval>>,
    pub data_intervals: Vec<Interval>,
    pub pk_gen_state: Pk,
    pub empty_links_list: Vec<Link>,
}

/// Represents some interval between values.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Interval(pub usize, pub usize);

impl<Pk> Persistable for SpaceInfo<Pk>
where
    Pk: Archive + Serialize<AllocSerializer<{ INNER_PAGE_SIZE }>> + PrimaryKeyGeneratorState,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<_, { INNER_PAGE_SIZE }>(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;

    use crate::page::{PrimaryKeyGenerator, SpaceInfo, INNER_PAGE_SIZE};
    use crate::util::Persistable;

    #[test]
    fn test_as_bytes() {
        let info = SpaceInfo {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            primary_key_intervals: vec![],
            secondary_index_intervals: HashMap::new(),
            data_intervals: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
        };
        let bytes = info.as_bytes();
        assert!(bytes.as_ref().len() < INNER_PAGE_SIZE)
    }

    #[test]
    fn test_pk_gen_state() {
        let info = SpaceInfo {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            primary_key_intervals: vec![],
            secondary_index_intervals: HashMap::new(),
            data_intervals: vec![],
            pk_gen_state: AtomicU64::new(0),
            empty_links_list: vec![],
        };
        assert_eq!(<AtomicU64 as PrimaryKeyGenerator<u64>>::next(&info.pk_gen_state), 0);
    }
}
