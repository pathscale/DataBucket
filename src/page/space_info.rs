//! [`SpaceInfo`] declaration.
use std::collections::HashMap;

use crate::util::Persistable;
use crate::DataType;
use crate::{space, Link};
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Portable, Serialize};

pub type SpaceName = String;

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
#[repr(C)]
pub struct SpaceInfo<Pk = ()> {
    pub id: space::Id,
    pub page_count: u32,
    pub name: SpaceName,
    pub primary_key_intervals: Vec<Interval>,
    pub secondary_index_intervals: HashMap<String, Vec<Interval>>,
    pub data_intervals: Vec<Interval>,
    pub pk_gen_state: Pk,
    pub empty_links_list: Vec<Link>,
    pub secondary_index_map: HashMap<String, DataType>,
}

unsafe impl<Pk> Portable for SpaceInfo<Pk> where Pk: Portable {}

/// Represents some interval between values.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Interval(pub usize, pub usize);

impl Interval {
    pub fn contains(&self, interval: &Interval) -> bool {
        self.0 <= interval.0 && self.1 >= interval.1
    }
}

impl<Pk> Persistable for SpaceInfo<Pk>
where
    Pk: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::page::{SpaceInfo, INNER_PAGE_SIZE};
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
            secondary_index_map: HashMap::new(),
        };
        let bytes = info.as_bytes();
        assert!(bytes.as_ref().len() < INNER_PAGE_SIZE)
    }
}
