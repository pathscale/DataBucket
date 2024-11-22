//! [`SpaceInfo`] declaration.
use std::collections::HashMap;

use rkyv::{Archive, Deserialize, Serialize};

use crate::page::ty::PageType;
use crate::page::GeneralHeader;
use crate::util::Persistable;
use crate::{page, space};

use super::PAGE_SIZE;

pub type SpaceName = String;

// TODO: This must be modified to describe table structure. I think page intervals
//       can describe what lays in them. Like page 2-3 is primary index, 3 secondary1,
//       4-... data pages, so we need some way to describe this.

// TODO: Test all pages united in one file, start from basic situation with just
//       3 pages: info, primary index and data. And then try to modify this more.

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(Archive, Clone, Deserialize, Debug, PartialEq, Serialize)]
pub struct SpaceInfo {
    pub id: space::Id,
    pub page_count: u32,
    pub name: SpaceName,
    pub primary_key_intervals: Vec<Interval>,
    pub secondary_index_intervals: HashMap<String, Vec<Interval>>,
    pub data_intervals: Vec<Interval>,
}

/// Represents some interval between values.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Interval(pub usize, pub usize);

impl From<SpaceInfo> for page::General<SpaceInfo> {
    fn from(info: SpaceInfo) -> Self {
        let header = GeneralHeader {
            page_id: page::PageId::from(0),
            previous_id: page::PageId::from(0),
            next_id: page::PageId::from(0),
            page_type: PageType::SpaceInfo,
            space_id: info.id,
            data_length: PAGE_SIZE as u32,
        };
        page::General {
            header,
            inner: info,
        }
    }
}

impl Persistable for SpaceInfo {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use crate::page::{SpaceInfo, INNER_PAGE_LENGTH};
    use crate::util::Persistable;

    #[test]
    fn test_as_bytes() {
        let info = SpaceInfo {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            primary_key_intervals: vec![],
            secondary_index_intervals: HashMap::new(),
            data_intervals: vec![]
        };
        let bytes = info.as_bytes();
        assert!(bytes.as_ref().len() < INNER_PAGE_LENGTH)
    }
}
