//! [`SpaceInfo`] declaration.

use rkyv::{Archive, Deserialize, Serialize};

use crate::{page, space};
use crate::page::GeneralHeader;
use crate::page::r#type::PageType;

pub type SpaceName = String;

// TODO: This must be modified to describe table structure. I think page intervals
//       can describe what lays in them. Like page 2-3 is primary index, 3 secondary1,
//       4-... data pages, so we need some way to describe this.

// TODO: Test all pages united in one file, start from basic situation with just
//       3 pages: info, primary index and data. And then try to modify this more.

// TODO: Minor. Add some schema description in `SpaceIndo`

/// Internal information about a `Space`. Always appears first before all other
/// pages in a `Space`.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct SpaceInfo {
    pub id: space::Id,
    pub page_count: u32,
    pub name: SpaceName,
    pub primary_key_intervals: Vec<Interval>
}

/// Represents some interval between values.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Interval(usize, usize);

impl From<SpaceInfo> for page::General<SpaceInfo> {
    fn from(info: SpaceInfo) -> Self {
        let header = GeneralHeader {
            page_id: page::Id::from(0),
            previous_id: page::Id::from(0),
            next_id: page::Id::from(0),
            page_type: PageType::SpaceInfo,
            space_id: info.id,
        };
        page::General {
            header,
            inner: info,
        }
    }
}