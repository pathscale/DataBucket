//! [`GeneralHeader`] definitions.

use rkyv::{Archive, Deserialize, Serialize};

use crate::page;
use crate::page::r#type::PageType;
use crate::space;

/// Header that appears on every page before it's inner data.
#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct GeneralHeader {
    pub page_id: page::Id,
    pub previous_id: page::Id,
    pub next_id: page::Id,
    pub page_type: PageType,
    pub space_id: space::Id,
}

impl GeneralHeader {
    pub fn new(page_id: page::Id, type_: PageType, space_id: space::Id) -> Self {
        Self {
            page_id,
            previous_id: 0.into(),
            next_id: 0.into(),
            page_type: type_,
            space_id,
        }
    }

    /// Creates a new [`GeneralHeader`] for a page that follows page with given
    /// header. It means that [`PageType`] and [`space::Id`] are same and
    /// old [`page::Id`] will be `previous_id`.
    pub fn follow(&self) -> Self {
        Self {
            page_id: self.page_id.next(),
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type: self.page_type,
            space_id: self.space_id,
        }
    }

    /// Creates a new [`GeneralHeader`] for a page that follows page with given
    /// header but with different [`PageType`]. [`space::Id`] is same and old
    /// [`page::Id`] will be `previous_id`.
    pub fn follow_with(&self, page_type: PageType) -> Self {
        Self {
            page_id: self.page_id.next(),
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type,
            space_id: self.space_id,
        }
    }
}
