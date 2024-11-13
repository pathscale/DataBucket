//! [`GeneralHeader`] definitions.

use rkyv::{Archive, Deserialize, Serialize};

use crate::page;
use crate::page::ty::PageType;
use crate::space;
pub const PAGE_HEADER_SIZE: usize = 32;

/// Header that appears on every page before it's inner data.
#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct GeneralHeader {
    pub page_id: page::PageId,
    pub previous_id: page::PageId,
    pub next_id: page::PageId,
    pub page_type: PageType,
    pub space_id: space::Id,
}

impl GeneralHeader {
    pub fn new(page_id: page::PageId, type_: PageType, space_id: space::Id) -> Self {
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
    /// old [`page::PageId`] will be `previous_id`.
    pub fn follow(&mut self) -> Self {
        self.next_id = self.page_id.next();
        Self {
            page_id: self.next_id,
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type: self.page_type,
            space_id: self.space_id,
        }
    }

    /// Creates a new [`GeneralHeader`] for a page that follows page with given
    /// header but with different [`PageType`]. [`space::Id`] is same and old
    /// [`page::PageId`] will be `previous_id`.
    pub fn follow_with(&mut self, page_type: PageType) -> Self {
        self.next_id = self.page_id.next();
        Self {
            page_id: self.next_id,
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type,
            space_id: self.space_id,
        }
    }
}
