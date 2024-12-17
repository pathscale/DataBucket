//! [`GeneralHeader`] definitions.

use rkyv::{Archive, Deserialize, Serialize};

use crate::page::ty::PageType;
use crate::space;
use crate::util::Persistable;
use crate::{page, PAGE_SIZE};

pub const DATA_VERSION: u32 = 1u32;

/// Header that appears on every page before it's inner data.
#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct GeneralHeader {
    pub data_version: u32,
    pub space_id: space::Id,
    pub page_id: page::PageId,
    pub previous_id: page::PageId,
    pub next_id: page::PageId,
    pub page_type: PageType,
    pub data_length: u32,
}

impl GeneralHeader {
    pub fn new(page_id: page::PageId, type_: PageType, space_id: space::Id) -> Self {
        Self {
            data_version: DATA_VERSION,
            page_id,
            previous_id: 0.into(),
            next_id: 0.into(),
            page_type: type_,
            space_id,
            data_length: PAGE_SIZE as u32,
        }
    }

    /// Creates a new [`GeneralHeader`] for a page that follows page with given
    /// header. It means that [`PageType`] and [`space::Id`] are same and
    /// old [`page::PageId`] will be `previous_id`.
    pub fn follow(&mut self) -> Self {
        self.next_id = self.page_id.next();
        Self {
            data_version: DATA_VERSION,
            page_id: self.next_id,
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type: self.page_type,
            space_id: self.space_id,
            data_length: PAGE_SIZE as u32,
        }
    }

    /// Creates a new [`GeneralHeader`] for a page that follows page with given
    /// header but with different [`PageType`]. [`space::Id`] is same and old
    /// [`page::PageId`] will be `previous_id`.
    pub fn follow_with(&mut self, page_type: PageType) -> Self {
        self.next_id = self.page_id.next();
        Self {
            data_version: DATA_VERSION,
            page_id: self.next_id,
            previous_id: self.page_id,
            next_id: 0.into(),
            page_type,
            space_id: self.space_id,
            data_length: PAGE_SIZE as u32,
        }
    }
}

impl Persistable for GeneralHeader {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

#[cfg(test)]
mod test {
    use crate::page::header::DATA_VERSION;
    use crate::util::Persistable;
    use crate::{GeneralHeader, PageType, GENERAL_HEADER_SIZE, PAGE_SIZE};

    #[test]
    fn test_data_version() {
        let header = GeneralHeader::new(1.into(), PageType::Empty, 2.into());
        assert_eq!(header.data_version, 1u32);
    }

    #[test]
    fn test_as_bytes() {
        let header = GeneralHeader {
            data_version: DATA_VERSION,
            page_id: 1.into(),
            previous_id: 2.into(),
            next_id: 3.into(),
            page_type: PageType::Empty,
            space_id: 4.into(),
            data_length: PAGE_SIZE as u32,
        };
        let bytes = header.as_bytes();
        assert_eq!(bytes.as_ref().len(), GENERAL_HEADER_SIZE)
    }

    #[test]
    fn test_as_bytes_max() {
        let header = GeneralHeader {
            data_version: DATA_VERSION,
            page_id: u32::MAX.into(),
            previous_id: (u32::MAX - 1).into(),
            next_id: (u32::MAX - 2).into(),
            page_type: PageType::Empty,
            space_id: (u32::MAX - 3).into(),
            data_length: PAGE_SIZE as u32,
        };
        let bytes = header.as_bytes();
        assert_eq!(bytes.as_ref().len(), GENERAL_HEADER_SIZE)
    }
}
