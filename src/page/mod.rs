mod data;
mod header;
mod index;
mod page;
mod space_info;
mod ty;

use derive_more::{Display, From};
use rkyv::{with::Skip, Archive, Deserialize, Serialize};

use crate::page::header::GeneralHeader;

pub use space_info::SpaceInfo;

// TODO: Move to config
/// The size of a page. Header size and other parts are _included_ in this size.
/// That's exact page size.
pub const PAGE_SIZE: usize = 4096 * 4;

/// Length of [`GeneralHeader`].
///
/// ## Rkyv representation
///
/// Length of the values are:
///
/// * `page_id` - 4 bytes,
/// * `previous_id` - 4 bytes,
/// * `next_id` - 4 bytes,
/// * `page_type` - 2 bytes,
/// * `space_id` - 4 bytes,
///
/// **2 bytes are added by rkyv implicitly.**
pub const HEADER_LENGTH: usize = 20;

/// Length of the inner part of [`General`] page. It's counted as [`PAGE_SIZE`]
/// without [`General`] page [`HEADER_LENGTH`].
pub const INNER_PAGE_LENGTH: usize = PAGE_SIZE - HEADER_LENGTH;

/// Represents page's identifier. Is unique within the table bounds
#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Display,
    Eq,
    From,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
pub struct PageId(u32);

impl PageId {
    pub fn next(self) -> Self {
        PageId(self.0 + 1)
    }
}

impl From<PageId> for usize {
    fn from(value: PageId) -> Self {
        value.0 as usize
    }
}

/// General page representation.
#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct General<Inner = Empty> {
    pub header: GeneralHeader,
    pub inner: Inner,
}

/// Empty page. It's default allocated page.
#[derive(
    Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
pub struct Empty {
    #[with(Skip)]
    pub page_id: PageId,

    pub bytes: [u8; INNER_PAGE_LENGTH],
}

impl Empty {
    pub fn new(id: PageId) -> Self {
        Self {
            page_id: id,
            bytes: [0; PAGE_SIZE - HEADER_LENGTH],
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page;
    use crate::page::ty::PageType;
    use crate::page::{GeneralHeader, HEADER_LENGTH, INNER_PAGE_LENGTH, PAGE_SIZE};

    fn get_general_header() -> GeneralHeader {
        GeneralHeader {
            page_id: 1.into(),
            previous_id: 2.into(),
            next_id: 4.into(),
            page_type: PageType::Index,
            space_id: 5.into(),
        }
    }

    fn get_general_page() -> page::General {
        page::General {
            header: get_general_header(),
            inner: page::Empty::new(1.into()),
        }
    }

    #[test]
    fn general_header_length_valid() {
        let header = get_general_header();
        let bytes = rkyv::to_bytes::<_, 32>(&header).unwrap();

        assert_eq!(bytes.len(), HEADER_LENGTH)
    }

    #[test]
    fn general_empty_page_valid() {
        let page = get_general_page();
        let bytes = rkyv::to_bytes::<_, 4096>(&page).unwrap();

        assert_eq!(bytes.len(), PAGE_SIZE)
    }

    #[test]
    fn empty_page_valid() {
        let page = page::Empty::new(1.into());
        let bytes = rkyv::to_bytes::<_, 4096>(&page).unwrap();

        assert_eq!(bytes.len(), INNER_PAGE_LENGTH)
    }
}
