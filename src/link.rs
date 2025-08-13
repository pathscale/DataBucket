use rkyv::{Archive, Deserialize, Serialize};

use crate::page;

pub const LINK_LENGTH: usize = 12;

#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[rkyv(derive(Debug, PartialOrd, PartialEq, Eq, Ord))]
pub struct Link {
    pub page_id: page::PageId,
    pub offset: u32,
    pub length: u32,
}

impl Link {
    /// Unites two [`Link`]'s into one if they have same border. If [`Link`]'s
    /// could not be united, `None` returned.
    pub fn unite(self, other: Link) -> Option<Link> {
        if self.offset + self.length != other.offset || self.page_id != other.page_id {
            None
        } else {
            Some(Link {
                page_id: self.page_id,
                offset: self.offset,
                length: self.length + other.length,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::link::Link;
    use crate::link::LINK_LENGTH;

    #[test]
    fn link_length_valid() {
        let link = Link {
            page_id: 1.into(),
            offset: 10,
            length: 20,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&link).unwrap();

        assert_eq!(bytes.len(), LINK_LENGTH)
    }
}
