use derive_more::Display;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(
    Archive,
    Copy,
    Clone,
    Deserialize,
    Debug,
    Display,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    Serialize,
)]
#[archive_attr(repr(u16))]
#[repr(u16)]
pub enum PageType {
    /// Header for empty `Page`. Used in case page is just allocated.
    Empty = 0,
    /// Space header `Page` type.
    SpaceInfo = 1,
    /// Table data `Page` type.
    Data = 2,
    /// Index `Page` type.
    Index = 3,
}
