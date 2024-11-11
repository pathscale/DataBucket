//! [`Space`] type and related types declaration.

use derive_more::{Display, From};
use rkyv::{Archive, Deserialize, Serialize};

use crate::page;

/// [`Space`] represents whole [`WorkTable`] file.
#[derive(Debug, Default)]
pub struct Space {
    pub pages: Vec<page::General>,
}

/// Represents space's identifier.
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
pub struct Id(u32);
