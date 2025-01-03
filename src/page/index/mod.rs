use rkyv::{Archive, Deserialize, Serialize};

use crate::{Link, SizeMeasurable};

mod data_page;
mod mappers;

pub use data_page::IndexPage;
pub use mappers::{map_tree_index, map_unique_tree_index};

/// Represents `key/value` pair of B-Tree index, where value is always
/// [`data::Link`], as it is represented in primary and secondary indexes.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexValue<T> {
    pub key: T,
    pub link: Link,
}

impl<T> SizeMeasurable for IndexValue<T>
where
    T: SizeMeasurable,
{
    fn aligned_size(&self) -> usize {
        self.key.aligned_size() + self.link.aligned_size()
    }
}