use rkyv::{Archive, Deserialize, Serialize};

use crate::{Link, SizeMeasurable};

mod page;
mod mappers;
mod table_of_contents_page;

pub use page::IndexPage;
pub use table_of_contents_page::TableOfContentsPage;
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