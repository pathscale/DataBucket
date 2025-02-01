use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};

use crate::{Link, SizeMeasurable};

mod page;
mod mappers;
mod table_of_contents_page;
mod new_page;

pub use page::IndexPage;
pub use new_page::NewIndexPage;
pub use table_of_contents_page::TableOfContentsPage;
pub use mappers::{map_tree_index, map_unique_tree_index};

/// Represents `key/value` pair of B-Tree index, where value is always
/// [`data::Link`], as it is represented in primary and secondary indexes.
#[derive(Archive, Clone, Deserialize, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
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

impl<T> From<IndexValue<T>> for Pair<T, Link>
where T: Ord
{
    fn from(value: IndexValue<T>) -> Self {
        Pair {
            key: value.key,
            value: value.link,
        }
    }
}