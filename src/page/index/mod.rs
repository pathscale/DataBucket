use crate::{Link, SizeMeasurable};
use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};

mod page;
mod table_of_contents_page;

pub use page::{get_index_page_size_from_data_length, IndexPage};
pub use table_of_contents_page::TableOfContentsPage;

/// Represents `key/value` pair of B-Tree index, where value is always
/// [`data::Link`], as it is represented in primary and secondary indexes.
#[derive(
    Archive, Clone, Deserialize, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
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
where
    T: Ord,
{
    fn from(value: IndexValue<T>) -> Self {
        Pair {
            key: value.key,
            value: value.link,
        }
    }
}

impl<T> From<Pair<T, Link>> for IndexValue<T>
where
    T: Ord,
{
    fn from(pair: Pair<T, Link>) -> Self {
        IndexValue {
            key: pair.key,
            link: pair.value,
        }
    }
}

impl<T> From<MultiPair<T, Link>> for IndexValue<T>
where
    T: Ord,
{
    fn from(pair: MultiPair<T, Link>) -> Self {
        IndexValue {
            key: pair.key,
            link: pair.value,
        }
    }
}
