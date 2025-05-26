use std::io::SeekFrom;

use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::{seek_to_page_start, Link, Persistable, SizeMeasurable, GENERAL_HEADER_SIZE};

mod page;
mod page_cdc_impl;
mod page_for_unsized;
mod table_of_contents_page;

use crate::page::PageId;

pub use page::{get_index_page_size_from_data_length, IndexPage};
pub use page_for_unsized::{UnsizedIndexPage, UnsizedIndexPageUtility};
pub use table_of_contents_page::TableOfContentsPage;

pub trait IndexPageUtility<T> {
    type Utility: Persistable + Send + Sync;

    fn parse_index_page_utility(
        file: &mut File,
        page_id: PageId,
    ) -> impl std::future::Future<Output = eyre::Result<Self::Utility>> + Send;

    fn persist_index_page_utility(
        file: &mut File,
        page_id: PageId,
        utility: Self::Utility,
    ) -> impl std::future::Future<Output = eyre::Result<()>> + Send {
        async move {
            seek_to_page_start(file, page_id.0).await?;
            file.seek(SeekFrom::Current(GENERAL_HEADER_SIZE as i64))
                .await?;
            file.write_all(utility.as_bytes().as_ref()).await?;
            Ok(())
        }
    }
}

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
