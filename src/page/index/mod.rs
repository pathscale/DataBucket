use std::fmt::Debug;
use std::io::SeekFrom;

use indexset::core::multipair::MultiPair;
use indexset::core::pair::Pair;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use crate::{
    align, align8, seek_to_page_start, Link, Persistable, SizeMeasurable, VariableSizeMeasurable,
    GENERAL_HEADER_SIZE,
};

mod page;
mod page_cdc_impl;
mod page_for_unsized;
mod page_for_unsized_cdc_impl;
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
        if let Some(align) = T::align() {
            if align % 8 == 0 {
                return align8(self.key.aligned_size() + self.link.aligned_size());
            }
        }
        align(self.key.aligned_size() + self.link.aligned_size())
    }
}

impl<T> VariableSizeMeasurable for IndexValue<T>
where
    T: VariableSizeMeasurable,
{
    fn aligned_size(length: usize) -> usize {
        length
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

impl<T, L> From<Pair<T, L>> for IndexValue<T>
where
    T: Ord,
    L: Into<Link>,
{
    fn from(pair: Pair<T, L>) -> Self {
        IndexValue {
            key: pair.key,
            link: pair.value.into(),
        }
    }
}

impl<T, L> From<MultiPair<T, L>> for IndexValue<T>
where
    T: Ord,
    L: Into<Link>,
{
    fn from(pair: MultiPair<T, L>) -> Self {
        IndexValue {
            key: pair.key,
            link: pair.value.into(),
        }
    }
}
