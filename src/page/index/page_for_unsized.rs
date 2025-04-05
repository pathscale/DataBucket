use data_bucket_codegen::Persistable;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::align8;
use crate::page::index::IndexPageUtility;
use crate::page::PageId;
use crate::Persistable;
use crate::{seek_to_page_start, IndexValue, SizeMeasurable, GENERAL_HEADER_SIZE};

#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct UnsizedIndexPage<T: Default + SizeMeasurable, const DATA_LENGTH: u32> {
    pub slots_size: u16,
    pub node_id_size: u16,
    pub node_id: T,
    pub last_value_offset: u32,
    pub slots: Vec<(u32, u16)>,
    pub index_values: Vec<IndexValue<T>>,
}

#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Persistable,
)]
#[persistable(by_parts)]
pub struct UnsizedIndexPageUtility<T: Default + SizeMeasurable> {
    pub slots_size: u16,
    pub node_id_size: u16,
    pub node_id: T,
    pub last_value_offset: u32,
    pub slots: Vec<(u32, u16)>,
}

impl<T: Default + SizeMeasurable, const DATA_LENGTH: u32> IndexPageUtility<T>
    for UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    type Utility = UnsizedIndexPageUtility<T>;

    async fn parse_index_page_utility(
        file: &mut File,
        page_id: PageId,
    ) -> eyre::Result<Self::Utility> {
        seek_to_page_start(file, page_id.0).await?;
        let offset = GENERAL_HEADER_SIZE as i64;
        file.seek(SeekFrom::Current(offset)).await?;

        let mut slot_size_bytes = vec![0u8; UnsizedIndexPageUtility::<T>::slots_size_size()];
        file.read_exact(slot_size_bytes.as_mut_slice()).await?;
        let archived = unsafe {
            rkyv::access_unchecked::<<u16 as Archive>::Archived>(
                &slot_size_bytes[0..UnsizedIndexPageUtility::<T>::slots_size_size()],
            )
        };
        let slots_size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        let mut node_id_size_bytes = vec![0u8; UnsizedIndexPageUtility::<T>::node_id_size_size()];
        file.read_exact(node_id_size_bytes.as_mut_slice()).await?;
        let archived = unsafe {
            rkyv::access_unchecked::<<u16 as Archive>::Archived>(
                &node_id_size_bytes[0..UnsizedIndexPageUtility::<T>::node_id_size_size()],
            )
        };
        let node_id_size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        let index_utility_len = UnsizedIndexPageUtility::<T>::persisted_size(
            slots_size as usize,
            node_id_size as usize,
        );
        file.seek(SeekFrom::Current(
            -(UnsizedIndexPageUtility::<T>::slots_size_size() as i64
                + UnsizedIndexPageUtility::<T>::node_id_size_size() as i64),
        ))
        .await?;
        let mut index_utility_bytes = vec![0u8; index_utility_len];
        file.read_exact(index_utility_bytes.as_mut_slice()).await?;
        let utility = UnsizedIndexPageUtility::<T>::from_bytes(&index_utility_bytes);

        Ok(utility)
    }
}

impl<T, const DATA_LENGTH: u32> UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + Default
        + SizeMeasurable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    pub fn new(node_id: T, value: IndexValue<T>) -> eyre::Result<Self> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        let len = bytes.len() as u32;
        Ok(Self {
            slots_size: 1,
            node_id_size: node_id.aligned_size() as u16,
            node_id,
            last_value_offset: len,
            slots: vec![(len, len as u16)],
            index_values: vec![value],
        })
    }

    pub async fn persist_value(
        file: &mut File,
        page_id: PageId,
        current_offset: u32,
        value: IndexValue<T>,
    ) -> eyre::Result<u32>
    where
        T: Archive
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        // We seek to page's end and will write values from tail.
        seek_to_page_start(file, page_id.0 + 1).await?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        let offset = current_offset + bytes.len() as u32;
        file.seek(SeekFrom::Current(offset as i64)).await?;
        file.write_all(bytes.as_slice()).await?;

        Ok(offset)
    }
}

impl<T, const DATA_LENGTH: u32> Persistable for UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + Clone
        + Default
        + SizeMeasurable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> + Send {
        let data_length = DATA_LENGTH as usize;
        let utility = UnsizedIndexPageUtility {
            slots_size: self.slots_size,
            node_id_size: self.node_id_size,
            node_id: self.node_id.clone(),
            last_value_offset: self.last_value_offset,
            slots: self.slots.clone(),
        };
        let utility_bytes = utility.as_bytes();
        let utility_bytes = utility_bytes.as_ref().to_vec();
        let utility_len = utility_bytes.len();
        let mut bytes = vec![0u8; data_length];
        bytes.splice(0..utility_len, utility_bytes.iter().map(|v| *v));

        for ((offset, len), value) in self.slots.iter().zip(self.index_values.iter()) {
            let offset = data_length - *offset as usize;
            let len = *len as usize;
            let value_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
            bytes.splice(offset..(offset + len), value_bytes.iter().map(|v| *v));
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let slots_size_bytes = &bytes[0..UnsizedIndexPageUtility::<T>::slots_size_size()];
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&slots_size_bytes) };
        let slots_size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        let node_id_size_bytes = &bytes[UnsizedIndexPageUtility::<T>::slots_size_size()
            ..UnsizedIndexPageUtility::<T>::node_id_size_size()
                + UnsizedIndexPageUtility::<T>::node_id_size_size()];
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&node_id_size_bytes) };
        let node_id_size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        let utility_len = UnsizedIndexPageUtility::<T>::persisted_size(
            slots_size as usize,
            node_id_size as usize,
        );
        let utility = UnsizedIndexPageUtility::<T>::from_bytes(&bytes[0..utility_len]);
        let mut index_values = Vec::with_capacity(utility.slots.len());
        for (offset, len) in &utility.slots {
            let offset = bytes.len() - *offset as usize;
            let len = *len as usize;
            let value_bytes = &bytes[offset..(offset + len)];
            let archived = unsafe {
                rkyv::access_unchecked::<<IndexValue<T> as Archive>::Archived>(value_bytes)
            };
            let val = rkyv::deserialize::<_, rkyv::rancor::Error>(archived)
                .expect("data should be valid");
            index_values.push(val)
        }

        Self {
            slots_size,
            node_id_size,
            node_id: utility.node_id,
            last_value_offset: utility.last_value_offset,
            slots: utility.slots,
            index_values,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{IndexValue, Link, Persistable, UnsizedIndexPage};

    #[test]
    fn to_bytes_and_back() {
        let value = IndexValue {
            key: "Something for Someone".to_string(),
            link: Link {
                page_id: 0.into(),
                offset: 0,
                length: 40,
            },
        };
        let page =
            UnsizedIndexPage::<_, 1024>::new("Something for Someone".to_string(), value).unwrap();
        let bytes = page.as_bytes();
        assert_eq!(bytes.as_ref().len(), 1024);
        let page_back = UnsizedIndexPage::from_bytes(bytes.as_ref());
        assert_eq!(page_back, page)
    }
}
