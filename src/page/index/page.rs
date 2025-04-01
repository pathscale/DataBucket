//! [`crate::page::IndexPage`] definition.

use std::fmt::Debug;
use std::hash::Hash;
use std::io::SeekFrom;
use std::mem;

use data_bucket_codegen::Persistable;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::page::{IndexValue, PageId};
use crate::{
    align, align8, seek_to_page_start, Link, Persistable, SizeMeasurable, GENERAL_HEADER_SIZE,
};

pub fn get_index_page_size_from_data_length<T>(length: usize) -> usize
where
    T: Default + SizeMeasurable,
{
    let size_field_size = IndexPage::<T>::size_size();
    let node_id_size = IndexPage::<T>::node_id_size();
    let current_index_size = IndexPage::<T>::current_index_size();
    let current_length_size = IndexPage::<T>::current_length_size();
    let slot_size = IndexPage::<T>::slots_value_size();
    let index_value_size = IndexPage::<T>::index_values_value_size();
    let slots_vec_size = IndexPage::<T>::slots_size(0);
    let index_values_vec_size = IndexPage::<T>::slots_size(0);

    (length
        - node_id_size
        - size_field_size
        - current_index_size
        - current_length_size
        - slots_vec_size
        - index_values_vec_size)
        / (slot_size + index_value_size)
}

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Persistable,
)]
#[persistable(by_parts)]
pub struct IndexPage<T: Default + SizeMeasurable> {
    pub size: u16,
    pub node_id: T,
    pub current_index: u16,
    pub current_length: u16,
    pub slots: Vec<u16>,
    pub index_values: Vec<IndexValue<T>>,
}

#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Persistable,
)]
#[persistable(by_parts)]
pub struct IndexPageUtility<T: Default + SizeMeasurable> {
    pub size: u16,
    pub node_id: T,
    pub current_index: u16,
    pub current_length: u16,
    pub slots: Vec<u16>,
}

impl<T: Default + SizeMeasurable> IndexPage<T> {
    pub fn new(node_id: T, size: usize) -> Self
    where
        T: Clone,
    {
        let slots = vec![0u16; size];
        let index_values = vec![IndexValue::default(); size];
        Self {
            size: size as u16,
            node_id,
            current_index: 0,
            current_length: 0,
            slots,
            index_values,
        }
    }

    pub fn split(&mut self, index: usize) -> IndexPage<T>
    where
        T: Clone,
    {
        let mut new_page = IndexPage::new(self.node_id.clone(), self.size as usize);
        let mut first_empty_value = u16::MAX;
        for (index, slot) in self.slots[index..].iter_mut().enumerate() {
            if first_empty_value > *slot {
                first_empty_value = *slot;
            }
            let mut index_value = IndexValue::default();
            mem::swap(&mut self.index_values[*slot as usize], &mut index_value);
            new_page.index_values[index] = index_value;
            new_page.slots[index] = index as u16;
            new_page.current_index = (index + 1) as u16;
            *slot = 0;
        }
        new_page.current_length = self.current_length - index as u16;

        self.current_index = first_empty_value;
        self.node_id = self.index_values[self.slots[index - 1] as usize]
            .key
            .clone();
        self.current_length = index as u16;

        new_page
    }

    pub async fn parse_index_page_utility(
        file: &mut File,
        page_id: PageId,
    ) -> eyre::Result<IndexPageUtility<T>>
    where
        T: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0).await?;
        let offset = GENERAL_HEADER_SIZE as i64;
        file.seek(SeekFrom::Current(offset)).await?;

        let mut size_bytes = vec![0u8; IndexPageUtility::<T>::size_size()];
        file.read_exact(size_bytes.as_mut_slice()).await?;
        let archived = unsafe {
            rkyv::access_unchecked::<<u16 as Archive>::Archived>(
                &size_bytes[0..IndexPageUtility::<T>::size_size()],
            )
        };
        let size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        let index_utility_len = IndexPageUtility::<T>::persisted_size(size as usize);
        file.seek(SeekFrom::Current(
            -(IndexPageUtility::<T>::size_size() as i64),
        ))
        .await?;
        let mut index_utility_bytes = vec![0u8; index_utility_len];
        file.read_exact(index_utility_bytes.as_mut_slice()).await?;
        let utility = IndexPageUtility::<T>::from_bytes(&index_utility_bytes);

        Ok(utility)
    }

    pub async fn persist_index_page_utility(
        file: &mut File,
        page_id: PageId,
        utility: IndexPageUtility<T>,
    ) -> eyre::Result<()>
    where
        T: Archive
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0).await?;
        file.seek(SeekFrom::Current(GENERAL_HEADER_SIZE as i64))
            .await?;
        file.write_all(utility.as_bytes().as_ref()).await?;
        Ok(())
    }

    async fn read_value(file: &mut File) -> eyre::Result<IndexValue<T>>
    where
        T: Archive,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        let mut bytes = vec![0u8; IndexPage::<T>::index_values_value_size()];
        file.read_exact(bytes.as_mut_slice()).await?;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(bytes.as_slice());
        let archived =
            unsafe { rkyv::access_unchecked::<<IndexValue<T> as Archive>::Archived>(&v[..]) };
        Ok(rkyv::deserialize(archived).expect("data should be valid"))
    }

    pub async fn read_value_with_index(
        file: &mut File,
        page_id: PageId,
        size: usize,
        index: usize,
    ) -> eyre::Result<IndexValue<T>>
    where
        T: Archive,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0).await?;
        let offset = Self::get_value_offset(size, index);
        file.seek(SeekFrom::Current(offset as i64)).await?;
        Self::read_value(file).await
    }

    fn get_value_offset(size: usize, value_index: usize) -> usize
    where
        T: Default + SizeMeasurable,
    {
        let mut offset = GENERAL_HEADER_SIZE;
        offset += IndexPage::<T>::size_size();
        offset += IndexPage::<T>::node_id_size();
        offset += IndexPage::<T>::current_index_size();
        offset += IndexPage::<T>::current_length_size();
        offset += IndexPage::<T>::slots_size(size);
        offset += value_index * IndexPage::<T>::index_values_value_size();

        offset
    }

    pub async fn persist_value(
        file: &mut File,
        page_id: PageId,
        size: usize,
        value: IndexValue<T>,
        mut value_index: u16,
    ) -> eyre::Result<u16>
    where
        T: Archive
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0).await?;

        let offset = Self::get_value_offset(size, value_index as usize);
        file.seek(SeekFrom::Current(offset as i64)).await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        file.write_all(bytes.as_slice()).await?;

        if value_index != size as u16 - 1 {
            let mut value = Self::read_value(file).await?;
            while value != IndexValue::default() {
                value_index += 1;
                value = Self::read_value(file).await?;
            }
        }

        Ok(value_index + 1)
    }

    pub async fn remove_value(
        file: &mut File,
        page_id: PageId,
        size: usize,
        value_index: u16,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + SizeMeasurable
            + Eq
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0).await?;

        let offset = Self::get_value_offset(size, value_index as usize);
        file.seek(SeekFrom::Current(offset as i64)).await?;
        let value = IndexValue::<T>::default();
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        file.write_all(bytes.as_slice()).await?;

        Ok(())
    }

    pub fn get_node(&self) -> Vec<Pair<T, Link>>
    where
        T: Clone + Ord,
    {
        let mut node = Vec::with_capacity(self.current_length as usize);
        for slot in &self.slots[..self.current_length as usize] {
            node.push(self.index_values[*slot as usize].clone().into())
        }
        node
    }

    pub fn from_node(node: &[impl Into<IndexValue<T>> + Clone], size: usize) -> Self
    where
        T: Clone + Ord + Default,
    {
        let mut page = IndexPage::new(
            Into::<IndexValue<T>>::into(
                node.last()
                    .expect("should contain at least one key")
                    .clone(),
            )
            .key,
            size,
        );

        for (i, pair) in node.iter().enumerate() {
            page.index_values[i] = Into::<IndexValue<T>>::into(pair.clone());
            page.slots[i] = i as u16;
        }
        page.current_index = node.len() as u16;
        page.current_length = node.len() as u16;

        page
    }
}

#[cfg(test)]
mod tests {
    use crate::page::IndexValue;
    use crate::{
        align8, get_index_page_size_from_data_length, IndexPage, Link, Persistable, SizeMeasurable,
        INNER_PAGE_SIZE,
    };

    #[test]
    fn test_bytes() {
        let size: usize = get_index_page_size_from_data_length::<u64>(INNER_PAGE_SIZE);
        let page = IndexPage::<u64>::new(1, size);
        let bytes = page.as_bytes();
        let new_page = IndexPage::<u64>::from_bytes(bytes.as_ref());

        assert_eq!(new_page.node_id, page.node_id);
        assert_eq!(new_page.current_index, page.current_index);
        assert_eq!(new_page.size, page.size);
        assert_eq!(new_page.slots, page.slots);
        assert_eq!(new_page.index_values, page.index_values);
    }

    #[test]
    fn test_split() {
        let mut page = IndexPage::<u64>::new(7, 8);
        page.slots = vec![0, 1, 2, 3, 4, 5, 6, 7];
        page.current_index = 8;
        page.current_length = 8;
        page.index_values = {
            let mut v = vec![];
            for i in &page.slots {
                v.push(IndexValue {
                    key: *i as u64,
                    link: Default::default(),
                })
            }
            v
        };

        let split = page.split(4);
        assert_eq!(page.current_index, 4);
        assert_eq!(page.current_length, 4);
        assert_eq!(page.slots[page.current_index as usize], 0);

        assert_eq!(page.index_values[0].key, 0);
        assert_eq!(page.index_values[1].key, 1);
        assert_eq!(page.index_values[2].key, 2);
        assert_eq!(page.index_values[3].key, 3);

        assert_eq!(split.current_index, 4);
        assert_eq!(split.current_length, 4);
        assert_eq!(split.slots[0], 0);
        assert_eq!(split.slots[1], 1);
        assert_eq!(split.slots[2], 2);
        assert_eq!(split.slots[3], 3);

        assert_eq!(split.index_values[0].key, 4);
        assert_eq!(split.index_values[1].key, 5);
        assert_eq!(split.index_values[2].key, 6);
        assert_eq!(split.index_values[3].key, 7);
    }
}
