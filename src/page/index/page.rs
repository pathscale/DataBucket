//! [`crate::page::IndexPage`] definition.

use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;

use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize, Serialize};

use crate::page::{IndexValue, PageId};
use crate::{
    align, align8, seek_to_page_start, Link, Persistable, SizeMeasurable, GENERAL_HEADER_SIZE,
};

pub fn get_index_page_size_from_data_length<T>(length: usize) -> usize
where
    T: Default + SizeMeasurable,
{
    let node_id_size = T::default().aligned_size();
    let slot_size = u16::default().aligned_size();
    let index_value_size = align8(T::default().aligned_size() + Link::default().aligned_size());
    let vec_util_size = 8;
    let size = (length - node_id_size - slot_size * 3 - vec_util_size * 2)
        / (slot_size + index_value_size);
    size
}

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexPage<T> {
    pub size: u16,
    pub node_id: T,
    pub current_index: u16,
    pub current_length: u16,
    pub slots: Vec<u16>,
    pub index_values: Vec<IndexValue<T>>,
}

#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct IndexPageUtility<T> {
    pub node_id: T,
    pub current_index: u16,
    pub current_length: u16,
    pub slots: Vec<u16>,
}

impl<T> IndexPage<T> {
    pub fn new(node_id: T, size: usize) -> Self
    where
        T: Default + Clone,
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
        T: Clone + Default,
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

    fn index_page_utility_length(size: usize) -> usize
    where
        T: Default + SizeMeasurable,
    {
        T::default().aligned_size()
            + u16::default().aligned_size()
            + u16::default().aligned_size()
            + align(size * u16::default().aligned_size())
            + 8
    }

    fn get_index_page_utility_from_bytes(bytes: &[u8]) -> IndexPageUtility<T>
    where
        T: Archive + Default + SizeMeasurable,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        let t_size = T::default().aligned_size();
        let mut offset = 0;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + t_size]);
        let archived = unsafe { rkyv::access_unchecked::<<T as Archive>::Archived>(&v[..]) };
        let node_id = rkyv::deserialize(archived).expect("data should be valid");

        offset = t_size;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + 2]);
        let archived = unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&v[..]) };
        let current_index =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        offset = offset + 2;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + 2]);
        let archived = unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&v[..]) };
        let current_length =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        offset = offset + 2;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..]);
        let archived = unsafe { rkyv::access_unchecked::<<Vec<u16> as Archive>::Archived>(&v[..]) };
        let slots = rkyv::deserialize::<Vec<u16>, rkyv::rancor::Error>(archived)
            .expect("data should be valid");

        IndexPageUtility {
            node_id,
            current_index,
            current_length,
            slots,
        }
    }

    pub fn parse_index_page_utility(
        file: &mut File,
        page_id: PageId,
    ) -> eyre::Result<IndexPageUtility<T>>
    where
        T: Archive + Default + SizeMeasurable,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0)?;
        let offset = GENERAL_HEADER_SIZE as i64;
        file.seek(SeekFrom::Current(offset))?;

        let mut size_bytes = vec![0u8; 2];
        file.read_exact(size_bytes.as_mut_slice())?;
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&size_bytes[0..2]) };
        let size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        let index_utility_len = Self::index_page_utility_length(size as usize);
        let mut index_utility_bytes = vec![0u8; index_utility_len];
        file.read_exact(index_utility_bytes.as_mut_slice())?;
        let utility = Self::get_index_page_utility_from_bytes(index_utility_bytes.as_ref());

        Ok(utility)
    }

    pub fn persist_index_page_utility(
        file: &mut File,
        page_id: PageId,
        utility: IndexPageUtility<T>,
    ) -> eyre::Result<()>
    where
        T: Archive
            + Default
            + SizeMeasurable
            + for<'a> Serialize<
                Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
            >,
    {
        seek_to_page_start(file, page_id.0)?;
        file.seek(SeekFrom::Current(
            GENERAL_HEADER_SIZE as i64 + u16::default().aligned_size() as i64,
        ))?;

        let node_id_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&utility.node_id)?;
        file.write(node_id_bytes.as_slice())?;
        let current_index_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&utility.current_index)?;
        file.write(current_index_bytes.as_slice())?;
        let current_length_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&utility.current_length)?;
        file.write(current_length_bytes.as_slice())?;
        let slots_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&utility.slots)?;
        file.write(slots_bytes.as_slice())?;
        Ok(())
    }

    fn read_value(file: &mut File) -> eyre::Result<IndexValue<T>>
    where
        T: Archive + Default + SizeMeasurable,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        let mut bytes = vec![0u8; align8(IndexValue::<T>::default().aligned_size())];
        file.read_exact(bytes.as_mut_slice())?;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(bytes.as_slice());
        let archived =
            unsafe { rkyv::access_unchecked::<<IndexValue<T> as Archive>::Archived>(&v[..]) };
        Ok(rkyv::deserialize(archived).expect("data should be valid"))
    }

    pub fn read_value_with_index(
        file: &mut File,
        page_id: PageId,
        size: usize,
        index: usize,
    ) -> eyre::Result<IndexValue<T>>
    where
        T: Archive + Default + SizeMeasurable,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0)?;

        let offset = Self::get_value_offset(size, index);
        file.seek(SeekFrom::Current(offset as i64))?;
        Self::read_value(file)
    }

    fn get_value_offset(size: usize, value_index: usize) -> usize
    where
        T: Default + SizeMeasurable,
    {
        let mut offset = GENERAL_HEADER_SIZE;
        offset += u16::default().aligned_size();
        offset += T::default().aligned_size();
        offset += u16::default().aligned_size();
        offset += u16::default().aligned_size();
        offset += align(size * u16::default().aligned_size()) + 8;
        offset += value_index * align8(IndexValue::<T>::default().aligned_size());

        offset
    }

    pub fn persist_value(
        file: &mut File,
        page_id: PageId,
        size: usize,
        value: IndexValue<T>,
        mut value_index: u16,
    ) -> eyre::Result<u16>
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
        seek_to_page_start(file, page_id.0)?;

        let offset = Self::get_value_offset(size, value_index as usize);
        file.seek(SeekFrom::Current(offset as i64))?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        file.write(bytes.as_slice())?;

        if value_index != size as u16 - 1 {
            let mut value = Self::read_value(file)?;
            while value != IndexValue::default() {
                value_index += 1;
                value = Self::read_value(file)?;
            }
        }

        Ok(value_index + 1)
    }

    pub fn remove_value(
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
        seek_to_page_start(file, page_id.0)?;

        let offset = Self::get_value_offset(size, value_index as usize);
        file.seek(SeekFrom::Current(offset as i64))?;
        let value = IndexValue::<T>::default();
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        file.write(bytes.as_slice())?;

        Ok(())
    }

    pub fn get_node(&self) -> Vec<Pair<T, Link>>
    where
        T: Clone + Ord,
    {
        let mut node = Vec::with_capacity(self.size as usize);
        for slot in &self.slots[..self.current_index as usize] {
            node.push(self.index_values[*slot as usize].clone().into())
        }
        node
    }

    pub fn from_node(node: &Vec<impl Into<IndexValue<T>> + Clone>, size: usize) -> Self
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

impl<T> Persistable for IndexPage<T>
where
    T: Archive
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Default
        + SizeMeasurable
        + Clone,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        let mut bytes = Vec::with_capacity(self.size as usize);
        let size_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.size).unwrap();
        bytes.extend_from_slice(size_bytes.as_ref());
        let node_id_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.node_id).unwrap();
        bytes.extend_from_slice(node_id_bytes.as_ref());
        let current_index_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&self.current_index).unwrap();
        bytes.extend_from_slice(current_index_bytes.as_ref());
        let current_length_bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&self.current_length).unwrap();
        bytes.extend_from_slice(current_length_bytes.as_ref());
        let slots_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.slots).unwrap();
        bytes.extend_from_slice(slots_bytes.as_ref());
        let values_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&self.index_values).unwrap();
        bytes.extend_from_slice(values_bytes.as_ref());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&bytes[0..2]) };
        let size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");

        let mut offset = 2;
        let utility_length = Self::index_page_utility_length(size as usize);
        let index_utility_bytes = &bytes[offset..offset + utility_length];
        let utility = Self::get_index_page_utility_from_bytes(index_utility_bytes);
        offset += utility_length;

        let values_len = size as usize * align8(IndexValue::<T>::default().aligned_size()) + 8;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + values_len]);
        let archived =
            unsafe { rkyv::access_unchecked::<<Vec<IndexValue<T>> as Archive>::Archived>(&v[..]) };
        let index_values = rkyv::deserialize::<Vec<IndexValue<T>>, rkyv::rancor::Error>(archived)
            .expect("data should be valid");

        Self {
            slots: utility.slots,
            size,
            current_index: utility.current_index,
            current_length: utility.current_length,
            node_id: utility.node_id,
            index_values,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::page::IndexValue;
    use crate::{align8, IndexPage, Link, Persistable, SizeMeasurable, INNER_PAGE_SIZE};

    pub fn get_size_from_data_length<T>(length: usize) -> usize
    where
        T: Default + SizeMeasurable,
    {
        let node_id_size = T::default().aligned_size();
        let slot_size = u16::default().aligned_size();
        let index_value_size = align8(T::default().aligned_size() + Link::default().aligned_size());
        let vec_util_size = 8;
        let size = (length - node_id_size - slot_size * 2 - vec_util_size * 2)
            / (slot_size + index_value_size);
        size
    }

    #[test]
    fn test_bytes() {
        let size: usize = get_size_from_data_length::<u64>(INNER_PAGE_SIZE);
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
