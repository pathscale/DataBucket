//! [`crate::page::IndexPage`] definition.

use std::array;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::io::{Read, Seek, SeekFrom, Write};

use rkyv::{Archive, Deserialize, Serialize};
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::Serializer;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;

use crate::page::{IndexValue, PageId};
use crate::{align, seek_to_page_start, Link, Persistable, SizeMeasurable, GENERAL_HEADER_SIZE};

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NewIndexPage<T> {
    pub size: u16,
    pub node_id: T,
    pub values_count: u16,
    pub slots: Vec<u16>,
    pub index_values: Vec<IndexValue<T>>,
}

impl<T> NewIndexPage<T> {
    pub fn new(node_id: T, size: usize) -> Self
    where T: Default + Clone,
    {
        let slots =  vec![0u16; size];
        let mut v = IndexValue::default();
        v.link.page_id = 1.into();
        v.link.length = 4;
        v.link.offset = 3;
        let index_values = vec![v; size];
        Self {
            size: size as u16,
            node_id,
            values_count: 0,
            slots,
            index_values,
        }
    }

    pub fn parse_slots_and_values_count(file: &mut File, page_id: PageId, size: usize) -> eyre::Result<(Vec<u16>, u16)>
    where T: Default + SizeMeasurable
    {
        seek_to_page_start(file, page_id.0)?;
        let offset = GENERAL_HEADER_SIZE as i64 + align(T::default().aligned_size()) as i64;
        file.seek(SeekFrom::Current(offset))?;
        let mut values_count_bytes = vec![0u8; align(u16::default().aligned_size())];
        file.read_exact(values_count_bytes.as_mut_slice())?;
        let archived = unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(values_count_bytes.as_slice()) };
        let values_count = rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");

        let mut slots_bytes = vec![0u8; align(size * u16::default().aligned_size() + 8)];
        file.read_exact(slots_bytes.as_mut_slice())?;
        let archived = unsafe { rkyv::access_unchecked::<<Vec<u16> as Archive>::Archived>(slots_bytes.as_slice()) };
        let slots = rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");

        Ok((slots, values_count))
    }

    pub fn persist_slots(file: &mut File, page_id: PageId, slots: Vec<u16>, values_count: u16) -> eyre::Result<()>
    where T: Default + SizeMeasurable
    {
        seek_to_page_start(file, page_id.0)?;
        file.seek(SeekFrom::Current(GENERAL_HEADER_SIZE as i64 + align(T::default().aligned_size()) as i64))?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&values_count)?;
        file.write(bytes.as_slice())?;

        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&slots)?;
        file.write(bytes.as_slice())?;
        Ok(())
    }

    pub fn persist_value(file: &mut File, page_id: PageId, size: usize, value: IndexValue<T>, value_index: u16) -> eyre::Result<()>
    where
        T: Archive
        + Default
        + SizeMeasurable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    {
        seek_to_page_start(file, page_id.0)?;

        let mut offset = GENERAL_HEADER_SIZE;
        offset += align(T::default().aligned_size());
        offset += align(u16::default().aligned_size() * size + 8);
        offset += value_index as usize * align(T::default().aligned_size() + PageId::default().0.aligned_size());

        file.seek(SeekFrom::Current(offset as i64))?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&value)?;
        file.write(bytes.as_slice())?;
        Ok(())
    }
}

impl<T> Persistable for NewIndexPage<T>
where
    T: Archive
    + for<'a> Serialize<
        Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
    > + Default + SizeMeasurable + Debug + Clone,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        let mut bytes = Vec::with_capacity(self.size as usize);
        let size_bytes =  rkyv::to_bytes::<rkyv::rancor::Error>(&self.size).unwrap();
        bytes.extend_from_slice(size_bytes.as_ref());
        let node_id_bytes =  rkyv::to_bytes::<rkyv::rancor::Error>(&self.node_id).unwrap();
        bytes.extend_from_slice(node_id_bytes.as_ref());
        let values_count_bytes =  rkyv::to_bytes::<rkyv::rancor::Error>(&self.values_count).unwrap();
        bytes.extend_from_slice(values_count_bytes.as_ref());
        let slots_bytes =  rkyv::to_bytes::<rkyv::rancor::Error>(&self.slots).unwrap();
        bytes.extend_from_slice(slots_bytes.as_ref());
        let values_bytes =  rkyv::to_bytes::<rkyv::rancor::Error>(&self.index_values).unwrap();
        bytes.extend_from_slice(values_bytes.as_ref());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let archived = unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&bytes[0..2]) };
        let size = rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        println!("{}", size);
        let t_size = T::default().aligned_size();
        let mut offset = 2;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + t_size]);
        let archived = unsafe { rkyv::access_unchecked::<<T as Archive>::Archived>(&v[..]) };
        let node_id = rkyv::deserialize(archived).expect("data should be valid");
        println!("{:?}", node_id);
        offset = 2 + t_size;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(&bytes[offset..offset + 2]);
        let archived = unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(&v[..]) };
        let values_count = rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        println!("{:?}", values_count);

        Self::new(node_id, size as usize)
    }
}

#[cfg(test)]
mod tests {
    use crate::{align8, Link, NewIndexPage, Persistable, SizeMeasurable, INNER_PAGE_SIZE};

    pub fn get_size_from_data_length<T>(length: usize) -> usize
    where
        T: Default + SizeMeasurable,
    {
        let node_id_size = T::default().aligned_size();
        let slot_size = u16::default().aligned_size();
        let index_value_size = align8(T::default().aligned_size() + Link::default().aligned_size());
        let vec_util_size = 8;
        let size = (length - node_id_size - slot_size  * 2 - vec_util_size * 2) / (slot_size + index_value_size);
        size
    }

    #[test]
    fn test_bytes() {
        let size: usize = get_size_from_data_length::<u64>(INNER_PAGE_SIZE);
        let page = NewIndexPage::<u64>::new(1, size);
        let bytes = page.as_bytes();
        println!("{:?}", bytes.as_ref());
        println!("{}", size);
        println!("{}", bytes.as_ref().len());
        let page = NewIndexPage::<u64>::from_bytes(bytes.as_ref());

    }
}
