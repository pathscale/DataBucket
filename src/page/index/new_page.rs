//! [`crate::page::IndexPage`] definition.

use std::fmt::Debug;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use rkyv::{Archive, Deserialize, Serialize};
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::Serializer;
use rkyv::ser::sharing::Share;
use rkyv::util::AlignedVec;

use crate::page::{IndexValue, PageId};
use crate::{align, seek_to_page_start, Persistable, SizeMeasurable, GENERAL_HEADER_SIZE};

/// Represents a page, which is filled with [`IndexValue`]'s of some index.
#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct NewIndexPage<T> {
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
        let index_values = vec![IndexValue::default(); size];
        Self {
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
        file.seek(SeekFrom::Current(GENERAL_HEADER_SIZE as i64 + align(T::default().aligned_size()) as i64))?;
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
    >,
{
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        rkyv::to_bytes::<rkyv::rancor::Error>(self).unwrap()
    }
}

