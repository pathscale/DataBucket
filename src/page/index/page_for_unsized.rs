use data_bucket_codegen::Persistable;
use indexset::core::pair::Pair;
use rkyv::de::Pool;
use rkyv::rancor::Strategy;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::ser::sharing::Share;
use rkyv::ser::Serializer;
use rkyv::util::AlignedVec;
use rkyv::{to_bytes, Archive, Deserialize, Serialize};
use std::fmt::Debug;
use std::io::SeekFrom;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

use crate::page::index::IndexPageUtility;
use crate::page::PageId;
use crate::{align8, VariableSizeMeasurable};
use crate::{seek_to_page_start, IndexValue, SizeMeasurable, GENERAL_HEADER_SIZE};
use crate::{Link, Persistable};

#[derive(Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct UnsizedIndexPage<
    T: Default + SizeMeasurable + VariableSizeMeasurable,
    const DATA_LENGTH: u32,
> {
    pub slots_size: u16,
    pub node_id_size: u16,
    pub node_id: IndexValue<T>,
    pub last_value_offset: u32,
    pub slots: Vec<(u32, u16)>,
    pub index_values: Vec<IndexValue<T>>,
}

#[derive(
    Archive, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Persistable,
)]
#[persistable(by_parts, unsized_gens)]
pub struct UnsizedIndexPageUtility<T: Default + SizeMeasurable + VariableSizeMeasurable> {
    pub slots_size: u16,
    pub node_id_size: u16,
    pub node_id: IndexValue<T>,
    pub last_value_offset: u32,
    pub slots: Vec<(u32, u16)>,
}

impl<T: Default + SizeMeasurable + VariableSizeMeasurable> UnsizedIndexPageUtility<T> {
    pub fn update_node_id(&mut self, node_id: IndexValue<T>) -> eyre::Result<()> {
        self.node_id_size = node_id.aligned_size() as u16;
        self.node_id = node_id;

        Ok(())
    }
}

impl<T: Default + SizeMeasurable + VariableSizeMeasurable, const DATA_LENGTH: u32>
    IndexPageUtility<T> for UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + Debug
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        > + Send
        + Sync,
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
        + Clone
        + SizeMeasurable
        + VariableSizeMeasurable
        + for<'a> Serialize<
            Strategy<Serializer<AlignedVec, ArenaHandle<'a>, Share>, rkyv::rancor::Error>,
        >,
    <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
{
    pub fn new(node_id: IndexValue<T>) -> eyre::Result<Self> {
        let len = node_id.aligned_size() as u32;
        println!("{:?}", to_bytes(&node_id).unwrap().len());
        println!("{:?}", len);
        Ok(Self {
            slots_size: 1,
            node_id_size: len as u16,
            node_id: node_id.clone(),
            last_value_offset: len,
            slots: vec![(len, len as u16)],
            index_values: vec![node_id],
        })
    }

    fn new_with_values(values: Vec<IndexValue<T>>) -> Self
    where
        T: Clone,
    {
        let slots_size = values.len() as u16;
        let node_id = values.last().expect("Node should be not empty").clone();
        let node_id_size = node_id.aligned_size() as u16;
        let mut last_value_offset = 0;
        let mut slots = vec![];
        for val in &values {
            let len = val.aligned_size() as u32;
            last_value_offset += len;
            slots.push((last_value_offset, len as u16));
        }
        Self {
            slots_size,
            node_id_size,
            node_id,
            last_value_offset,
            slots,
            index_values: values,
        }
    }

    fn rebuild(&mut self)
    where
        T: Clone,
    {
        self.node_id = self.index_values.last().unwrap().clone();
        self.node_id_size = self.node_id.aligned_size() as u16;
        self.last_value_offset = 0;
        let mut slots = vec![];
        for val in &self.index_values {
            let len = val.aligned_size() as u32;
            self.last_value_offset += len;
            slots.push((self.last_value_offset, len as u16));
        }
        self.slots = slots;
        self.slots_size = self.slots.len() as u16
    }

    pub fn split(&mut self, index: usize) -> UnsizedIndexPage<T, DATA_LENGTH>
    where
        T: Clone,
    {
        let index_values = self.index_values.split_off(index);
        let new_page = UnsizedIndexPage::new_with_values(index_values);
        self.rebuild();

        new_page
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
        file.seek(SeekFrom::Current(-(offset as i64))).await?;
        file.write_all(bytes.as_slice()).await?;

        Ok(offset)
    }

    async fn read_value(file: &mut File, len: u16) -> eyre::Result<IndexValue<T>>
    where
        T: Archive,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        let mut bytes = vec![0u8; len as usize];
        file.read_exact(bytes.as_mut_slice()).await?;
        let mut v = AlignedVec::<4>::new();
        v.extend_from_slice(bytes.as_slice());
        let archived =
            unsafe { rkyv::access_unchecked::<<IndexValue<T> as Archive>::Archived>(&v[..]) };
        Ok(rkyv::deserialize(archived).expect("data should be valid"))
    }

    pub async fn read_value_with_offset(
        file: &mut File,
        page_id: PageId,
        offset: u32,
        len: u16,
    ) -> eyre::Result<IndexValue<T>>
    where
        T: Archive,
        <T as Archive>::Archived: Deserialize<T, Strategy<Pool, rkyv::rancor::Error>>,
    {
        seek_to_page_start(file, page_id.0 + 1).await?;
        file.seek(SeekFrom::Current(-(offset as i64))).await?;
        Self::read_value(file, len).await
    }

    pub fn get_node(&self) -> Vec<Pair<T, Link>>
    where
        T: Clone + Ord,
    {
        self.index_values
            .clone()
            .into_iter()
            .map(|v| v.into())
            .collect()
    }

    pub fn from_node(node: &[impl Into<IndexValue<T>> + Clone]) -> Self
    where
        T: Clone + Ord + Default,
    {
        let values = node.iter().map(|v| v.clone().into()).collect::<Vec<_>>();
        Self::new_with_values(values)
    }
}

impl<T, const DATA_LENGTH: u32> Persistable for UnsizedIndexPage<T, DATA_LENGTH>
where
    T: Archive
        + Clone
        + Default
        + Debug
        + SizeMeasurable
        + VariableSizeMeasurable
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
        println!("{:?}", utility_bytes);
        println!(
            "{:?}",
            UnsizedIndexPageUtility::<T>::persisted_size(
                self.slots_size as usize,
                self.node_id_size as usize
            )
        );
        println!("{:?}", utility_len);
        let mut bytes = vec![0u8; data_length];
        bytes.splice(0..utility_len, utility_bytes.iter().copied());

        for ((offset, len), value) in self.slots.iter().zip(self.index_values.iter()) {
            let offset = data_length - *offset as usize;
            let len = *len as usize;
            let value_bytes = rkyv::to_bytes::<rkyv::rancor::Error>(value).unwrap();
            bytes.splice(offset..(offset + len), value_bytes.iter().copied());
        }

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let slots_size_bytes = &bytes[0..UnsizedIndexPageUtility::<T>::slots_size_size()];
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(slots_size_bytes) };
        let slots_size =
            rkyv::deserialize::<u16, rkyv::rancor::Error>(archived).expect("data should be valid");
        let node_id_size_bytes = &bytes[UnsizedIndexPageUtility::<T>::slots_size_size()
            ..UnsizedIndexPageUtility::<T>::node_id_size_size()
                + UnsizedIndexPageUtility::<T>::node_id_size_size()];
        let archived =
            unsafe { rkyv::access_unchecked::<<u16 as Archive>::Archived>(node_id_size_bytes) };
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
            key: "Someone from somewhere".to_string(),
            link: Link {
                page_id: 0.into(),
                offset: 0,
                length: 40,
            },
        };
        let page = UnsizedIndexPage::<_, 1024>::new(IndexValue {
            key: "Someone from somewhere".to_string(),
            link: Default::default(),
        })
        .unwrap();
        let bytes = page.as_bytes();
        assert_eq!(bytes.as_ref().len(), 1024);
        let page_back = UnsizedIndexPage::from_bytes(bytes.as_ref());
        assert_eq!(page_back, page)
    }

    #[test]
    fn split() {
        let mut values = vec![];
        for i in 0..10 {
            values.push(IndexValue {
                key: format!("{}___________________{}", i, i),
                link: Link {
                    page_id: 0.into(),
                    offset: i * 24,
                    length: 24,
                },
            })
        }
        let mut page = UnsizedIndexPage::<String, 1024>::new_with_values(values);
        let split = page.split(5);

        assert_eq!(page.slots_size, 5);
        let offset = page.slots.iter().map(|(_, l)| *l).sum::<u16>();
        assert_eq!(page.last_value_offset, offset as u32);
        assert_eq!(page.last_value_offset, page.slots.last().unwrap().0);

        assert_ne!(page.node_id, split.node_id);

        assert_eq!(split.slots_size, 5);
        let offset = split.slots.iter().map(|(_, l)| *l).sum::<u16>();
        assert_eq!(split.last_value_offset, offset as u32);
        assert_eq!(split.last_value_offset, page.slots.last().unwrap().0)
    }
}
