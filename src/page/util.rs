use rkyv::Deserialize;
use std::io;
use std::io::prelude::*;

use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::General;
use crate::{DataPage, GeneralPage, IndexData, Persistable, GENERAL_HEADER_SIZE, PAGE_SIZE};

pub fn map_index_pages_to_general<T>(
    pages: Vec<IndexData<T>>,
    header: &mut GeneralHeader,
) -> Vec<General<IndexData<T>>> {
    let mut previous_header = header;
    let mut general_pages = vec![];

    for p in pages {
        let general = General {
            header: previous_header.follow_with(PageType::Index),
            inner: p,
        };

        general_pages.push(general);
        previous_header = &mut general_pages.last_mut().unwrap().header;
    }

    general_pages
}

pub fn map_data_pages_to_general<const DATA_LENGTH: usize>(
    pages: Vec<DataPage<DATA_LENGTH>>,
    header: &mut GeneralHeader,
) -> Vec<General<DataPage<DATA_LENGTH>>> {
    let mut previous_header = header;
    let mut general_pages = vec![];

    for p in pages {
        let general = General {
            header: previous_header.follow_with(PageType::Data),
            inner: p,
        };

        general_pages.push(general);
        previous_header = &mut general_pages.last_mut().unwrap().header;
    }

    general_pages
}

pub fn persist_page<T>(page: &mut GeneralPage<T>, file: &mut std::fs::File) -> eyre::Result<()>
where
    T: Persistable,
{
    use std::io::prelude::*;

    let page_count = page.header.page_id.0 as i64 + 1;
    let inner_bytes = page.inner.as_bytes();
    page.header.data_length = inner_bytes.as_ref().len() as u32;

    file.write_all(page.header.as_bytes().as_ref())?;
    file.write_all(inner_bytes.as_ref())?;
    let curr_position = file.stream_position()?;
    file.seek(io::SeekFrom::Current(
        (page_count * PAGE_SIZE as i64) - curr_position as i64,
    ))?;

    Ok(())
}

pub fn parse_page<Page, const PAGE_SIZE: u32>(
    file: &mut std::fs::File,
    index: u32,
) -> eyre::Result<GeneralPage<Page>>
where
    Page: rkyv::Archive,
    <Page as rkyv::Archive>::Archived:
        rkyv::Deserialize<Page, rkyv::de::deserializers::SharedDeserializeMap>,
{
    let current_position = file.stream_position()?;
    let start_pos = index as i64 * PAGE_SIZE as i64;
    file.seek(io::SeekFrom::Current(start_pos - current_position as i64))?;

    let mut buffer = [0; GENERAL_HEADER_SIZE];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<GeneralHeader>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let header: GeneralHeader = archived.deserialize(&mut map)?;

    let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<Page>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let info = archived.deserialize(&mut map)?;

    Ok(GeneralPage {
        header,
        inner: info,
    })
}

pub fn parse_data_page<const PAGE_SIZE: usize, const INNER_PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index: u32,
) -> eyre::Result<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    let current_position = file.stream_position()?;
    let start_pos = index as i64 * PAGE_SIZE as i64;
    file.seek(io::SeekFrom::Current(start_pos - current_position as i64))?;

    let mut buffer = [0; GENERAL_HEADER_SIZE];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<GeneralHeader>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let header: GeneralHeader = archived.deserialize(&mut map)?;

    let mut buffer = [0u8; INNER_PAGE_SIZE];
    if header.next_id == 0.into() {
        file.read(&mut buffer)?;
    } else {
        file.read_exact(&mut buffer)?;
    }

    let data = DataPage {
        data: buffer,
        length: header.data_length,
    };

    Ok(GeneralPage {
        header,
        inner: data,
    })
}

#[cfg(test)]
mod test {
    use scc::TreeIndex;

    use crate::page::INNER_PAGE_SIZE;
    use crate::{
        map_index_pages_to_general, map_unique_tree_index, GeneralHeader, Link, PageType, DATA_VERSION, PAGE_SIZE
    };

    #[test]
    fn test_map() {
        let index = TreeIndex::new();
        for i in 0..3060 {
            let l = Link {
                page_id: 1.into(),
                offset: 0,
                length: 32,
            };
            index.insert(i, l).expect("is ok");
        }

        let res = map_unique_tree_index::<_, { INNER_PAGE_SIZE }>(&index);
        let mut header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 0.into(),
            page_id: 0.into(),
            previous_id: 0.into(),
            next_id: 0.into(),
            page_type: PageType::SpaceInfo,
            data_length: PAGE_SIZE as u32,
        };
        let generalised = map_index_pages_to_general(res, &mut header);
        assert_eq!(generalised.len(), 3);
        let first = generalised.get(0).unwrap().header;
        let second = generalised.get(1).unwrap().header;
        let third = generalised.get(2).unwrap().header;

        assert_eq!(header.next_id, first.page_id);
        assert_eq!(first.space_id, header.space_id);
        assert_eq!(first.previous_id, header.page_id);
        assert_eq!(first.next_id, second.page_id);
        assert_eq!(first.page_type, PageType::Index);

        assert_eq!(first.next_id, second.page_id);
        assert_eq!(second.space_id, header.space_id);
        assert_eq!(second.previous_id, first.page_id);
        assert_eq!(second.next_id, third.page_id);
        assert_eq!(second.page_type, PageType::Index);

        assert_eq!(third.next_id, 0.into());
        assert_eq!(third.space_id, header.space_id);
        assert_eq!(third.previous_id, second.page_id);
        assert_eq!(third.page_type, PageType::Index);
    }
}
