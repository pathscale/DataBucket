use std::io;
use std::io::prelude::*;

use eyre::eyre;
use rkyv::{Archive, Deserialize};

use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::General;
use crate::{DataPage, GeneralPage, IndexData, Persistable, GENERAL_HEADER_SIZE, PAGE_SIZE};

use super::{Interval, SpaceInfo};

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

fn seek_to_page_start(file: &mut std::fs::File, index: u32) -> eyre::Result<()> {
    let current_position: u64 = file.stream_position()?;
    let start_pos = index as i64 * PAGE_SIZE as i64;
    file.seek(io::SeekFrom::Current(start_pos - current_position as i64))?;

    Ok(())
}

fn parse_general_header(file: &mut std::fs::File) -> eyre::Result<GeneralHeader> {
    let mut buffer = [0; GENERAL_HEADER_SIZE];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<GeneralHeader>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let header: GeneralHeader = archived.deserialize(&mut map)?;

    Ok(header)
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
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

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
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

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

pub fn parse_index_page<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index: u32,
) -> eyre::Result<GeneralPage<IndexData<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
{
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

    let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<IndexData<T>>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let index: IndexData<T> = archived.deserialize(&mut map)?;

    Ok(GeneralPage {
        header,
        inner: index,
    })
}

pub fn parse_space_info<const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
) -> eyre::Result<SpaceInfo> {
    file.seek(io::SeekFrom::Start(0))?;
    let header = parse_general_header(file)?;

    let mut buffer = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::archived_root::<SpaceInfo>(&buffer[..]) };
    let mut map = rkyv::de::deserializers::SharedDeserializeMap::new();
    let space_info: SpaceInfo = archived.deserialize(&mut map)?;

    Ok(space_info)
}

pub fn read_index_pages<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index_name: &str,
    intervals: Vec<Interval>,
) -> eyre::Result<Vec<IndexData<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived:
        rkyv::Deserialize<T, rkyv::de::deserializers::SharedDeserializeMap>,
{
    let space_info = parse_space_info::<PAGE_SIZE>(file)?;

    let space_info_intervals = space_info
        .secondary_index_intervals
        .get(index_name)
        .ok_or_else(|| eyre!("No index with name \"{}\" found", index_name))?;

    // check that all of the provided intervals are valid
    for interval in intervals.iter() {
        let mut contained = false;
        for space_info_interval in space_info_intervals.iter() {
            if space_info_interval.contains(interval) {
                contained = true;
                break;
            }
        }
        if !contained {
            return Err(eyre!("The index interval {:?} is not valid", interval));
        }
    }

    let mut result: Vec<IndexData<T>> = vec![];
    for interval in intervals.iter() {
        for index in interval.0..interval.1 {
            let index_page = parse_index_page::<T, PAGE_SIZE>(file, index as u32)?;
            result.push(index_page.inner);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::fs::remove_file;
    use std::path::Path;
    use scc::TreeIndex;

    use crate::page::index::IndexValue;
    use crate::page::INNER_PAGE_SIZE;
    use crate::{
        map_index_pages_to_general, map_unique_tree_index, DataType, GeneralHeader, GeneralPage,
        IndexData, Interval, Link, PageType, SpaceInfoData, DATA_VERSION, PAGE_SIZE,
    };

    use super::{persist_page, read_index_pages};

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

    fn create_space_with_intervals(intervals: &Vec<Interval>) -> GeneralPage<SpaceInfoData> {
        let space_info_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 0.into(),
            previous_id: 0.into(),
            next_id: 1.into(),
            page_type: PageType::SpaceInfo,
            data_length: 0u32,
        };
        let space_info = SpaceInfoData {
            id: 0.into(),
            page_count: 0,
            name: "Test".to_string(),
            row_schema: vec![],
            primary_key_intervals: vec![],
            secondary_index_intervals: HashMap::from([(
                "string_index".to_owned(),
                intervals.clone(),
            )]),
            data_intervals: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
            secondary_index_map: HashMap::from([("string_index".to_owned(), DataType::String)]),
        };
        let space_info_page = GeneralPage {
            header: space_info_header,
            inner: space_info,
        };

        space_info_page
    }

    fn create_index_pages(intervals: &Vec<Interval>) -> Vec<GeneralPage<IndexData<String>>> {
        let mut index_pages = Vec::<GeneralPage<IndexData<String>>>::new();

        for interval in intervals {
            for index in interval.0..interval.1 {
                let index_header = GeneralHeader {
                    data_version: DATA_VERSION,
                    space_id: 1.into(),
                    page_id: (index as u32).into(),
                    previous_id: (if index > 0 { index as u32 - 1 } else { 0 }).into(),
                    next_id: (index as u32 + 1).into(),
                    page_type: PageType::SpaceInfo,
                    data_length: 0u32,
                };
                let index_data = IndexData {
                    index_values: vec![IndexValue {
                        key: "first_value".to_string(),
                        link: Link {
                            page_id: 2.into(),
                            length: 0,
                            offset: 0,
                        },
                    }],
                };
                let index_page = GeneralPage {
                    header: index_header,
                    inner: index_data,
                };
                index_pages.push(index_page);
            }
        }

        index_pages
    }

    #[test]
    fn test_read_index_pages() {
        let filename = "tests/data/table.wt";
        if Path::new(filename).exists() {
            remove_file(filename).unwrap();
        }
        let mut file = std::fs::File::create(filename).unwrap();

        let intervals = vec![Interval(1, 3), Interval(5, 8)];

        // create the space page
        let mut space_info_page = create_space_with_intervals(&intervals);
        persist_page(&mut space_info_page, &mut file).unwrap();

        // create the index pages
        for mut index_page in create_index_pages(&intervals) {
            persist_page(&mut index_page, &mut file).unwrap();
        }

        // read the data
        let mut file = std::fs::File::open(filename).unwrap();
        let index_pages =
            read_index_pages::<String, PAGE_SIZE>(&mut file, "string_index", vec![Interval(1, 2)])
                .unwrap();
        assert_eq!(index_pages[0].index_values.len(), 1);
        assert_eq!(index_pages[0].index_values[0].key, "first_value");
        assert_eq!(index_pages[0].index_values[0].link.page_id, 2.into());
        assert_eq!(index_pages[0].index_values[0].link.offset, 0);
        assert_eq!(index_pages[0].index_values[0].link.length, 0);
    }
}
