use std::io;
use std::io::prelude::*;

use eyre::eyre;
use rkyv::api::high::HighDeserializer;
use rkyv::Archive;

use super::{Interval, SpaceInfo};
use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::General;
use crate::persistence::data::rkyv_data::parse_archived_row;
use crate::persistence::data::DataTypeValue;
use crate::{DataPage, GeneralPage, IndexData, Link, Persistable, GENERAL_HEADER_SIZE, PAGE_SIZE};

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

    seek_to_page_start(file, page.header.page_id.0)?;

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
    let archived =
        unsafe { rkyv::access_unchecked::<<GeneralHeader as Archive>::Archived>(&buffer[..]) };
    let header =
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");

    Ok(header)
}

pub fn parse_page<Page, const PAGE_SIZE: u32>(
    file: &mut std::fs::File,
    index: u32,
) -> eyre::Result<GeneralPage<Page>>
where
    Page: rkyv::Archive,
    <Page as rkyv::Archive>::Archived:
        rkyv::Deserialize<Page, HighDeserializer<rkyv::rancor::Error>>,
{
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

    let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived = unsafe { rkyv::access_unchecked::<<Page as Archive>::Archived>(&buffer[..]) };
    let info = rkyv::deserialize(archived).expect("data should be valid");

    Ok(GeneralPage {
        header,
        inner: info,
    })
}

pub fn parse_data_record<const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index: u32,
    offset: u32,
    length: u32,
    schema: &Vec<(String, String)>,
) -> eyre::Result<Vec<DataTypeValue>> {
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;
    if header.page_type != PageType::Data {
        return Err(eyre::Report::msg(format!(
            "The type of the page with index {} is not `Data`",
            index
        )));
    }

    file.seek(io::SeekFrom::Current(offset as i64))?;
    let mut buffer = vec![0u8; length as usize];
    file.read_exact(&mut buffer)?;

    let parsed_record = parse_archived_row(&buffer, &schema);

    Ok(parsed_record)
}

pub fn parse_index_page<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index: u32,
) -> eyre::Result<Vec<IndexData<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

    let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived =
        unsafe { rkyv::access_unchecked::<<Vec<IndexData<T>> as Archive>::Archived>(&buffer[..]) };
    let index_records: Vec<IndexData<T>> =
        rkyv::deserialize(archived).expect("data should be valid");

    Ok(index_records)
}

pub fn parse_space_info<const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
) -> eyre::Result<SpaceInfo> {
    file.seek(io::SeekFrom::Start(0))?;
    let header = parse_general_header(file)?;

    let mut buffer = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived =
        unsafe { rkyv::access_unchecked::<<SpaceInfo as Archive>::Archived>(&buffer[..]) };
    let space_info: SpaceInfo =
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).expect("data should be valid");

    Ok(space_info)
}

pub fn read_secondary_index_pages<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    index_name: &str,
    intervals: Vec<Interval>,
) -> eyre::Result<Vec<IndexData<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
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
            let mut index_records = parse_index_page::<T, PAGE_SIZE>(file, index as u32)?;
            result.append(&mut index_records);
        }
    }

    Ok(result)
}

pub fn read_index_pages<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    intervals: &Vec<Interval>,
) -> eyre::Result<Vec<IndexData<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    let mut result: Vec<IndexData<T>> = vec![];
    for interval in intervals.iter() {
        for index in interval.0..interval.1 {
            let mut index_records = parse_index_page::<T, PAGE_SIZE>(file, index as u32)?;
            result.append(&mut index_records);
        }
    }
    Ok(result)
}

fn read_data_pages<const PAGE_SIZE: usize>(
    mut file: &mut std::fs::File,
) -> eyre::Result<Vec<Vec<DataTypeValue>>> {
    let space_info = parse_space_info::<PAGE_SIZE>(file)?;
    let primary_key_fields = space_info.primary_key_fields;
    if primary_key_fields.len() != 1 {
        panic!("Currently only single primary key is supported");
    }

    let primary_key_type = space_info
        .row_schema
        .iter()
        .filter(|(field_name, _field_type)| field_name == &primary_key_fields[0])
        .map(|(_field_name, field_type)| field_type)
        .take(1)
        .collect::<Vec<&String>>()[0]
        .as_str();
    let links = match primary_key_type {
        "i64" => read_index_pages::<i64, PAGE_SIZE>(&mut file, &space_info.primary_key_intervals)?
            .iter()
            .map(|index_page| &index_page.index_values)
            .flatten()
            .map(|index_value| index_value.link)
            .collect::<Vec<Link>>(),
        _ => panic!("Unsupported primary key data type `{}`", primary_key_type),
    };

    let mut result: Vec<Vec<_>> = vec![];
    for link in links {
        let row = parse_data_record::<PAGE_SIZE>(
            &mut file,
            link.page_id.0,
            link.offset,
            link.length,
            &space_info.row_schema,
        )?;
        result.push(row);
    }

    Ok(result)
}

#[cfg(test)]
mod test {
    use rkyv::{Archive, Deserialize, Serialize};
    use scc::ebr::Guard;
    use scc::TreeIndex;
    use std::collections::HashMap;
    use std::fs::remove_file;
    use std::path::Path;

    use crate::page::index::IndexValue;
    use crate::page::util::read_secondary_index_pages;
    use crate::page::INNER_PAGE_SIZE;
    use crate::{
        map_index_pages_to_general, map_unique_tree_index, GeneralHeader, GeneralPage, IndexData,
        Interval, Link, PageType, SpaceInfoData, DATA_VERSION, PAGE_SIZE,
    };

    use super::persist_page;

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

        let guard = Guard::new();
        let res = map_unique_tree_index::<_, { INNER_PAGE_SIZE }>(index.iter(&guard));
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
            primary_key_fields: vec![],
            primary_key_intervals: vec![],
            secondary_index_intervals: HashMap::from([(
                "string_index".to_owned(),
                intervals.clone(),
            )]),
            data_intervals: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
            secondary_index_types: vec![("string_index".to_string(), "String".to_string())],
        };
        let space_info_page = GeneralPage {
            header: space_info_header,
            inner: space_info,
        };

        space_info_page
    }

    fn create_index_pages(intervals: &Vec<Interval>) -> Vec<GeneralPage<Vec<IndexData<String>>>> {
        let mut index_pages = Vec::<GeneralPage<Vec<IndexData<String>>>>::new();

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
                    inner: vec![index_data],
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
        let mut file: std::fs::File = std::fs::File::create(filename).unwrap();

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
        let index_pages = read_secondary_index_pages::<String, PAGE_SIZE>(
            &mut file,
            "string_index",
            vec![Interval(1, 2), Interval(5, 6)],
        )
        .unwrap();
        assert_eq!(index_pages[0].index_values.len(), 1);
        assert_eq!(index_pages[0].index_values[0].key, "first_value");
        assert_eq!(index_pages[0].index_values[0].link.page_id, 2.into());
        assert_eq!(index_pages[0].index_values[0].link.offset, 0);
        assert_eq!(index_pages[0].index_values[0].link.length, 0);
    }

    #[derive(Archive, Debug, Deserialize, Serialize)]
    struct TableStruct {
        int1: i32,
        string1: String,
    }

    #[test]
    fn test_read_table_data() {
        let filename = "tests/data/table_with_rows.wt";
        if Path::new(filename).exists() {
            remove_file(filename).unwrap();
        }
        let mut file: std::fs::File = std::fs::File::create(filename).unwrap();

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
            id: 1.into(),
            page_count: 4,
            name: "test space".to_owned(),
            row_schema: vec![
                ("int1".to_string(), "i32".to_string()),
                ("string1".to_string(), "String".to_string()),
            ],
            primary_key_fields: vec!["int1".to_string()],
            primary_key_intervals: vec![Interval(1, 3)],
            secondary_index_types: vec![],
            secondary_index_intervals: Default::default(),
            data_intervals: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
        };
        let mut space_info_page = GeneralPage {
            header: space_info_header,
            inner: space_info
        };
        persist_page(&mut space_info_page, &mut file).unwrap();

        let index1_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 1.into(),
            previous_id: 0.into(),
            next_id: 2.into(),
            page_type: PageType::Index,
            data_length: 0,
        };

        let index2_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 2.into(),
            previous_id: 0.into(),
            next_id: 3.into(),
            page_type: PageType::Index,
            data_length: 0,
        };

        let data1_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 3.into(),
            previous_id: 2.into(),
            next_id: 4.into(),
            page_type: PageType::Data,
            data_length: 0,
        };

        let data1_row1 = TableStruct {
            int1: 1,
            string1: "first string".to_string(),
        };

        let data1_row2 = TableStruct {
            int1: 2,
            string1: "second string".to_string(),
        };

        let data1_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data1_row1).unwrap();
        let data2_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data1_row2).unwrap();

        let data2_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 4.into(),
            previous_id: 3.into(),
            next_id: 5.into(),
            page_type: PageType::Data,
            data_length: 0,
        };

        let index_data: IndexData<i32> = IndexData {
            index_values: vec![
                IndexValue {
                    key: 0,
                    link: Link {
                        page_id: 2.into(),
                        offset: 0,
                        length: archived_page.len(),
                    }
                }
            ]
        };
    }
}
