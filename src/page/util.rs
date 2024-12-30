use std::io;
use std::io::prelude::*;

use eyre::eyre;
use rkyv::api::high::HighDeserializer;
use rkyv::Archive;

use super::index::IndexValue;
use super::{Interval, SpaceInfo};
use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::General;
use crate::persistence::data::rkyv_data::parse_archived_row;
use crate::persistence::data::DataTypeValue;
use crate::{DataPage, GeneralPage, IndexData, Link, Persistable, GENERAL_HEADER_SIZE, PAGE_SIZE};

pub fn map_index_pages_to_general<T>(pages: Vec<IndexData<T>>) -> Vec<General<IndexData<T>>> {
    let mut header = &mut GeneralHeader::new(0.into(), PageType::Index, 0.into());
    let mut general_pages = vec![];

    let mut pages = pages.into_iter();
    if let Some(p) = pages.next() {
        let general = General {
            header: header.clone(),
            inner: p,
        };
        general_pages.push(general);
    }
    let mut previous_header = header;

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
) -> Vec<General<DataPage<DATA_LENGTH>>> {
    let mut header = &mut GeneralHeader::new(0.into(), PageType::Data, 0.into());
    let mut general_pages = vec![];

    let mut pages = pages.into_iter();
    if let Some(p) = pages.next() {
        let general = General {
            header: header.clone(),
            inner: p,
        };
        general_pages.push(general);
    }
    let mut previous_header = header;

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

pub fn seek_to_page_start(file: &mut std::fs::File, index: u32) -> eyre::Result<()> {
    file.seek(io::SeekFrom::Start(index as u64 * PAGE_SIZE as u64))?;

    Ok(())
}

pub fn seek_by_link(file: &mut std::fs::File, link: Link) -> eyre::Result<()> {
    file.seek(io::SeekFrom::Start(
        link.page_id.0 as u64 * PAGE_SIZE as u64 + GENERAL_HEADER_SIZE as u64 + link.offset as u64,
    ))?;

    Ok(())
}

pub fn update_at<const DATA_LENGTH: usize>(
    file: &mut std::fs::File,
    link: Link,
    new_data: &[u8],
) -> eyre::Result<()> {
    if new_data.len() as u32 != link.length {
        return Err(eyre!(
            "New data length {} does not match link length {}",
            new_data.len(),
            link.length
        ));
    }

    if (link.offset + link.length) as usize > DATA_LENGTH {
        return Err(eyre!(
            "Link range (offset: {}, length: {}) exceeds data bounds ({})",
            link.offset,
            link.length,
            DATA_LENGTH
        ));
    }

    seek_by_link(file, link)?;
    file.write(new_data)?;
    Ok(())
}

pub fn parse_general_header(file: &mut std::fs::File) -> eyre::Result<GeneralHeader> {
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
) -> eyre::Result<Vec<IndexValue<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    seek_to_page_start(file, index)?;
    let header = parse_general_header(file)?;

    let mut buffer: Vec<u8> = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer)?;
    let archived =
        unsafe { rkyv::access_unchecked::<<IndexData<T> as Archive>::Archived>(&buffer[..]) };
    let index_records: Vec<IndexValue<T>> = rkyv::deserialize::<IndexData<T>, _>(archived)
        .expect("data should be valid")
        .index_values;

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
) -> eyre::Result<Vec<IndexValue<T>>>
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

    let mut result: Vec<IndexValue<T>> = vec![];
    for interval in intervals.iter() {
        for index in interval.0..=interval.1 {
            let mut index_records = parse_index_page::<T, PAGE_SIZE>(file, index as u32)?;
            result.append(&mut index_records);
        }
    }

    Ok(result)
}

pub fn read_index_pages<T, const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
    intervals: &Vec<Interval>,
) -> eyre::Result<Vec<IndexValue<T>>>
where
    T: Archive,
    <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
{
    let mut result: Vec<IndexValue<T>> = vec![];
    for interval in intervals.iter() {
        for index in interval.0..=interval.1 {
            let mut index_records = parse_index_page::<T, PAGE_SIZE>(file, index as u32)?;
            result.append(&mut index_records);
        }
    }
    Ok(result)
}

fn read_links<DataType, const PAGE_SIZE: usize>(
    mut file: &mut std::fs::File,
    space_info: &SpaceInfo,
) -> eyre::Result<Vec<Link>> {
    Ok(
        read_index_pages::<i32, PAGE_SIZE>(&mut file, &space_info.primary_key_intervals)?
            .iter()
            .map(|index_value| index_value.link)
            .collect::<Vec<Link>>(),
    )
}

pub fn read_rows_schema<const PAGE_SIZE: usize>(
    file: &mut std::fs::File,
) -> eyre::Result<Vec<(String, String)>> {
    let space_info = parse_space_info::<PAGE_SIZE>(file)?;
    Ok(space_info.row_schema)
}

pub fn read_data_pages<const PAGE_SIZE: usize>(
    mut file: &mut std::fs::File,
) -> eyre::Result<Vec<Vec<DataTypeValue>>> {
    let space_info = parse_space_info::<PAGE_SIZE>(file)?;
    let primary_key_fields = &space_info.primary_key_fields;
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
        "String" => read_links::<String, PAGE_SIZE>(&mut file, &space_info)?,
        "i128" => read_links::<i128, PAGE_SIZE>(&mut file, &space_info)?,
        "i64" => read_links::<i64, PAGE_SIZE>(&mut file, &space_info)?,
        "i32" => read_links::<i32, PAGE_SIZE>(&mut file, &space_info)?,
        "i16" => read_links::<i16, PAGE_SIZE>(&mut file, &space_info)?,
        "i8" => read_links::<i8, PAGE_SIZE>(&mut file, &space_info)?,
        "u128" => read_links::<u128, PAGE_SIZE>(&mut file, &space_info)?,
        "u64" => read_links::<u64, PAGE_SIZE>(&mut file, &space_info)?,
        "u32" => read_links::<u32, PAGE_SIZE>(&mut file, &space_info)?,
        "u16" => read_links::<u16, PAGE_SIZE>(&mut file, &space_info)?,
        "u8" => read_links::<u8, PAGE_SIZE>(&mut file, &space_info)?,
        "f64" => read_links::<f64, PAGE_SIZE>(&mut file, &space_info)?,
        "f32" => read_links::<f32, PAGE_SIZE>(&mut file, &space_info)?,
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
pub mod test {
    use rkyv::{Archive, Deserialize, Serialize};
    use scc::ebr::Guard;
    use scc::TreeIndex;
    use std::collections::HashMap;
    use std::fs::remove_file;
    use std::path::Path;

    use crate::page::index::IndexValue;
    use crate::page::util::read_secondary_index_pages;
    use crate::page::INNER_PAGE_SIZE;
    use crate::persistence::data::DataTypeValue;
    use crate::{
        map_index_pages_to_general, map_unique_tree_index, read_data_pages,
        GeneralHeader, GeneralPage, IndexData, Interval, Link, PageType, SpaceInfoData,
        DATA_VERSION, PAGE_SIZE,
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
            data_length: 0 as u32,
        };
        let generalised = map_index_pages_to_general(res);
        assert_eq!(generalised.len(), 3);
        let first = generalised.get(0).unwrap().header;
        let second = generalised.get(1).unwrap().header;
        let third = generalised.get(2).unwrap().header;

        assert_eq!(first.page_id, header.page_id);
        assert_eq!(first.space_id, header.space_id);
        assert_eq!(first.previous_id, header.previous_id);
        assert_eq!(first.next_id, header.next_id);
        assert_eq!(first.page_type, PageType::Index);

        assert_eq!(first.next_id, header.page_id);
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

    fn create_index_pages(intervals: &Vec<Interval>) -> Vec<GeneralPage<IndexData<String>>> {
        let mut index_pages = Vec::<GeneralPage<IndexData<String>>>::new();

        for interval in intervals {
            for index in interval.0..=interval.1 {
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
        let mut file: std::fs::File = std::fs::File::create(filename).unwrap();

        let intervals = vec![Interval(1, 2), Interval(5, 7)];

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
        assert_eq!(index_pages.len(), 4);
        assert_eq!(index_pages[0].key, "first_value");
        assert_eq!(index_pages[0].link.page_id, 2.into());
        assert_eq!(index_pages[0].link.offset, 0);
        assert_eq!(index_pages[0].link.length, 0);
    }

    #[derive(Archive, Debug, Deserialize, Serialize)]
    struct TableStruct {
        int1: i32,
        string1: String,
    }

    pub fn create_test_database_file(filename: &str) {
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
            primary_key_intervals: vec![Interval(1, 1)],
            secondary_index_types: vec![],
            secondary_index_intervals: Default::default(),
            data_intervals: vec![],
            pk_gen_state: (),
            empty_links_list: vec![],
        };
        let mut space_info_page = GeneralPage {
            header: space_info_header,
            inner: space_info,
        };
        persist_page(&mut space_info_page, &mut file).unwrap();

        let index_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 1.into(),
            previous_id: 0.into(),
            next_id: 2.into(),
            page_type: PageType::Index,
            data_length: 0,
        };

        let data_header = GeneralHeader {
            data_version: DATA_VERSION,
            space_id: 1.into(),
            page_id: 2.into(),
            previous_id: 2.into(),
            next_id: 4.into(),
            page_type: PageType::Data,
            data_length: 0,
        };

        let data_row1 = TableStruct {
            int1: 1,
            string1: "first string".to_string(),
        };

        let data_row2 = TableStruct {
            int1: 2,
            string1: "second string".to_string(),
        };

        let data_row1_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data_row1).unwrap();
        let data_row1_offset = 0;
        let data_row1_length = data_row1_inner.len();

        let data_row2_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data_row2).unwrap();
        let data_row2_offset = data_row1_offset + data_row1_length;
        let data_row2_length = data_row2_inner.len();

        let data_rows12_buffer = [data_row1_inner, data_row2_inner].concat();

        let mut data_page = GeneralPage::<Vec<u8>> {
            header: data_header,
            inner: data_rows12_buffer,
        };

        let index_data: IndexData<i32> = IndexData::<i32> {
            index_values: vec![
                IndexValue::<i32> {
                    key: 1,
                    link: Link {
                        page_id: data_header.page_id,
                        offset: data_row1_offset as u32,
                        length: data_row1_length as u32,
                    },
                },
                IndexValue::<i32> {
                    key: 2,
                    link: Link {
                        page_id: data_header.page_id,
                        offset: data_row2_offset as u32,
                        length: data_row2_length as u32,
                    },
                },
            ],
        };
        let mut index_page = GeneralPage {
            header: index_header,
            inner: index_data,
        };

        persist_page(&mut index_page, &mut file).unwrap();
        persist_page(&mut data_page, &mut file).unwrap();
    }

    #[test]
    fn test_read_table_data() {
        let filename = "tests/data/table_with_rows.wt";
        create_test_database_file(filename);

        let mut file: std::fs::File = std::fs::File::open(filename).unwrap();
        let data_pages: Vec<Vec<DataTypeValue>> = read_data_pages::<PAGE_SIZE>(&mut file).unwrap();
        assert_eq!(data_pages[0][0], DataTypeValue::I32(1));
        assert_eq!(
            data_pages[0][1],
            DataTypeValue::String("first string".to_string())
        );
        assert_eq!(data_pages[1][0], DataTypeValue::I32(2));
        assert_eq!(
            data_pages[1][1],
            DataTypeValue::String("second string".to_string())
        );
    }
}
