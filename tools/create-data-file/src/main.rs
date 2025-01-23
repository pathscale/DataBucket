use clap::Parser;
use data_bucket::{persist_page, GeneralHeader, GeneralPage, PageType, DATA_VERSION};
use data_bucket::{IndexData, IndexValue, Interval, Link, SpaceInfoData};
use rkyv::rancor::Error;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    fs::{remove_file, File},
    str,
};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String,
    #[arg(short, long, default_value_t = 5)]
    count: usize,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    _ = remove_file(args.filename.as_str());
    let mut output_file = File::create(args.filename.as_str())?;

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
        name: "generated space".to_owned(),
        row_schema: vec![
            ("val".to_string(), "i32".to_string()),
            ("attr".to_string(), "String".to_string()),
        ],
        primary_key_fields: vec!["val".to_string()],
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
    persist_page(&mut space_info_page, &mut output_file).unwrap();

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

    let page_size = 100;
    let total_pages = (args.count + page_size - 1) / page_size;

    for page_idx in 0..total_pages {
        let start = page_idx * page_size;
        let end = usize::min(start + page_size, args.count);

        let (mut data_page, offsets) = generate_data_page(start as i32, end - start, data_header);
        persist_page(&mut data_page, &mut output_file).unwrap();

        let index_data = create_index_data(&data_page, &offsets);

        let mut index_page = GeneralPage {
            header: index_header,
            inner: index_data,
        };
        persist_page(&mut index_page, &mut output_file).unwrap();
    }

    Ok(())
}

#[derive(Archive, Debug, Deserialize, Serialize)]
struct TableStruct {
    val: i32,
    attr: String,
}

pub fn generate_data_page(
    start_key: i32,
    count: usize,
    header: GeneralHeader,
) -> (GeneralPage<Vec<u8>>, Vec<(i32, u32, u32)>) {
    let mut buffer = Vec::new();
    let mut offsets = Vec::new();
    let mut current_offset = 0;

    for i in 0..count {
        let key = start_key + i as i32;
        let data = TableStruct {
            val: key,
            attr: format!("string {}", key),
        };
        let serialized_data = rkyv::to_bytes::<Error>(&data).unwrap();
        let length = serialized_data.len() as u32;

        buffer.extend_from_slice(&serialized_data);
        offsets.push((key, current_offset as u32, length));
        current_offset += length as usize;
    }

    (
        GeneralPage {
            header,
            inner: buffer,
        },
        offsets,
    )
}

fn create_index_data(page: &GeneralPage<Vec<u8>>, offsets: &[(i32, u32, u32)]) -> IndexData<i32> {
    let index_values = offsets
        .iter()
        .map(|(key, offset, length)| IndexValue::<i32> {
            key: *key,
            link: Link {
                page_id: page.header.page_id,
                offset: *offset,
                length: *length,
            },
        })
        .collect();

    IndexData { index_values }
}
