use clap::Parser;
use data_bucket::{
    persist_page, DataPage, GeneralHeader, GeneralPage, Link, PageType, SpaceInfoPage,
    INNER_PAGE_SIZE, PAGE_SIZE,
};
use nagoya::io::File as _;
use rkyv::{Archive, Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(about = "Create a v3 demonstration store with independently readable data pages")]
struct Args {
    #[arg(short, long)]
    filename: String,
    #[arg(short, long, default_value_t = 5)]
    count: usize,
}

#[derive(Archive, Debug, Deserialize, Serialize)]
struct TableStruct {
    val: i32,
    attr: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    eyre::ensure!(
        args.count <= i32::MAX as usize,
        "count exceeds the demonstration key range"
    );
    let file = std::fs::OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .open(&args.filename)?;
    nagoya::block_on(async move {
        let mut file = nagoya::io::HostFile::new(file);
        let mut info = GeneralPage {
            header: GeneralHeader::new(0.into(), PageType::SpaceInfo, 1.into()),
            inner: SpaceInfoPage {
                id: 1.into(),
                page_count: 0,
                name: "generated space".into(),
                version: 1,
                row_schema: vec![
                    ("val".into(), "i32".into()),
                    ("attr".into(), "String".into()),
                ],
                primary_key_fields: vec!["val".into()],
                secondary_index_types: vec![],
                pk_gen_state: (),
                empty_links_list: vec![],
            },
        };
        persist_page::<_, { PAGE_SIZE as u32 }>(&mut info, &mut file).await?;
        let mut id = 1u32;
        let mut page = GeneralPage {
            header: GeneralHeader::new(id.into(), PageType::Data, 1.into()),
            inner: DataPage::<INNER_PAGE_SIZE>::new(),
        };
        for key in 0..args.count {
            let row = TableStruct {
                val: key as i32,
                attr: format!("string {key}"),
            };
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&row)?;
            let needed = page.inner.length as usize
                + bytes.len()
                + (page.inner.rows.len() + 1) * data_bucket::ROW_SLOT_SIZE
                + data_bucket::DATA_TRAILER_SIZE;
            if needed > INNER_PAGE_SIZE {
                persist_page::<_, { PAGE_SIZE as u32 }>(&mut page, &mut file).await?;
                id += 1;
                page = GeneralPage {
                    header: GeneralHeader::new(id.into(), PageType::Data, 1.into()),
                    inner: DataPage::new(),
                };
            }
            page.inner.update_at(
                Link {
                    page_id: id.into(),
                    offset: page.inner.length,
                    length: bytes.len() as u32,
                },
                &bytes,
            )?;
        }
        persist_page::<_, { PAGE_SIZE as u32 }>(&mut page, &mut file).await?;
        info.inner.page_count = id;
        persist_page::<_, { PAGE_SIZE as u32 }>(&mut info, &mut file).await?;
        file.sync_all().await?;
        println!("wrote {} rows across {} data pages", args.count, id);
        Ok(())
    })
}
