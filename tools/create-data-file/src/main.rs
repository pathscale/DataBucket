use clap::Parser;
use std::{fs::{remove_file, File}, str};

use data_bucket::{persist_page, GeneralHeader, GeneralPage, PageType, Persistable};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();

    let mut header: GeneralHeader = GeneralHeader {
        space_id: 1.into(),
        page_id: 2.into(),
        previous_id: 0.into(),
        next_id: 0.into(),
        page_type: PageType::SpaceInfo,
        data_length: 0 as u32,
    };
    let mut inner: String = "hello".into();
    let mut page: GeneralPage<String> = GeneralPage { header, inner };

    _ = remove_file(args.filename.as_str());
    let mut output_file = File::create(args.filename.as_str())?;
    persist_page(&mut page, &mut output_file)?;

    Ok(())
}
