use clap::Parser;
use std::{fs::{remove_file, File}, str};

use data_bucket::{persist_page, GeneralHeader, GeneralPage, PageType, Persistable};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String,
    #[arg(short, long)]
    pages_count: u32,
    #[arg(long, default_value_t = ("hello".to_string()))]
    page_content: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    _ = remove_file(args.filename.as_str());
    let mut output_file = File::create(args.filename.as_str())?;

    for page_id in 0..args.pages_count {
        let header: GeneralHeader = GeneralHeader {
            space_id: 1.into(),
            page_id: page_id.into(),
            previous_id: (if (page_id > 0) { page_id - 1} else {0}).into(),
            next_id: (page_id + 1).into(),
            page_type: PageType::SpaceInfo,
            data_length: 0 as u32,
        };
        let mut page: GeneralPage<String> = GeneralPage { header, inner: args.page_content.clone() };

        persist_page(&mut page, &mut output_file)?;
    }

    Ok(())
}
