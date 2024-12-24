use clap::Parser;
use data_bucket::{persistence::data::DataTypeValue, read_data_pages, PAGE_SIZE};
use std::{fs::File, str};

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let mut file = File::open(args.filename)?;

    let data_pages: Vec<Vec<DataTypeValue>> = read_data_pages::<PAGE_SIZE>(&mut file)?;

    for page in data_pages {
        println!("{:?}", page);
    }

    Ok(())
}
