use clap::Parser;
use std::{fs::File, str};

use data_bucket::load_pages;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let mut file = File::open(args.filename)?;
    let pages = load_pages(&mut file)?;

    for page in pages {
        println!("Header:");
        println!("{:?}", page.header);
        println!("Data:");
        println!("{}", str::from_utf8(&page.inner)?);
        println!();
    }

    Ok(())
}
