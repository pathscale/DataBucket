use clap::Parser;
use data_bucket::{
    page::parse_space_info, parse_data_page, parse_general_header_by_index,
    persistence::data::DataTypeValue, space, PAGE_SIZE,
};
use data_bucket::{parse_page, GeneralPage, SpaceInfoPage};
use std::str;
use tokio::fs::File;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    filename: String,
}

fn print_horizontal_cells_delimiters(column_widths: &[usize]) {
    print!("+");
    for column_width in column_widths.iter() {
        print!("-");
        for _ in 0..*column_width {
            print!("-");
        }
        print!("-+");
    }
    println!();
}

fn print_padded_string(string: &str, column_width: usize) {
    print!("{}", string);
    for _ in 0..column_width - string.len() {
        print!(" ");
    }
}

fn format_table(header: &Vec<String>, rows: &Vec<Vec<String>>) {
    let mut column_widths = vec![0; header.len()];
    for i in 0..header.len() {
        column_widths[i] = header[i].len();
    }
    for row in rows.iter() {
        for i in 0..row.len() {
            if row[i].len() > column_widths[i] {
                column_widths[i] = row[i].len();
            }
        }
    }

    print_horizontal_cells_delimiters(&column_widths[..]);
    print!("|");
    for i in 0..header.len() {
        print!(" ");
        print_padded_string(header[i].as_str(), column_widths[i]);
        print!(" |");
    }
    println!();
    print_horizontal_cells_delimiters(&column_widths[..]);
    for row in rows.iter() {
        print!("|");
        for i in 0..row.len() {
            print!(" ");
            print_padded_string(row[i].as_str(), column_widths[i]);
            print!(" |");
        }
        println!();
    }
    print_horizontal_cells_delimiters(&column_widths[..]);
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let mut file = File::open(args.filename).await.unwrap();

    println!("{:?}", file.metadata().await);

    let space_info = parse_space_info::<PAGE_SIZE>(&mut file).await;

    let info = parse_general_header_by_index(&mut file, 0).await;
    let info2 = parse_data_page::<PAGE_SIZE, PAGE_SIZE>(&mut file, 0).await;

    let space_info2 = parse_page::<SpaceInfoPage<()>, { PAGE_SIZE as u32 }>(&mut file, 0)
        .await
        .unwrap();

    //let t1 = space_info.header;

    let _rows: Vec<Vec<DataTypeValue>> = vec![];

    println!("1{:?}", space_info);
    println!("2{:?}", space_info2);

    println!("Info {:?}", info);
    //println!("Info {:?}", info2);
    // println!("Head er {:?}", t1);

    //let pages = PageIterator::new(space_info.unwrap().primary_key_fields.clone());
    //  for page in pages {
    //let links = LinksIterator::new(&mut file, page, &space_info).collect::<Vec<_>>();
    //      for row in DataIterator::new(&mut file, row_schema.clone(), links) {
    //           rows.push(row);
    //      }
    //}

    //let rows: Vec<Vec<DataTypeValue>> = read_data_pages::<PAGE_SIZE>(&mut file)?;

    Ok(())
}
