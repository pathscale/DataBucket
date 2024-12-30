use clap::Parser;
use data_bucket::{
    page::{parse_space_info, DataIterator, LinksIterator, PageIterator},
    persistence::data::DataTypeValue,
    read_data_pages, read_rows_schema, space, PAGE_SIZE,
};
use std::{fs::File, str};

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
    for i in 0..column_width - string.len() {
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

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    let mut file = File::open(args.filename)?;

    let space_info = parse_space_info::<PAGE_SIZE>(&mut file)?;
    let row_schema = space_info.row_schema.clone();

    let mut rows: Vec<Vec<DataTypeValue>> = vec![];

    let pages = PageIterator::new(space_info.primary_key_intervals.clone());
    for page in pages {
        let links = LinksIterator::new(&mut file, page, &space_info).collect::<Vec<_>>();
        for row in DataIterator::new(&mut file, row_schema.clone(), links) {
            rows.push(row);
        }
    }

    let rows: Vec<Vec<DataTypeValue>> = read_data_pages::<PAGE_SIZE>(&mut file)?;

    let header: Vec<String> = row_schema
        .iter()
        .map(|(column, _data_type)| column.to_owned())
        .collect();
    let rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|column| column.to_string())
                .collect::<Vec<String>>()
        })
        .collect();

    format_table(&header, &rows);

    Ok(())
}
