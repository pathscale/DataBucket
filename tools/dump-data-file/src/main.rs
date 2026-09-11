use clap::Parser;
use data_bucket::{parse_data_page, parse_general_header_by_index, PageType, GENERAL_HEADER_SIZE};

#[derive(Parser, Debug)]
#[command(about = "Inspect v3 page identities and live row extents without reading any index")]
struct Args {
    #[arg(short, long)]
    filename: String,
    #[arg(long, default_value_t = 16384)]
    page_size: u32,
    /// Include each live row archive as hexadecimal bytes. Row decoding belongs to the owning schema.
    #[arg(long)]
    hex: bool,
}

async fn dump<const STRIDE: u32, const CAPACITY: usize>(args: &Args) -> eyre::Result<()> {
    let file = std::fs::File::open(&args.filename)?;
    let length = file.metadata()?.len();
    let mut file = nagoya::io::HostFile::new(file);
    let mut rows = 0usize;
    for id in 0..length.div_ceil(u64::from(STRIDE)) {
        let id = u32::try_from(id)?;
        let header = parse_general_header_by_index::<STRIDE>(&mut file, id).await?;
        println!(
            "page {}: {:?}, format {}, initialized {}",
            id, header.page_type, header.data_version, header.data_length
        );
        if header.page_type != PageType::Data {
            continue;
        }
        let page = parse_data_page::<STRIDE, CAPACITY, STRIDE>(&mut file, id).await?;
        for slot in page.inner.rows {
            rows += 1;
            print!(
                "  row offset={} length={} file_offset={}",
                slot.offset,
                slot.length,
                u64::from(id) * u64::from(STRIDE)
                    + GENERAL_HEADER_SIZE as u64
                    + u64::from(slot.offset)
            );
            if args.hex {
                print!(" bytes=");
                for byte in &page.inner.data[slot.offset as usize..][..slot.length as usize] {
                    print!("{byte:02x}");
                }
            }
            println!();
        }
    }
    println!("live rows: {rows}");
    Ok(())
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();
    nagoya::block_on(async {
        match args.page_size {
            512 => dump::<512, { 512 - GENERAL_HEADER_SIZE }>(&args).await,
            4096 => dump::<4096, { 4096 - GENERAL_HEADER_SIZE }>(&args).await,
            8192 => dump::<8192, { 8192 - GENERAL_HEADER_SIZE }>(&args).await,
            16384 => dump::<16384, { 16384 - GENERAL_HEADER_SIZE }>(&args).await,
            32768 => dump::<32768, { 32768 - GENERAL_HEADER_SIZE }>(&args).await,
            _ => eyre::bail!("supported page sizes: 512, 4096, 8192, 16384, 32768"),
        }
    })
}
