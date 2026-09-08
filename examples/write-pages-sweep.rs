//! Where batching starts to matter, as a function of how many pages a caller
//! actually hands over at once.
//!
//! The headline number for `persist_pages_batch` is measured at 6,400 pages in
//! one call. The question this answers is whether anything reaches that, and
//! what the two paths cost at the sizes a caller really passes.

use data_bucket::page::{persist_page, persist_pages_batch};
use data_bucket::{DataPage, GeneralHeader, GeneralPage, PageType, DATA_VERSION, INNER_PAGE_SIZE};
use std::time::Instant;

const SIZES: [usize; 8] = [1, 2, 4, 16, 64, 256, 1024, 6400];
const REPS: usize = 9;

fn pages(count: usize) -> Vec<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    (0..count as u32)
        .map(|id| {
            let mut data = [0u8; INNER_PAGE_SIZE];
            for (n, byte) in data.iter_mut().enumerate() {
                *byte = (n % 251) as u8;
            }
            GeneralPage {
                header: GeneralHeader {
                    data_version: DATA_VERSION,
                    space_id: 1.into(),
                    page_id: id.into(),
                    previous_id: 0.into(),
                    next_id: 0.into(),
                    page_type: PageType::Data,
                    data_length: 0,
                },
                inner: DataPage { length: INNER_PAGE_SIZE as u32, data },
            }
        })
        .collect()
}

async fn fresh(path: &std::path::Path) -> tokio::fs::File {
    tokio::fs::OpenOptions::new()
        .read(true).write(true).create(true).truncate(true)
        .open(path).await.unwrap()
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

#[tokio::main]
async fn main() {
    let path = std::env::var("SCRATCH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
        .join("data_bucket_sweep.wt");

    println!("  pages        MB   one-at-a-time      batched      gain");
    for count in SIZES {
        let bytes = count * data_bucket::PAGE_SIZE;
        let (mut ones, mut many) = (Vec::new(), Vec::new());
        // One untimed pass of each, so neither pays for the file appearing.
        { let mut f = fresh(&path).await; persist_pages_batch(pages(count), &mut f).await.unwrap(); f.sync_all().await.unwrap(); }
        for _ in 0..REPS {
            let mut all = pages(count);
            let mut file = fresh(&path).await;
            let at = Instant::now();
            for page in &mut all { persist_page(page, &mut file).await.unwrap(); }
            file.sync_all().await.unwrap();
            ones.push(at.elapsed().as_secs_f64());

            let all = pages(count);
            let mut file = fresh(&path).await;
            let at = Instant::now();
            persist_pages_batch(all, &mut file).await.unwrap();
            file.sync_all().await.unwrap();
            many.push(at.elapsed().as_secs_f64());
        }
        let (one, batch) = (median(ones), median(many));
        println!(
            "  {count:>5}   {:>7.2}      {:>7.2} ms    {:>7.2} ms    {:>5.2}x",
            bytes as f64 / 1e6, one * 1e3, batch * 1e3, one / batch
        );
    }
    let _ = std::fs::remove_file(&path);
}
