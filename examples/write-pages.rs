//! What persisting a file's worth of pages costs, on the real path.
//!
//! Not a model of it: this calls `persist_page` and `persist_pages_batch`
//! themselves, on the async file handles they take.

use data_bucket::page::{persist_page, persist_pages_batch};
use data_bucket::{DataPage, GeneralHeader, GeneralPage, PageType, DATA_VERSION, INNER_PAGE_SIZE};
use std::time::Instant;

const PAGES: u32 = 6_400;
const REPS: usize = 5;

fn pages() -> Vec<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    (0..PAGES)
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
                inner: DataPage {
                    length: INNER_PAGE_SIZE as u32,
                    data,
                },
            }
        })
        .collect()
}

async fn fresh(path: &std::path::Path) -> tokio::fs::File {
    tokio::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .unwrap()
}

#[tokio::main]
async fn main() {
    let path = std::env::var("SCRATCH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir())
        .join("data_bucket_write_pages.wt");
    let bytes = PAGES as usize * data_bucket::PAGE_SIZE;

    println!(
        "{PAGES} pages, {:.1} MB, median of {REPS}\n",
        bytes as f64 / 1e6
    );

    let mut one_at_a_time = Vec::new();
    let mut batched = Vec::new();
    for _ in 0..REPS {
        let mut all = pages();
        let mut file = fresh(&path).await;
        let at = Instant::now();
        for page in &mut all {
            persist_page(page, &mut file).await.unwrap();
        }
        file.sync_all().await.unwrap();
        one_at_a_time.push(at.elapsed().as_secs_f64());

        let all = pages();
        let mut file = fresh(&path).await;
        let at = Instant::now();
        persist_pages_batch(all, &mut file).await.unwrap();
        file.sync_all().await.unwrap();
        batched.push(at.elapsed().as_secs_f64());
    }

    let median = |mut v: Vec<f64>| {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    };
    let (one, many) = (median(one_at_a_time), median(batched));
    println!(
        "  persist_page, one at a time     {:>8.1} ms   {:>6.0} MB/s",
        one * 1e3,
        bytes as f64 / 1e6 / one
    );
    println!(
        "  persist_pages_batch             {:>8.1} ms   {:>6.0} MB/s   {:>5.2}x",
        many * 1e3,
        bytes as f64 / 1e6 / many,
        one / many
    );

    let _ = std::fs::remove_file(&path);
}
