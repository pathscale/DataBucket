//! What persisting a file's worth of pages costs, on the real path.
//!
//! Not a model of it: this calls `persist_page` and `persist_pages_batch`
//! themselves, on the async file handles they take.

use data_bucket::page::{persist_page, persist_pages_batch};
use data_bucket::{
    DataPage, GeneralHeader, GeneralPage, PageType, DATA_VERSION, DEFAULT_PAGE_STRIDE,
    INNER_PAGE_SIZE,
};
use std::time::Instant;
// The file is a `nagoya::io::File` now, so the trait has to be in scope and
// the openers come from `nagoya::io` rather than a runtime's own module.
use nagoya::io::File as _;

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
                    rows: Vec::new(),
                    length: INNER_PAGE_SIZE as u32,
                    data,
                },
            }
        })
        .collect()
}

/// Pages at every `stride`-th id, which is the shape of an update to a file
/// that already exists: the ids are not consecutive, so nothing coalesces.
fn scattered(stride: u32) -> Vec<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    pages()
        .into_iter()
        .enumerate()
        .filter(|(id, _)| (*id as u32).is_multiple_of(stride))
        .map(|(_, page)| page)
        .collect()
}

async fn existing(path: &std::path::Path) -> nagoya::io::HostFile {
    nagoya::io::open_or_create(path).await.unwrap()
}

async fn fresh(path: &std::path::Path) -> nagoya::io::HostFile {
    let _ = nagoya::io::remove_file(path).await;
    nagoya::io::create(path).await.unwrap()
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

    // **The order of the arms is a variable, so it is one that can be set.**
    // Run in a fixed order, whichever arm goes second inherits a file the first
    // arm just wrote and looks faster for it. `REVERSE=1` runs them the other
    // way round; the two orders agreeing is what makes either number mean
    // anything.
    let reverse = std::env::var("REVERSE").is_ok();
    let mut one_at_a_time = Vec::new();
    let mut batched = Vec::new();

    let run_one = async |timings: &mut Vec<f64>| {
        let mut all = pages();
        let mut file = fresh(&path).await;
        let at = Instant::now();
        for page in &mut all {
            persist_page::<_, DEFAULT_PAGE_STRIDE>(page, &mut file)
                .await
                .unwrap();
        }
        file.sync_all().await.unwrap();
        timings.push(at.elapsed().as_secs_f64());
    };
    let run_batch = async |timings: &mut Vec<f64>| {
        let all = pages();
        let mut file = fresh(&path).await;
        let at = Instant::now();
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(all, &mut file)
            .await
            .unwrap();
        file.sync_all().await.unwrap();
        timings.push(at.elapsed().as_secs_f64());
    };

    for _ in 0..REPS {
        if reverse {
            run_batch(&mut batched).await;
            run_one(&mut one_at_a_time).await;
        } else {
            run_one(&mut one_at_a_time).await;
            run_batch(&mut batched).await;
        }
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

    // ---- the case that is not a whole file
    //
    // Everything above rewrites the file from empty, so the ids run 0..PAGES
    // with no gaps and the batch path sees one enormous consecutive run. That
    // is its best case and a database's rarest one. Updating scattered pages
    // in a file that already exists breaks the run at every page, so the batch
    // path falls back to one write per page and can only win by what it saves
    // per page, not by joining anything up.
    const STRIDE: u32 = 10;
    let touched = scattered(STRIDE).len();
    let touched_bytes = touched * data_bucket::PAGE_SIZE;

    // Lay the whole file down once, outside the clock, so the updates land in
    // a file that is already the right length.
    {
        let mut file = fresh(&path).await;
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(pages(), &mut file)
            .await
            .unwrap();
        file.sync_all().await.unwrap();
    }

    let mut one_scattered = Vec::new();
    let mut batch_scattered = Vec::new();
    for _ in 0..REPS {
        let mut some = scattered(STRIDE);
        let mut file = existing(&path).await;
        let at = Instant::now();
        for page in &mut some {
            persist_page::<_, DEFAULT_PAGE_STRIDE>(page, &mut file)
                .await
                .unwrap();
        }
        file.sync_all().await.unwrap();
        one_scattered.push(at.elapsed().as_secs_f64());

        let some = scattered(STRIDE);
        let mut file = existing(&path).await;
        let at = Instant::now();
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(some, &mut file)
            .await
            .unwrap();
        file.sync_all().await.unwrap();
        batch_scattered.push(at.elapsed().as_secs_f64());
    }

    let (one_s, many_s) = (median(one_scattered), median(batch_scattered));
    println!(
        "\nevery {STRIDE}th page of an existing file, {touched} pages, {:.1} MB",
        touched_bytes as f64 / 1e6
    );
    println!(
        "  persist_page, one at a time     {:>8.1} ms   {:>6.0} MB/s",
        one_s * 1e3,
        touched_bytes as f64 / 1e6 / one_s
    );
    println!(
        "  persist_pages_batch             {:>8.1} ms   {:>6.0} MB/s   {:>5.2}x",
        many_s * 1e3,
        touched_bytes as f64 / 1e6 / many_s,
        one_s / many_s
    );

    let _ = std::fs::remove_file(&path);
}
