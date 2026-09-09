use crate::error::Error;
use crate::AsyncFile;
use nagoya::io::SeekFrom;
use rkyv::api::high::HighDeserializer;
use rkyv::Archive;

use super::SpaceInfoPage;
use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::PageId;
use crate::{DataPage, GeneralPage, Link, Persistable, GENERAL_HEADER_SIZE};

/// Returned when a write into a page would not fit the page slot: letting
/// it through would spill past a [`PAGE_SIZE`] boundary and corrupt a
/// neighboring page (or, for tail-first writes, this page's own header).
#[derive(Debug)]
pub struct PageOverflowError {
    /// The page whose write is over budget.
    pub page_id: PageId,
    /// Inner-page bytes the write needs (for slot writes, where the write
    /// would end within the slot).
    pub data_length: usize,
    /// The slot budget for inner data ([`INNER_PAGE_SIZE`]).
    pub capacity: usize,
}

impl std::fmt::Display for PageOverflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "page {} write needs {} bytes, exceeding the {}-byte page slot",
            self.page_id, self.data_length, self.capacity
        )
    }
}

impl std::error::Error for PageOverflowError {}

pub fn map_data_pages_to_general<const DATA_LENGTH: usize>(
    pages: Vec<DataPage<DATA_LENGTH>>,
) -> Vec<GeneralPage<DataPage<DATA_LENGTH>>> {
    // We are starting ID's from `1` because `0`'s page in file is info page.
    let header = &mut GeneralHeader::new(1.into(), PageType::Data, 0.into());
    let mut general_pages = vec![];

    let mut pages = pages.into_iter();
    if let Some(p) = pages.next() {
        let general = GeneralPage {
            header: *header,
            inner: p,
        };
        general_pages.push(general);
    }
    let mut previous_header = header;

    for p in pages {
        let general = GeneralPage {
            header: previous_header.follow_with(PageType::Data),
            inner: p,
        };

        general_pages.push(general);
        previous_header = &mut general_pages.last_mut().unwrap().header;
    }

    general_pages
}

pub async fn persist_page<'a, T, const STRIDE: u32>(
    page: &'a mut GeneralPage<T>,
    file: &'a mut impl AsyncFile,
) -> crate::error::Result<()>
where
    T: Persistable + Send + Sync,
{
    seek_to_page_start::<STRIDE>(file, page.header.page_id.0).await?;

    // The cursor is at the page start and `persist_page_in_place` says how far
    // it moved, so the padding to the next page boundary is arithmetic.
    //
    // It used to ask the file instead, with `stream_position`, which is a
    // system call per page for two numbers already in hand. Measured over
    // 6,400 pages, about 14% of the write.
    let written = persist_page_in_place::<T, STRIDE>(page, file).await?;
    // Checked, because a page that wrote more than its slot must not turn into
    // a `usize` underflow and an absurd seek. The inner length is already
    // guarded against this page's own stride, so reaching this needs a header
    // that serialises to more than `GENERAL_HEADER_SIZE`.
    let stride = STRIDE as usize;
    let padding = stride.checked_sub(written).ok_or(Error::PageOverflow {
        page: page.header.page_id,
        needed: written,
        capacity: stride,
    })?;
    if padding > 0 {
        file.seek(SeekFrom::Current(padding as i64)).await?;
    }

    Ok(())
}

/// Write one page where the cursor already is, and return how many bytes that
/// took.
///
/// **The length is the point of the return value.** A caller that has to leave
/// the cursor on the next page boundary needs it, and asking the file where it
/// ended up costs a system call for something computed two lines above.
async fn persist_page_in_place<'a, T, const STRIDE: u32>(
    page: &'a mut GeneralPage<T>,
    file: &'a mut impl AsyncFile,
) -> crate::error::Result<usize>
where
    T: Persistable + Send + Sync,
{
    let inner_bytes = page.inner.as_bytes();
    let inner_length = inner_bytes.as_ref().len();
    // An over-budget page must fail here, in its own persist, instead of
    // silently corrupting the neighboring page. The budget is this page's own
    // stride less its header, not the crate default: a table writing a larger
    // page must be allowed to fill it, and a table writing a smaller one must
    // be stopped before it overruns.
    let capacity = STRIDE as usize - GENERAL_HEADER_SIZE;
    if inner_length > capacity {
        return Err(Error::PageOverflow {
            page: page.header.page_id,
            needed: inner_length,
            capacity,
        });
    }
    page.header.data_length = inner_length as u32;
    let header_bytes = page.header.as_bytes();
    let header_length = header_bytes.as_ref().len();
    file.write_all(header_bytes.as_ref()).await?;
    file.write_all(inner_bytes.as_ref()).await?;
    Ok(header_length + inner_length)
}

pub async fn persist_pages_batch<T, const STRIDE: u32>(
    pages: Vec<GeneralPage<T>>,
    file: &mut impl AsyncFile,
) -> crate::error::Result<()>
where
    T: Persistable + Send + Sync,
{
    // **One write for a run of consecutive pages, not one per page.**
    //
    // Every page in a run occupies exactly `STRIDE` at a known offset, so a
    // run can be laid out in memory and handed to the file in a single call.
    // This said `PAGE_SIZE` when the stride was a crate constant; it is a
    // parameter now, and the run arithmetic below follows the parameter.
    // Writing them one at a time, with a seek between each, measured 76.0 ms
    // against 10.3 for the same 104 MB in one write.
    //
    // The run is broken whenever the page ids stop being consecutive, because
    // then the offsets are not contiguous and the buffer would no longer
    // correspond to a stretch of the file. Callers usually pass a contiguous
    // batch and get one write; a caller that does not still gets a correct
    // file, one write per run.
    //
    // **The buffer is bounded.** A run of ten thousand pages is 160 MB, and
    // this runs on a virtual machine whose memory is not ours to spend. A run
    // longer than `MAX_RUN_PAGES` is flushed in pieces, each still one write,
    // each still at the right offset.
    //
    // **One difference from writing page by page, and it is on disk rather
    // than in the bytes.** Writing one at a time seeks over the space between
    // a page's content and the next page's start, and on a filesystem that
    // supports holes that space is never allocated. Writing a run in one call
    // puts explicit zeroes there. A file read back is identical either way,
    // which is what the tests hold; what differs is blocks allocated, and on
    // `ext4` a file of half-empty pages will now occupy what it claims to.
    // Data pages are full, so their padding is nothing; index and space pages
    // are not.
    /// Pages buffered before a run is flushed regardless of how long it is.
    /// 512 pages is 8 MiB at the default page size.
    const MAX_RUN_PAGES: u32 = 512;

    let mut iter = pages.into_iter().peekable();
    let mut buffer: Vec<u8> = Vec::new();
    let mut run_start: Option<u32> = None;
    let mut expected_next: u32 = 0;

    while let Some(mut page) = iter.next() {
        let id = page.header.page_id.0;
        let breaks_run = run_start.is_some() && id != expected_next;
        if breaks_run {
            flush_run::<STRIDE>(file, run_start.take(), &mut buffer).await?;
        }
        if run_start.is_none() {
            run_start = Some(id);
        }
        let start = run_start.expect("just set");

        // **Pad before the next page, never after the last one.** Writing one
        // page at a time only ever *seeks* past the end of a page, and a seek
        // past the end of a file does not extend it, so the last page written
        // leaves the file at its content length rather than at a page
        // boundary. Padding after every page would make the file longer than
        // the path this replaces produces, which is a change nobody asked for.
        let offset_in_run = (id - start) as usize * STRIDE as usize;
        buffer.resize(offset_in_run, 0);
        persist_page_in_place_to::<T, STRIDE>(&mut page, &mut buffer)?;

        expected_next = id.checked_add(1).ok_or(Error::Corrupt {
            what: "page id overflowed while batching",
        })?;

        // Flush at the end, and before the buffer grows past its bound. The
        // next page then starts a fresh run at its own offset, which is
        // correct because that offset is absolute.
        let run_is_long = id - start + 1 >= MAX_RUN_PAGES;
        if iter.peek().is_none() || run_is_long {
            flush_run::<STRIDE>(file, run_start.take(), &mut buffer).await?;
        }
    }

    Ok(())
}

/// Write an accumulated run of pages at the offset its first page names.
async fn flush_run<const STRIDE: u32>(
    file: &mut impl AsyncFile,
    run_start: Option<u32>,
    buffer: &mut Vec<u8>,
) -> crate::error::Result<()> {
    if let Some(start) = run_start {
        if !buffer.is_empty() {
            file.seek(SeekFrom::Start(page_start_offset::<STRIDE>(start)))
                .await?;
            file.write_all(buffer).await?;
        }
    }
    buffer.clear();
    Ok(())
}

/// The same as [`persist_page_in_place`], into memory rather than a file.
///
/// Shares the over-budget check, because a page too large for its slot must be
/// refused on both paths or the batch one becomes a way around it.
fn persist_page_in_place_to<T, const STRIDE: u32>(
    page: &mut GeneralPage<T>,
    out: &mut Vec<u8>,
) -> crate::error::Result<usize>
where
    T: Persistable + Send + Sync,
{
    let inner_bytes = page.inner.as_bytes();
    let inner_length = inner_bytes.as_ref().len();
    // Same budget as the file path: this page's own stride less its header.
    let capacity = STRIDE as usize - GENERAL_HEADER_SIZE;
    if inner_length > capacity {
        return Err(Error::PageOverflow {
            page: page.header.page_id,
            needed: inner_length,
            capacity,
        });
    }
    page.header.data_length = inner_length as u32;
    let header_bytes = page.header.as_bytes();
    let header_length = header_bytes.as_ref().len();
    out.extend_from_slice(header_bytes.as_ref());
    out.extend_from_slice(inner_bytes.as_ref());
    Ok(header_length + inner_length)
}

/// Byte offset of the page with the given index, computed in `u64`.
///
/// The arithmetic must never be done in `u32`: with the default 16 KiB
/// [`PAGE_SIZE`], any index past 262 143 puts the page start beyond 4 GiB,
/// and a `u32` multiply silently wraps the offset back into the start of
/// the file.
pub(crate) fn page_start_offset<const STRIDE: u32>(index: u32) -> u64 {
    index as u64 * u64::from(STRIDE)
}

pub async fn seek_to_page_start<const STRIDE: u32>(
    file: &mut impl AsyncFile,
    index: u32,
) -> crate::error::Result<()> {
    file.seek(SeekFrom::Start(page_start_offset::<STRIDE>(index)))
        .await?;
    Ok(())
}

pub async fn seek_by_link<const STRIDE: u32>(
    file: &mut impl AsyncFile,
    link: Link,
) -> crate::error::Result<()> {
    file.seek(SeekFrom::Start(
        page_start_offset::<STRIDE>(link.page_id.0)
            + GENERAL_HEADER_SIZE as u64
            + link.offset as u64,
    ))
    .await?;

    Ok(())
}

pub async fn update_at<const DATA_LENGTH: u32, const STRIDE: u32>(
    file: &mut impl AsyncFile,
    link: Link,
    new_data: &[u8],
) -> crate::error::Result<()> {
    if new_data.len() as u32 != link.length {
        return Err(Error::LinkLengthMismatch {
            expected: link.length,
            found: new_data.len(),
        });
    }

    // Sum in u64: `offset + length` in u32 can wrap past 4 GiB and slip
    // under the bound, letting the write land outside the page.
    if link.offset as u64 + link.length as u64 > DATA_LENGTH as u64 {
        return Err(Error::LinkOutOfBounds {
            offset: link.offset,
            length: link.length,
            capacity: DATA_LENGTH as usize,
        });
    }

    seek_by_link::<STRIDE>(file, link).await?;
    file.write_all(new_data).await?;
    Ok(())
}

pub async fn parse_general_header(
    file: &mut impl AsyncFile,
) -> crate::error::Result<GeneralHeader> {
    let mut buffer = [0; GENERAL_HEADER_SIZE];
    file.read_exact(&mut buffer).await?;
    // Validated: a header torn by a mid-write death must surface as an error
    // naming the page, not as undefined behavior in whatever reads it next.
    let archived = crate::access_archived::<<GeneralHeader as Archive>::Archived>(&buffer[..])
        .map_err(|_| Error::Corrupt {
            what: "page header",
        })?;
    let header =
        rkyv::deserialize::<_, rkyv::rancor::Error>(archived).map_err(|_| Error::Corrupt {
            what: "page header",
        })?;

    Ok(header)
}

pub async fn parse_page<Page, const INNER_PAGE_SIZE: u32, const STRIDE: u32>(
    file: &mut impl AsyncFile,
    index: u32,
) -> crate::error::Result<GeneralPage<Page>>
where
    Page: rkyv::Archive + Persistable,
    <Page as rkyv::Archive>::Archived:
        rkyv::Deserialize<Page, HighDeserializer<rkyv::rancor::Error>>,
{
    seek_to_page_start::<STRIDE>(file, index).await?;
    parse_page_in_place::<Page, INNER_PAGE_SIZE>(file).await
}

async fn parse_page_in_place<Page, const INNER_PAGE_SIZE: u32>(
    file: &mut impl AsyncFile,
) -> crate::error::Result<GeneralPage<Page>>
where
    Page: rkyv::Archive + Persistable,
    <Page as rkyv::Archive>::Archived:
        rkyv::Deserialize<Page, HighDeserializer<rkyv::rancor::Error>>,
{
    let header = parse_general_header(file).await?;
    let length = if header.data_length == 0 {
        INNER_PAGE_SIZE
    } else {
        header.data_length
    };

    let mut buffer: Vec<u8> = vec![0u8; length as usize];
    file.read_exact(&mut buffer).await?;
    let info = Page::from_bytes(buffer.as_ref(), header.data_version);

    Ok(GeneralPage {
        header,
        inner: info,
    })
}

pub async fn parse_pages_batch<Page, const PAGE_SIZE: u32, const STRIDE: u32>(
    file: &mut impl AsyncFile,
    indexes: Vec<u32>,
) -> crate::error::Result<Vec<GeneralPage<Page>>>
where
    Page: rkyv::Archive + Persistable,
    <Page as rkyv::Archive>::Archived:
        rkyv::Deserialize<Page, HighDeserializer<rkyv::rancor::Error>>,
{
    let mut iter = indexes.into_iter();
    if let Some(index) = iter.next() {
        let mut pages = vec![];
        seek_to_page_start::<STRIDE>(file, index).await?;
        let page = parse_page_in_place::<Page, PAGE_SIZE>(file).await?;
        pages.push(page);

        for index in iter {
            seek_to_page_start::<STRIDE>(file, index).await?;
            let page = parse_page_in_place::<Page, PAGE_SIZE>(file).await?;
            pages.push(page);
        }

        Ok(pages)
    } else {
        Ok(vec![])
    }
}

pub async fn parse_general_header_by_index<const STRIDE: u32>(
    file: &mut impl AsyncFile,
    index: u32,
) -> crate::error::Result<GeneralHeader> {
    seek_to_page_start::<STRIDE>(file, index).await?;
    let header = parse_general_header(file).await?;

    Ok(header)
}

pub async fn parse_data_page<
    const PAGE_SIZE: u32,
    const INNER_PAGE_SIZE: usize,
    const STRIDE: u32,
>(
    file: &mut impl AsyncFile,
    index: u32,
) -> crate::error::Result<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    seek_to_page_start::<STRIDE>(file, index).await?;
    parse_data_page_in_place::<PAGE_SIZE, INNER_PAGE_SIZE>(file).await
}

async fn parse_data_page_in_place<const PAGE_SIZE: u32, const INNER_PAGE_SIZE: usize>(
    file: &mut impl AsyncFile,
) -> crate::error::Result<GeneralPage<DataPage<INNER_PAGE_SIZE>>> {
    let header = parse_general_header(file).await?;

    let mut buffer = [0u8; INNER_PAGE_SIZE];
    if header.next_id == 0.into() {
        #[allow(clippy::unused_io_amount)]
        file.read(&mut buffer).await?;
    } else {
        file.read_exact(&mut buffer).await?;
    }

    let data = DataPage {
        data: buffer,
        length: header.data_length,
    };

    Ok(GeneralPage {
        header,
        inner: data,
    })
}

pub async fn parse_data_pages_batch<
    const PAGE_SIZE: u32,
    const INNER_PAGE_SIZE: usize,
    const STRIDE: u32,
>(
    file: &mut impl AsyncFile,
    indexes: Vec<u32>,
) -> crate::error::Result<Vec<GeneralPage<DataPage<INNER_PAGE_SIZE>>>> {
    let mut iter = indexes.into_iter();
    if let Some(index) = iter.next() {
        let mut pages = vec![];
        seek_to_page_start::<STRIDE>(file, index).await?;
        let page = parse_data_page_in_place::<PAGE_SIZE, INNER_PAGE_SIZE>(file).await?;
        pages.push(page);

        for index in iter {
            seek_to_page_start::<STRIDE>(file, index).await?;
            let page = parse_data_page_in_place::<PAGE_SIZE, INNER_PAGE_SIZE>(file).await?;
            pages.push(page);
        }

        Ok(pages)
    } else {
        Ok(vec![])
    }
}

// pub fn parse_data_record<const PAGE_SIZE: usize>(
//     file: &mut std::fs::File,
//     index: u32,
//     offset: u32,
//     length: u32,
//     schema: &Vec<(String, String)>,
// ) -> crate::error::Result<Vec<DataTypeValue>> {
//     seek_to_page_start(file, index)?;
//     let header = parse_general_header(file)?;
//     if header.page_type != PageType::Data {
//         return Err(eyre::Report::msg(format!(
//             "The type of the page with index {} is not `Data`",
//             index
//         )));
//     }
//     file.seek(io::SeekFrom::Current(offset as i64))?;
//     let mut buffer = vec![0u8; length as usize];
//     file.read_exact(&mut buffer)?;
//
//     let parsed_record = parse_archived_row(&buffer, &schema);
//
//     Ok(parsed_record)
// }

pub async fn parse_space_info<const PAGE_SIZE: usize>(
    file: &mut impl AsyncFile,
) -> crate::error::Result<SpaceInfoPage> {
    file.seek(SeekFrom::Start(0)).await?;
    let header = parse_general_header(file).await?;

    let mut buffer = vec![0u8; header.data_length as usize];
    file.read_exact(&mut buffer).await?;

    Ok(SpaceInfoPage::from_bytes(&buffer, header.data_version))
}

// pub fn read_index_pages<T, const PAGE_SIZE: usize>(
//     file: &mut std::fs::File,
//     length: u32,
// ) -> crate::error::Result<Vec<IndexValue<T>>>
// where
//     T: Archive,
//     <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, HighDeserializer<rkyv::rancor::Error>>,
// {
//     let mut result: Vec<IndexValue<T>> = vec![];
//     for index in 0..length {
//         let mut index_records = parse_index_page::<T, PAGE_SIZE>(file, index)?;
//         result.append(&mut index_records);
//     }
//     Ok(result)
// }
//
// fn read_links<DataType, const PAGE_SIZE: usize>(
//     mut file: &mut std::fs::File,
//     space_info: &SpaceInfo,
// ) -> crate::error::Result<Vec<Link>> {
//     Ok(
//         read_index_pages::<i32, PAGE_SIZE>(&mut file, space_info.primary_key_length)?
//             .iter()
//             .map(|index_value| index_value.link)
//             .collect::<Vec<Link>>(),
//     )
// }
//
// pub fn read_rows_schema<const PAGE_SIZE: usize>(
//     file: &mut std::fs::File,
// ) -> crate::error::Result<Vec<(String, String)>> {
//     let space_info = parse_space_info::<PAGE_SIZE>(file)?;
//     Ok(space_info.row_schema)
// }
//
// pub fn read_data_pages<const PAGE_SIZE: usize>(
//     mut file: &mut std::fs::File,
// ) -> crate::error::Result<Vec<Vec<DataTypeValue>>> {
//     let space_info = parse_space_info::<PAGE_SIZE>(file)?;
//     let primary_key_fields = &space_info.primary_key_fields;
//     if primary_key_fields.len() != 1 {
//         panic!("Currently only single primary key is supported");
//     }
//
//     let primary_key_type = space_info
//         .row_schema
//         .iter()
//         .filter(|(field_name, _)| field_name == &primary_key_fields[0])
//         .map(|(_, field_type)| field_type)
//         .take(1)
//         .collect::<Vec<&String>>()[0]
//         .as_str();
//     let links = match primary_key_type {
//         "String" => read_links::<String, PAGE_SIZE>(&mut file, &space_info)?,
//         "i128" => read_links::<i128, PAGE_SIZE>(&mut file, &space_info)?,
//         "i64" => read_links::<i64, PAGE_SIZE>(&mut file, &space_info)?,
//         "i32" => read_links::<i32, PAGE_SIZE>(&mut file, &space_info)?,
//         "i16" => read_links::<i16, PAGE_SIZE>(&mut file, &space_info)?,
//         "i8" => read_links::<i8, PAGE_SIZE>(&mut file, &space_info)?,
//         "u128" => read_links::<u128, PAGE_SIZE>(&mut file, &space_info)?,
//         "u64" => read_links::<u64, PAGE_SIZE>(&mut file, &space_info)?,
//         "u32" => read_links::<u32, PAGE_SIZE>(&mut file, &space_info)?,
//         "u16" => read_links::<u16, PAGE_SIZE>(&mut file, &space_info)?,
//         "u8" => read_links::<u8, PAGE_SIZE>(&mut file, &space_info)?,
//         "f64" => read_links::<f64, PAGE_SIZE>(&mut file, &space_info)?,
//         "f32" => read_links::<f32, PAGE_SIZE>(&mut file, &space_info)?,
//         _ => panic!("Unsupported primary key data type `{}`", primary_key_type),
//     };
//
//     let mut result: Vec<Vec<_>> = vec![];
//     for link in links {
//         let row = parse_data_record::<{ PAGE_SIZE as u32 }>(
//             &mut file,
//             link.page_id.0,
//             link.offset,
//             link.length,
//             &space_info.row_schema,
//         )?;
//         result.push(row);
//     }
//
//     Ok(result)
// }

#[cfg(test)]
mod tests {
    use super::{page_start_offset, parse_data_pages_batch, persist_pages_batch};
    use crate::page::header::GeneralHeader;
    use crate::page::ty::PageType;
    use crate::{
        DataPage, GeneralPage, DATA_VERSION, DEFAULT_PAGE_STRIDE, INNER_PAGE_SIZE, PAGE_SIZE,
    };
    // `sync_all` and the rest are trait methods now, not inherent ones, so the
    // trait has to be in scope for a `HostFile` to answer to them.
    use nagoya::io::File as _;

    /// First page index whose start offset no longer fits in `u32`.
    const FIRST_PAGE_PAST_4_GIB: u32 = (u32::MAX / PAGE_SIZE as u32) + 1;

    #[test]
    fn page_start_offset_is_computed_in_u64() {
        assert_eq!(
            page_start_offset::<{ PAGE_SIZE as u32 }>(FIRST_PAGE_PAST_4_GIB),
            FIRST_PAGE_PAST_4_GIB as u64 * PAGE_SIZE as u64
        );
        assert!(page_start_offset::<{ PAGE_SIZE as u32 }>(FIRST_PAGE_PAST_4_GIB) > u32::MAX as u64);
        // The largest possible page id must be addressable too.
        assert_eq!(
            page_start_offset::<{ PAGE_SIZE as u32 }>(u32::MAX),
            u32::MAX as u64 * PAGE_SIZE as u64
        );
        // The old `u32` arithmetic wrapped this offset back into the first
        // pages of the file.
        assert_ne!(
            page_start_offset::<{ PAGE_SIZE as u32 }>(FIRST_PAGE_PAST_4_GIB),
            FIRST_PAGE_PAST_4_GIB.wrapping_mul(PAGE_SIZE as u32) as u64
        );
    }

    fn data_page_with_marker(marker: &[u8]) -> DataPage<INNER_PAGE_SIZE> {
        let mut data = [0u8; INNER_PAGE_SIZE];
        data[..marker.len()].copy_from_slice(marker);
        DataPage {
            length: marker.len() as u32,
            data,
        }
    }

    async fn scratch(name: &str) -> (std::path::PathBuf, nagoya::io::HostFile) {
        let path =
            std::env::temp_dir().join(format!("data_bucket_{name}_{}.wt", std::process::id()));
        let _ = nagoya::io::remove_file(&path).await;
        let file = nagoya::io::create(&path).await.unwrap();
        (path, file)
    }

    /// A batch must land byte for byte where the same pages written one at a
    /// time would land.
    ///
    /// This is the guard on coalescing a run into one write: the whole point is
    /// that it is not observable in the file, only in how long it took.
    #[tokio::test]
    async fn a_batch_writes_what_one_at_a_time_writes() {
        let markers: [&[u8]; 4] = [b"alpha", b"beta", b"gamma", b"delta"];

        let (one_path, mut one) = scratch("batch_one_at_a_time").await;
        for (n, marker) in markers.iter().enumerate() {
            let mut page = page_at::<INNER_PAGE_SIZE>(n as u32, marker);
            super::persist_page::<_, DEFAULT_PAGE_STRIDE>(&mut page, &mut one)
                .await
                .unwrap();
        }
        one.sync_all().await.unwrap();
        drop(one);

        let (many_path, mut many) = scratch("batch_together").await;
        let pages: Vec<_> = markers
            .iter()
            .enumerate()
            .map(|(n, marker)| page_at::<INNER_PAGE_SIZE>(n as u32, marker))
            .collect();
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(pages, &mut many)
            .await
            .unwrap();
        many.sync_all().await.unwrap();
        drop(many);

        let expected = std::fs::read(&one_path).unwrap();
        let actual = std::fs::read(&many_path).unwrap();
        assert_eq!(
            expected.len(),
            actual.len(),
            "the batch produced a file of a different length"
        );
        assert_eq!(expected, actual, "the batch produced different bytes");

        std::fs::remove_file(&one_path).unwrap();
        std::fs::remove_file(&many_path).unwrap();
    }

    /// Page ids with a gap in them are two runs, and each has to land at the
    /// offset its own id names rather than after the one before it.
    #[tokio::test]
    async fn a_batch_with_a_gap_puts_each_page_at_its_own_offset() {
        let (path, mut file) = scratch("batch_with_a_gap").await;
        let pages = vec![
            page_at::<INNER_PAGE_SIZE>(0, b"first"),
            page_at::<INNER_PAGE_SIZE>(1, b"second"),
            // The gap: nothing at 2 or 3.
            page_at::<INNER_PAGE_SIZE>(4, b"fifth"),
        ];
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(pages, &mut file)
            .await
            .unwrap();
        file.sync_all().await.unwrap();
        drop(file);

        let bytes = std::fs::read(&path).unwrap();
        // The last page is not padded, exactly as writing one at a time leaves
        // it: page four's offset, its header, and its five bytes of marker.
        assert_eq!(
            bytes.len(),
            4 * PAGE_SIZE + crate::GENERAL_HEADER_SIZE + b"fifth".len(),
            "the file is the wrong length"
        );
        let marker_at = |page: usize, marker: &[u8]| {
            let from = page * PAGE_SIZE + crate::GENERAL_HEADER_SIZE;
            assert_eq!(
                &bytes[from..from + marker.len()],
                marker,
                "page {page} holds the wrong data"
            );
        };
        marker_at(0, b"first");
        marker_at(1, b"second");
        marker_at(4, b"fifth");
        // The skipped pages are zeroes, not a copy of anything.
        let gap = 2 * PAGE_SIZE + crate::GENERAL_HEADER_SIZE;
        assert!(
            bytes[gap..gap + 16].iter().all(|&b| b == 0),
            "the gap was written over"
        );

        std::fs::remove_file(&path).unwrap();
    }

    /// A run longer than the buffer bound is flushed in pieces, and the pieces
    /// have to join up exactly.
    ///
    /// This is the guard on bounding the buffer. The bound exists so a batch of
    /// ten thousand pages does not become a 160 MB allocation on a virtual
    /// machine, and the risk it introduces is a seam every 512 pages.
    #[tokio::test]
    async fn a_run_longer_than_the_buffer_bound_still_joins_up() {
        // Comfortably past `MAX_RUN_PAGES`, so at least one seam is crossed.
        const COUNT: u32 = 520;

        let (one_path, mut one) = scratch("long_run_one_at_a_time").await;
        for id in 0..COUNT {
            let mut page = page_at::<INNER_PAGE_SIZE>(id, format!("page{id}").as_bytes());
            super::persist_page::<_, DEFAULT_PAGE_STRIDE>(&mut page, &mut one)
                .await
                .unwrap();
        }
        one.sync_all().await.unwrap();
        drop(one);

        let (many_path, mut many) = scratch("long_run_batched").await;
        let pages: Vec<_> = (0..COUNT)
            .map(|id| page_at::<INNER_PAGE_SIZE>(id, format!("page{id}").as_bytes()))
            .collect();
        persist_pages_batch::<_, DEFAULT_PAGE_STRIDE>(pages, &mut many)
            .await
            .unwrap();
        many.sync_all().await.unwrap();
        drop(many);

        let expected = std::fs::read(&one_path).unwrap();
        let actual = std::fs::read(&many_path).unwrap();
        assert_eq!(expected.len(), actual.len(), "lengths differ across a seam");
        assert_eq!(expected, actual, "bytes differ across a seam");

        std::fs::remove_file(&one_path).unwrap();
        std::fs::remove_file(&many_path).unwrap();
    }

    #[tokio::test]
    async fn persist_page_rejects_inner_data_past_the_page_slot() {
        // A data page whose buffer is larger than the slot budget can hand
        // persist more bytes than fit between two page starts.
        const OVERSIZED: usize = crate::PAGE_SIZE + 128;

        let path = std::env::temp_dir().join(format!(
            "data_bucket_persist_overflow_{}.wt",
            std::process::id()
        ));
        let mut file = nagoya::io::create(&path).await.unwrap();

        let mut page = GeneralPage {
            header: GeneralHeader {
                data_version: DATA_VERSION,
                space_id: 1.into(),
                page_id: 1.into(),
                previous_id: 0.into(),
                next_id: 0.into(),
                page_type: PageType::Data,
                data_length: 0,
            },
            inner: DataPage {
                length: OVERSIZED as u32,
                data: [7u8; OVERSIZED],
            },
        };

        let err = super::persist_page::<_, { PAGE_SIZE as u32 }>(&mut page, &mut file)
            .await
            .unwrap_err();
        assert!(
            matches!(err, crate::error::Error::PageOverflow { .. }),
            "expected a page overflow, got: {err}"
        );

        // Nothing may have been written: the neighboring page is the one an
        // unchecked write would have corrupted.
        assert_eq!(nagoya::io::File::length(&mut file).await.unwrap(), 0);

        // A page that fits its slot still persists.
        page.inner.length = 64;
        super::persist_page::<_, { PAGE_SIZE as u32 }>(&mut page, &mut file)
            .await
            .unwrap();

        drop(file);
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn update_at_rejects_offset_plus_length_wrapping_u32() {
        let path = std::env::temp_dir().join(format!(
            "data_bucket_update_at_wrap_{}.wt",
            std::process::id()
        ));
        let mut file = nagoya::io::create(&path).await.unwrap();

        // In u32, offset + length wraps to 5 and used to pass the bounds
        // check, sending the write far outside the page.
        let link = crate::Link {
            page_id: 1.into(),
            offset: u32::MAX - 2,
            length: 8,
        };
        let err = super::update_at::<100, { PAGE_SIZE as u32 }>(
            &mut file,
            link,
            &[1, 2, 3, 4, 5, 6, 7, 8],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, crate::error::Error::LinkOutOfBounds { .. }));

        drop(file);
        std::fs::remove_file(&path).unwrap();
    }

    #[tokio::test]
    async fn batch_persist_and_parse_address_pages_past_4_gib() {
        const FIRST_MARKER: &[u8] = b"FIRSTPG!";
        const BOUNDARY_MARKER: &[u8] = b"BOUNDARY";

        let path = std::env::temp_dir().join(format!(
            "data_bucket_seek_past_4gib_{}.wt",
            std::process::id()
        ));
        let mut file = nagoya::io::create(&path).await.unwrap();

        let first_page = GeneralPage {
            header: GeneralHeader {
                data_version: DATA_VERSION,
                space_id: 1.into(),
                page_id: 1.into(),
                previous_id: 0.into(),
                next_id: 2.into(),
                page_type: PageType::Data,
                data_length: 0,
            },
            inner: data_page_with_marker(FIRST_MARKER),
        };
        let boundary_page = GeneralPage {
            header: GeneralHeader {
                data_version: DATA_VERSION,
                space_id: 1.into(),
                page_id: FIRST_PAGE_PAST_4_GIB.into(),
                previous_id: 1.into(),
                next_id: 0.into(),
                page_type: PageType::Data,
                data_length: 0,
            },
            inner: data_page_with_marker(BOUNDARY_MARKER),
        };

        persist_pages_batch::<_, { PAGE_SIZE as u32 }>(vec![first_page, boundary_page], &mut file)
            .await
            .unwrap();

        // The boundary page must have been written past 4 GiB (the file is
        // sparse, so this stays cheap), not wrapped back onto the first pages.
        // the async file buffers writes; flush so metadata() sees them.
        nagoya::io::Write::flush(&mut file).await.unwrap();
        let file_length = nagoya::io::File::length(&mut file).await.unwrap();
        assert!(file_length > page_start_offset::<{ PAGE_SIZE as u32 }>(FIRST_PAGE_PAST_4_GIB));

        let pages = parse_data_pages_batch::<
            { PAGE_SIZE as u32 },
            INNER_PAGE_SIZE,
            { PAGE_SIZE as u32 },
        >(&mut file, vec![1, FIRST_PAGE_PAST_4_GIB])
        .await
        .unwrap();

        assert_eq!(pages.len(), 2);
        assert_eq!(pages[0].header.page_id, 1.into());
        assert_eq!(&pages[0].inner.data[..FIRST_MARKER.len()], FIRST_MARKER);
        assert_eq!(pages[1].header.page_id, FIRST_PAGE_PAST_4_GIB.into());
        assert_eq!(
            &pages[1].inner.data[..BOUNDARY_MARKER.len()],
            BOUNDARY_MARKER
        );

        drop(file);
        std::fs::remove_file(&path).unwrap();
    }

    /// Half the default, so a page written at this stride lands where the
    /// default would put the *middle* of a page. A stride that were quietly
    /// ignored could not pass this.
    const HALF: usize = PAGE_SIZE / 2;
    const HALF_INNER: usize = HALF - crate::GENERAL_HEADER_SIZE;

    fn page_at<const INNER: usize>(id: u32, marker: &[u8]) -> GeneralPage<DataPage<INNER>> {
        let mut data = [0u8; INNER];
        data[..marker.len()].copy_from_slice(marker);
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
                length: marker.len() as u32,
                data,
            },
        }
    }

    #[tokio::test]
    async fn a_non_default_stride_is_written_and_read_back_at_that_stride() {
        let path =
            std::env::temp_dir().join(format!("data_bucket_stride_{}.wt", std::process::id()));
        let mut file = nagoya::io::create(&path).await.unwrap();

        let pages = vec![
            page_at::<HALF_INNER>(0, b"zero"),
            page_at::<HALF_INNER>(1, b"one"),
            page_at::<HALF_INNER>(2, b"two"),
        ];
        persist_pages_batch::<_, { HALF as u32 }>(pages, &mut file)
            .await
            .unwrap();
        // async-fs buffers in user space and flushes on drop, best effort and
        // silently. Asking the filesystem how long the file is before this
        // returns measures the buffer, not the write.
        nagoya::io::Write::flush(&mut file).await.unwrap();

        // The last page starts at two strides in, so the file ends somewhere in
        // the third. Not `3 * HALF`: unlike `persist_page`, a batch does not
        // seek to the end of its final page, so the file stops after the last
        // body rather than at a page boundary.
        //
        // This is the assertion the whole change exists for. At the default
        // stride the same three pages would end past `2 * PAGE_SIZE`, which is
        // four times further out and cannot be confused with this.
        let written = std::fs::metadata(&path).unwrap().len();
        assert!(
            written > 2 * HALF as u64 && written <= 3 * HALF as u64,
            "three pages at a {HALF}-byte stride should end inside the third, got {written}"
        );

        let read = parse_data_pages_batch::<{ HALF as u32 }, HALF_INNER, { HALF as u32 }>(
            &mut file,
            vec![0, 1, 2],
        )
        .await
        .unwrap();
        assert_eq!(read.len(), 3);
        assert_eq!(&read[0].inner.data[..4], b"zero");
        assert_eq!(&read[1].inner.data[..3], b"one");
        assert_eq!(&read[2].inner.data[..3], b"two");
        assert_eq!(read[2].header.page_id, 2.into());

        // Read at the default stride and page 1 is not where it was left. A
        // seek that ignored its parameter would pass the round trip above and
        // fail here, so this is what proves the parameter is load-bearing.
        let wrong = parse_data_pages_batch::<
            { PAGE_SIZE as u32 },
            INNER_PAGE_SIZE,
            { PAGE_SIZE as u32 },
        >(&mut file, vec![1])
        .await;
        assert!(
            wrong.is_err() || wrong.unwrap()[0].header.page_id != 1.into(),
            "reading at the wrong stride must not find page 1"
        );

        let _ = std::fs::remove_file(&path);
    }
}

// #[cfg(test)]
// pub mod test {
//     use std::collections::HashMap;
//     use std::fs::remove_file;
//     use std::path::Path;
//
//     use rkyv::{Archive, Deserialize, Serialize};
//
//     use crate::page::index::IndexValue;
//     use crate::persistence::data::DataTypeValue;
//     use crate::{read_data_pages, GeneralHeader, GeneralPage, IndexData, Interval, Link, PageType, SpaceInfoData, DATA_VERSION, PAGE_SIZE};
//
//     use super::persist_page;
//
//     fn create_space_with_intervals(intervals: &Vec<Interval>) -> GeneralPage<SpaceInfoData> {
//         let space_info_header = GeneralHeader {
//             data_version: DATA_VERSION,
//             space_id: 1.into(),
//             page_id: 0.into(),
//             previous_id: 0.into(),
//             next_id: 1.into(),
//             page_type: PageType::SpaceInfo,
//             data_length: 0u32,
//         };
//         let space_info = SpaceInfoData {
//             id: 0.into(),
//             page_count: 0,
//             name: "Test".to_string(),
//             row_schema: vec![],
//             primary_key_fields: vec![],
//             primary_key_length: 1,
//             secondary_index_lengths: HashMap::from([(
//                 "string_index".to_owned(),
//                 1,
//             )]),
//             data_length: 1,
//             pk_gen_state: (),
//             empty_links_list: vec![],
//             secondary_index_types: vec![("string_index".to_string(), "String".to_string())],
//         };
//         let space_info_page = GeneralPage {
//             header: space_info_header,
//             inner: space_info,
//         };
//
//         space_info_page
//     }
//
//     fn create_index_pages(intervals: &Vec<Interval>) -> Vec<GeneralPage<IndexData<String>>> {
//         let mut index_pages = Vec::<GeneralPage<IndexData<String>>>::new();
//
//         for interval in intervals {
//             for index in interval.0..=interval.1 {
//                 let index_header = GeneralHeader {
//                     data_version: DATA_VERSION,
//                     space_id: 1.into(),
//                     page_id: (index as u32).into(),
//                     previous_id: (if index > 0 { index as u32 - 1 } else { 0 }).into(),
//                     next_id: (index as u32 + 1).into(),
//                     page_type: PageType::SpaceInfo,
//                     data_length: 0u32,
//                 };
//                 let index_data = IndexData {
//                     index_values: vec![IndexValue {
//                         key: "first_value".to_string(),
//                         link: Link {
//                             page_id: 2.into(),
//                             length: 0,
//                             offset: 0,
//                         },
//                     }],
//                 };
//                 let index_page = GeneralPage {
//                     header: index_header,
//                     inner: index_data,
//                 };
//                 index_pages.push(index_page);
//             }
//         }
//
//         index_pages
//     }
//
//     #[test]
//     fn test_read_index_pages() {
//         let filename = "tests/data/table.wt";
//         if Path::new(filename).exists() {
//             remove_file(filename).unwrap();
//         }
//         let mut file: std::fs::File = std::fs::File::create(filename).unwrap();
//
//         let intervals = vec![Interval(1, 2), Interval(5, 7)];
//
//         // create the space page
//         let mut space_info_page = create_space_with_intervals(&intervals);
//         persist_page(&mut space_info_page, &mut file).unwrap();
//
//         // create the index pages
//         for mut index_page in create_index_pages(&intervals) {
//             persist_page(&mut index_page, &mut file).unwrap();
//         }
//
//         // read the data
//         let mut file = std::fs::File::open(filename).unwrap();
//         let index_pages = read_secondary_index_pages::<String, PAGE_SIZE>(
//             &mut file,
//             "string_index",
//             vec![Interval(1, 2), Interval(5, 6)],
//         )
//         .unwrap();
//         assert_eq!(index_pages.len(), 4);
//         assert_eq!(index_pages[0].key, "first_value");
//         assert_eq!(index_pages[0].link.page_id, 2.into());
//         assert_eq!(index_pages[0].link.offset, 0);
//         assert_eq!(index_pages[0].link.length, 0);
//     }
//
//     #[derive(Archive, Debug, Deserialize, Serialize)]
//     struct TableStruct {
//         int1: i32,
//         string1: String,
//     }
//
//     pub fn create_test_database_file(filename: &str) {
//         if Path::new(filename).exists() {
//             remove_file(filename).unwrap();
//         }
//         let mut file: std::fs::File = std::fs::File::create(filename).unwrap();
//
//         let space_info_header = GeneralHeader {
//             data_version: DATA_VERSION,
//             space_id: 1.into(),
//             page_id: 0.into(),
//             previous_id: 0.into(),
//             next_id: 1.into(),
//             page_type: PageType::SpaceInfo,
//             data_length: 0u32,
//         };
//         let space_info = SpaceInfoData {
//             id: 1.into(),
//             page_count: 4,
//             name: "test space".to_owned(),
//             row_schema: vec![
//                 ("int1".to_string(), "i32".to_string()),
//                 ("string1".to_string(), "String".to_string()),
//             ],
//             primary_key_fields: vec!["int1".to_string()],
//             primary_key_intervals: vec![Interval(1, 1)],
//             secondary_index_types: vec![],
//             secondary_index_intervals: Default::default(),
//             data_intervals: vec![],
//             pk_gen_state: (),
//             empty_links_list: vec![],
//         };
//         let mut space_info_page = GeneralPage {
//             header: space_info_header,
//             inner: space_info,
//         };
//         persist_page(&mut space_info_page, &mut file).unwrap();
//
//         let index_header = GeneralHeader {
//             data_version: DATA_VERSION,
//             space_id: 1.into(),
//             page_id: 1.into(),
//             previous_id: 0.into(),
//             next_id: 2.into(),
//             page_type: PageType::Index,
//             data_length: 0,
//         };
//
//         let data_header = GeneralHeader {
//             data_version: DATA_VERSION,
//             space_id: 1.into(),
//             page_id: 2.into(),
//             previous_id: 2.into(),
//             next_id: 4.into(),
//             page_type: PageType::Data,
//             data_length: 0,
//         };
//
//         let data_row1 = TableStruct {
//             int1: 1,
//             string1: "first string".to_string(),
//         };
//
//         let data_row2 = TableStruct {
//             int1: 2,
//             string1: "second string".to_string(),
//         };
//
//         let data_row1_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data_row1).unwrap();
//         let data_row1_offset = 0;
//         let data_row1_length = data_row1_inner.len();
//
//         let data_row2_inner = rkyv::to_bytes::<rkyv::rancor::Error>(&data_row2).unwrap();
//         let data_row2_offset = data_row1_offset + data_row1_length;
//         let data_row2_length = data_row2_inner.len();
//
//         let data_rows12_buffer = [data_row1_inner, data_row2_inner].concat();
//
//         let mut data_page = GeneralPage::<Vec<u8>> {
//             header: data_header,
//             inner: data_rows12_buffer,
//         };
//
//         let index_data: IndexData<i32> = IndexData::<i32> {
//             index_values: vec![
//                 IndexValue::<i32> {
//                     key: 1,
//                     link: Link {
//                         page_id: data_header.page_id,
//                         offset: data_row1_offset as u32,
//                         length: data_row1_length as u32,
//                     },
//                 },
//                 IndexValue::<i32> {
//                     key: 2,
//                     link: Link {
//                         page_id: data_header.page_id,
//                         offset: data_row2_offset as u32,
//                         length: data_row2_length as u32,
//                     },
//                 },
//             ],
//         };
//         let mut index_page = GeneralPage {
//             header: index_header,
//             inner: index_data,
//         };
//
//         persist_page(&mut index_page, &mut file).unwrap();
//         persist_page(&mut data_page, &mut file).unwrap();
//     }
//
//     #[test]
//     fn test_read_table_data() {
//         let filename = "tests/data/table_with_rows.wt";
//         create_test_database_file(filename);
//
//         let mut file: std::fs::File = std::fs::File::open(filename).unwrap();
//         let data_pages: Vec<Vec<DataTypeValue>> = read_data_pages::<{ PAGE_SIZE as u32 }>(&mut file).unwrap();
//         assert_eq!(data_pages[0][0], DataTypeValue::I32(1));
//         assert_eq!(
//             data_pages[0][1],
//             DataTypeValue::String("first string".to_string())
//         );
//         assert_eq!(data_pages[1][0], DataTypeValue::I32(2));
//         assert_eq!(
//             data_pages[1][1],
//             DataTypeValue::String("second string".to_string())
//         );
//     }
// }
