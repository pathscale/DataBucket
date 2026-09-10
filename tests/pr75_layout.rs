use data_bucket::{
    DataPage, GeneralHeader, GeneralPage, IndexPage, IndexValue, Link, PageType, Persistable,
};
use nagoya::io::{Error, Read, Seek, SeekFrom, Write};

struct NoIo;
impl Seek for NoIo {
    async fn seek(&mut self, _: SeekFrom) -> Result<u64, Error> {
        panic!("invalid layout reached seek")
    }
}
impl Write for NoIo {
    async fn write(&mut self, _: &[u8]) -> Result<usize, Error> {
        panic!("invalid layout reached write")
    }
    async fn flush(&mut self) -> Result<(), Error> {
        panic!("invalid layout reached flush")
    }
}

fn page() -> GeneralPage<DataPage<32>> {
    GeneralPage {
        header: GeneralHeader::new(1.into(), PageType::Data, 0.into()),
        inner: DataPage {
            data: [0; 32],
            length: 1,
        },
    }
}

#[test]
fn a_stride_smaller_than_the_header_is_rejected_before_io() {
    nagoya::block_on(async {
        assert!(data_bucket::persist_page::<_, 16>(&mut page(), &mut NoIo)
            .await
            .is_err());
        assert!(
            data_bucket::persist_pages_batch::<_, 16>(vec![page()], &mut NoIo)
                .await
                .is_err()
        );
    });
}

#[test]
fn an_inconsistent_update_layout_is_rejected_before_io() {
    let link = Link {
        page_id: 1.into(),
        offset: 5000,
        length: 1,
    };
    assert!(
        nagoya::block_on(data_bucket::update_at::<16356, 4096>(&mut NoIo, link, &[1])).is_err()
    );
}

#[test]
fn index_slot_capacity_remains_representable_on_large_pages() {
    let size = data_bucket::get_index_page_size_from_data_length::<u64>(4 * 1024 * 1024);
    assert_eq!(size, usize::from(u16::MAX));
    let page = IndexPage::new(IndexValue::<u64>::default(), size);
    assert_eq!(usize::from(page.size), page.slots.len());
    assert_eq!(usize::from(page.size), page.index_values.len());
}

struct ReadOnly {
    bytes: Vec<u8>,
    position: usize,
}
impl Read for ReadOnly {
    async fn read(&mut self, out: &mut [u8]) -> Result<usize, Error> {
        let n = out
            .len()
            .min(self.bytes.len().saturating_sub(self.position));
        out[..n].copy_from_slice(&self.bytes[self.position..self.position + n]);
        self.position += n;
        Ok(n)
    }
}
impl Seek for ReadOnly {
    async fn seek(&mut self, from: SeekFrom) -> Result<u64, Error> {
        match from {
            SeekFrom::Start(position) => self.position = position as usize,
            _ => panic!("unexpected seek"),
        }
        Ok(self.position as u64)
    }
}

#[test]
fn a_decoder_accepts_read_seek_without_write_or_durability() {
    let header = GeneralHeader::new(0.into(), PageType::Data, 0.into());
    let mut file = ReadOnly {
        bytes: header.as_bytes().as_ref().to_vec(),
        position: 0,
    };
    let decoded = nagoya::block_on(data_bucket::parse_general_header_by_index::<128>(
        &mut file, 0,
    ))
    .unwrap();
    assert_eq!(decoded.page_id, header.page_id);
}
