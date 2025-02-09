extern crate core;

pub mod link;
pub mod page;
pub mod persistence;
pub mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::SizeMeasure;
pub use page::{
    map_data_pages_to_general, map_index_pages_to_general,
    parse_data_page, parse_index_page, parse_page, persist_page,
    seek_by_link, seek_to_page_start, update_at, DataPage,
    GeneralPage, GeneralHeader, IndexValue, Interval, PageType,
    SpaceInfo, DATA_VERSION, GENERAL_HEADER_SIZE, INNER_PAGE_SIZE, PAGE_SIZE,
    TableOfContentsPage, IndexPage, get_index_page_size_from_data_length
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use util::{align, Persistable, SizeMeasurable, align8, align_vec};
pub use space::{Id as SpaceId};
