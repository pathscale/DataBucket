extern crate core;

pub mod link;
pub mod page;
pub mod persistence;
pub mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::{SizeMeasure, VariableSizeMeasure};
pub use page::{
    get_index_page_size_from_data_length, map_data_pages_to_general, parse_data_page,
    parse_data_pages_batch, parse_general_header_by_index, parse_page, parse_pages_batch,
    persist_page, persist_pages_batch, seek_by_link, seek_to_page_start, update_at, DataPage,
    GeneralHeader, GeneralPage, IndexPage, IndexPageUtility, IndexValue, Interval, PageType,
    SpaceInfoPage, TableOfContentsPage, UnsizedIndexPage, UnsizedIndexPageUtility, DATA_VERSION,
    GENERAL_HEADER_SIZE, INNER_PAGE_SIZE, PAGE_SIZE,
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use space::Id as SpaceId;
pub use util::{align, align8, align_vec, Persistable, SizeMeasurable, VariableSizeMeasurable};
