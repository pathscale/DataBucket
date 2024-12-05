pub mod link;
pub mod page;
pub mod persistence;
pub mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::SizeMeasure;
pub use page::{
    map_data_pages_to_general, map_index_pages_to_general, map_tree_index, map_unique_tree_index,
    parse_data_page, parse_index_page, parse_page, persist_page, read_index_pages,
    Data as DataPage, DataType, General as GeneralPage, GeneralHeader, IndexPage as IndexData,
    Interval, PageType, SpaceInfo as SpaceInfoData, DATA_VERSION, GENERAL_HEADER_SIZE,
    INNER_PAGE_SIZE, PAGE_SIZE,
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use util::{align, Persistable, SizeMeasurable};
