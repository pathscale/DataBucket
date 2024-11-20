pub mod link;
pub mod page;
pub mod persistence;
mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::{PersistIndex, PersistTable, SizeMeasure};
pub use page::{
    map_index_pages_to_general, map_tree_index, map_unique_tree_index, persist_page,
    General as GeneralPage, GeneralHeader, IndexPage as IndexData, Interval, PageType,
    SpaceInfo as SpaceInfoData, PAGE_SIZE, Data as DataPage, INNER_PAGE_LENGTH, map_data_pages_to_general, HEADER_LENGTH
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use util::{align, Persistable, SizeMeasurable};
