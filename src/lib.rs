pub mod link;
pub mod page;
pub mod persistence;
mod space;
pub mod util;

pub use link::Link;

pub use data_bucket_codegen::{PersistIndex, PersistTable, SizeMeasure};
pub use page::{
    map_index_pages_to_general, map_tree_index, map_unique_tree_index, General as GeneralPage,
    GeneralHeader, IndexPage as IndexData, PageType, SpaceInfo as SpaceInfoData,
};
pub use persistence::{PersistableIndex, PersistableTable};
pub use util::{align, SizeMeasurable};
