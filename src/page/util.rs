use crate::page::header::GeneralHeader;
use crate::page::ty::PageType;
use crate::page::General;
use crate::IndexData;

pub fn map_index_pages_to_general<T>(
    pages: Vec<IndexData<T>>,
    header: &mut GeneralHeader,
) -> Vec<General<IndexData<T>>> {
    let mut previous_header = header;
    let mut general_pages = vec![];

    for p in pages {
        let general = General {
            header: previous_header.follow_with(PageType::Index),
            inner: p,
        };

        general_pages.push(general);
        previous_header = &mut general_pages.last_mut().unwrap().header;
    }

    general_pages
}
