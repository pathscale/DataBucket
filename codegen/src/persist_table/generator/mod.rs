use proc_macro2::Ident;
use syn::ItemStruct;

mod space;
mod size_measurable;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub index_ident: Ident,
}
