use proc_macro2::Ident;
use syn::ItemStruct;

mod space;

pub struct Generator {
    struct_def: ItemStruct,
    pk_ident: Ident,
    index_ident: Ident,
}