use proc_macro2::Ident;
use syn::ItemStruct;

mod size_measurable;
mod space_serialize;
mod space_deserialize;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
    pub index_ident: Ident,
}
