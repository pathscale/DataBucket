use proc_macro2::Ident;
use syn::ItemStruct;

mod size_measurable;
mod space_deserialize;
mod space_serialize;

pub struct Generator {
    pub struct_def: ItemStruct,
    pub pk_ident: Ident,
}
