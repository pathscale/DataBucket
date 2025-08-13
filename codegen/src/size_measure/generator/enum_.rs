use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemEnum;

pub struct Generator {
    pub enum_def: ItemEnum,
}
impl Generator {
    pub fn gen_impl(&self) -> TokenStream {
        let enum_ident = &self.enum_def.ident;

        quote! {
            impl SizeMeasurable for #enum_ident {
                fn aligned_size(&self) -> usize {
                    std::mem::size_of::<#enum_ident>()
                }
                fn align() -> Option<usize> {
                    None
                }
            }
        }
    }
}
