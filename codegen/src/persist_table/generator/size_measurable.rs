use proc_macro2::TokenStream;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_size_measurable_impl(&self) -> syn::Result<TokenStream> {
        let pk_type = &self.pk_ident;

        Ok(quote! {
            impl SizeMeasurable for #pk_type {
                 fn aligned_size(&self) -> usize {
                    self.0.aligned_size()
                }
            }
        })
    }
}