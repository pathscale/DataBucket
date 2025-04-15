use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

pub struct Generator {
    pub struct_def: ItemStruct,
}

impl Generator {
    pub fn gen_impl(&self) -> TokenStream {
        let struct_ident = &self.struct_def.ident;

        let sum = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let ty = &f.ty;
                quote! {
                    <#ty as VariableSizeMeasurable>::aligned_size(length)
                }
            })
            .collect::<Vec<_>>();

        quote! {
            impl VariableSizeMeasurable for #struct_ident {
                fn aligned_size(length: usize) -> usize {
                    align(#(#sum)+*)
                }
            }
        }
    }
}
