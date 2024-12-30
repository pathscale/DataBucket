use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

pub struct Generator {
    pub struct_def: ItemStruct,
}

impl Generator {
    pub fn gen_impl(&self) -> TokenStream {
        let struct_ident = &self.struct_def.ident;

        let mut num = -1;
        let sum = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                if let Some(i) = &f.ident {
                    quote! {
                        self.#i.aligned_size()
                    }
                } else {
                    num += 1;
                    quote! {
                        self.#num.aligned_size()
                    }
                }
            })
            .collect::<Vec<_>>();

        quote! {
            impl SizeMeasurable for #struct_ident {
                fn aligned_size(&self) -> usize {
                    let len = #(#sum+)* 0;
                    align(len)
                }
            }
        }
    }
}
