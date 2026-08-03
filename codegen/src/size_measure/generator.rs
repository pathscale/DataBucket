use proc_macro2::TokenStream;
use quote::quote;
use syn::ItemStruct;

pub struct Generator {
    pub struct_def: ItemStruct,
}

impl Generator {
    pub fn gen_impl(&self) -> TokenStream {
        let struct_ident = &self.struct_def.ident;

        let mut num = 0;
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
                    let i = syn::Index::from(num);
                    num += 1;
                    quote! {
                        self.#i.aligned_size()
                    }
                }
            })
            .collect::<Vec<_>>();

        let align_check = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                let t = &f.ty;
                quote! {
                    if <#t as SizeMeasurable>::align() == Some(8) {
                        return Some(8)
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
                fn align() -> Option<usize> {
                    #(#align_check)*
                    None
                }
            }
        }
    }
}
