use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{quote, ToTokens};
use syn::ItemStruct;

pub struct Generator {
    pub struct_def: ItemStruct,
}

impl Generator {
    pub fn gen_persist_type(&self) -> syn::Result<TokenStream> {
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );
        let mut fields = vec![];
        let mut types = vec![];

        for field in &self.struct_def.fields {
            fields.push(field.ident.clone().unwrap());
            let index_type = field.ty.to_token_stream().to_string();
            let mut split = index_type.split("<");
            // skip `TreeIndex`
            split.next();
            let substr = split.next().unwrap().to_string();
            types.push(substr.split(",").next().unwrap().to_string());
        }

        let fields: Vec<_> = fields
            .into_iter()
            .zip(types)
            .map(|(i, t)| {
                let t: TokenStream = t.parse().unwrap();
                quote! {
                    #i: Vec<IndexPage<#t>>,
                }
            })
            .collect();

        Ok(quote! {
            #[derive(Debug, Default, Clone)]
            pub struct #name_ident {
                #(#fields)*
            }
        })
    }

    pub fn gen_persist_impl(&self) -> syn::Result<TokenStream> {
        let ident = &self.struct_def.ident;
        let name_ident = Ident::new(
            format!("{}Persisted", self.struct_def.ident).as_str(),
            Span::mixed_site(),
        );

        let field_names_lits: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| Literal::string(f.ident.as_ref().unwrap().to_string().as_str()))
            .map(|l| quote! { #l, })
            .collect();
        let field_names_match: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (Literal::string(f.ident.as_ref().unwrap().to_string().as_str()), f.ident.as_ref().unwrap(), !f.ty.to_token_stream().to_string().contains("lockfree")))
            .map(|(l, i, is_unique)| {
                let index_call = if is_unique {
                    quote! {
                        map_unique_tree_index(&self.#i)
                    }
                } else {
                    quote! {
                        map_tree_index(&self.#i)
                    }
                };
                quote! {
                    #l => {
                        #index_call
                    },
                }
            })
            .collect();

        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| (Literal::string(f.ident.as_ref().unwrap().to_string().as_str()), f.ident.as_ref().unwrap()))
            .map(|(l, i)| {
                quote! {
                    #i: self.get_pages_by_name(#l),
                }
            })
            .collect();

        Ok(quote! {
            impl PersistIndex for #ident {
                type PersistedIndex = #name_ident;

                fn get_index_names(&self) -> Vec<&str> {
                    vec![#(#field_names_lits)*]
                }

                fn get_pages_by_name<T>(&self, name: &str) -> Vec<IndexPage<T>> {
                    match name {
                        #(#field_names_match)*
                    }
                }

                fn get_persisted_index(&self) -> Self::PersistedIndex {
                    Self::PersistedIndex {
                        #(#field_names_init)*
                    }
                }
            }
        })
    }
}
