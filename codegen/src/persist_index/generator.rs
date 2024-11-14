use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::{quote, ToTokens};
use syn::ItemStruct;

use std::collections::HashMap;

pub struct Generator {
    struct_def: ItemStruct,
    field_types: HashMap<Ident, TokenStream>,
}

impl Generator {
    pub fn new(struct_def: ItemStruct) -> Self {
        Self {
            struct_def,
            field_types: HashMap::new(),
        }
    }

    pub fn gen_persist_type(&mut self) -> syn::Result<TokenStream> {
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
                self.field_types.insert(i.clone(), t.clone());
                quote! {
                    #i: Vec<GeneralPage<IndexData<#t>>>,
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
        let name = self.struct_def.ident.to_string().replace("Index", "");
        let const_name = Ident::new(
            format!("{}_PAGE_SIZE", name.to_uppercase()).as_str(),
            Span::mixed_site(),
        );

        let field_names_lits: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| Literal::string(f.ident.as_ref().unwrap().to_string().as_str()))
            .map(|l| quote! { #l, })
            .collect();
        let idents = self
            .struct_def
            .fields
            .iter()
            .map(|f| f.ident.as_ref().unwrap())
            .collect::<Vec<_>>();
        let field_names_init: Vec<_> = self
            .struct_def
            .fields
            .iter()
            .map(|f| {
                (
                    Literal::string(f.ident.as_ref().unwrap().to_string().as_str()),
                    f.ident.as_ref().unwrap(),
                    !f.ty
                        .to_token_stream()
                        .to_string()
                        .to_lowercase()
                        .contains("lockfree"),
                )
            })
            .map(|(l, i, is_unique)| {
                let ty = self.field_types.get(&i).unwrap();
                if is_unique {
                    quote! {
                        let mut #i = map_index_pages_to_general(map_unique_tree_index::<#ty, #const_name>(&self.#i), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                } else {
                    quote! {
                        let mut #i =  map_index_pages_to_general(map_tree_index::<#ty, #const_name>(&self.#i), previous_header);
                        previous_header = &mut #i.last_mut().unwrap().header;
                    }
                }
            })
            .collect();

        Ok(quote! {
            impl PersistableIndex for #ident {
                type PersistedIndex = #name_ident;

                fn get_index_names(&self) -> Vec<&str> {
                    vec![#(#field_names_lits)*]
                }

                fn get_persisted_index(&self, header: &mut GeneralHeader) -> Self::PersistedIndex {
                    let mut previous_header = header;

                    #(#field_names_init)*

                    Self::PersistedIndex {
                        #(#idents,)*
                    }
                }
            }
        })
    }
}
