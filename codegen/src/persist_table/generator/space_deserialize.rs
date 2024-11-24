use proc_macro2::{Ident, Literal, TokenStream};
use quote::__private::Span;
use quote::quote;

use crate::persist_table::generator::Generator;

impl Generator {
    pub fn gen_space_deserialize_impls(&self) -> syn::Result<TokenStream> {
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let space_into_table = self.gen_space_into_table()?;
        let space_ident = Ident::new(format!("{}Space", name).as_str(), Span::mixed_site());

        println!("{}", space_into_table.to_string());

        Ok(quote! {
            impl #space_ident {
                #space_into_table
            }
        })
    }

    fn gen_space_into_table(&self) -> syn::Result<TokenStream> {
        let wt_ident = &self.struct_def.ident;
        let name = self.struct_def.ident.to_string().replace("WorkTable", "");
        let index_ident = Ident::new(format!("{}Index", name).as_str(), Span::mixed_site());

        Ok(quote! {
            pub fn into_worktable(self, db_manager: std::sync::Arc<DatabaseManager>) -> #wt_ident {
                let data = DataPages::from_data(self.data.into_iter().map(|p| std::sync::Arc::new(Data::from_data_page(p))).collect());
                let indexes = #index_ident::from_persisted(self.indexes);

                let pk_map = TreeIndex::new();
                for page in self.primary_index {
                    page.inner.append_to_unique_tree_index(&pk_map);
                }

                let table = WorkTable {
                    data,
                    pk_map,
                    indexes,
                    pk_gen: Default::default(),
                    lock_map: LockMap::new(),
                    table_name: "",
                };

                #wt_ident(
                    table,
                    db_manager
                )
            }
        })
    }
}
