use proc_macro2::TokenStream;
use quote::{quote, ToTokens};
use syn::parse::Parse;
use syn::spanned::Spanned;

use crate::persist_index::generator::Generator;

mod generator;
mod parser;

pub fn expand(input: TokenStream) -> syn::Result<TokenStream> {
    let input_fn = match syn::parse2::<syn::ItemStruct>(input.clone()) {
        Ok(data) => data,
        Err(err) => {
            return Err(syn::Error::new(input.span(), err.to_string()));
        }
    };
    let gen = Generator {
        struct_def: input_fn,
    };

    let type_def = gen.gen_persist_type()?;
    let impl_def = gen.gen_persist_impl()?;

    Ok(quote! {
        #type_def

        #impl_def
    })
}

#[cfg(test)]
mod tests {
    use quote::quote;
    use rkyv::{Archive, Deserialize, Serialize};
    use scc::TreeIndex;

    use crate::persist_index::expand;

    #[derive(
        Archive, Copy, Clone, Deserialize, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
    )]
    pub struct Link {
        pub page_id: u32,
        pub offset: u32,
        pub length: u32,
    }

    pub struct TestIndex {
        test_idx: TreeIndex<i64, Link>,
        exchange_idx: TreeIndex<String, std::sync::Arc<lockfree::set::Set<Link>>>,
    }

    #[test]
    fn test() {
        let input = quote! {
            #[derive(Debug, Default, Clone)]
            pub struct TestIndex {
                test_idx: TreeIndex<i64, Link>,
                exchnage_idx: TreeIndex<String, std::sync::Arc<LockFreeSet<Link>>>
            }
        };

        let res = expand(input).unwrap();
        println!("{:?}", res.to_string())
    }
}
