// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, FnArg, Ident, ItemFn, Token, Type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro_attribute]
pub fn transactional_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    const CATALOG_ARGUMENT: &str = "Extension(catalog): Extension<Catalog>";

    let input = parse_macro_input!(item as ItemFn);

    let catalog_argument: FnArg = syn::parse_str(CATALOG_ARGUMENT).unwrap();
    let has_catalog_argument = input
        .sig
        .inputs
        .iter()
        .any(|argument| catalog_argument == *argument);

    if has_catalog_argument {
        let function_signature = &input.sig;
        let function_body = &input.block;
        let function_visibility = &input.vis;

        let expanded = quote! {
            #function_visibility #function_signature {
                ::database_common::DatabaseTransactionRunner::new(catalog)
                    .transactional(|catalog: ::dill::Catalog| async move {
                        #function_body
                    })
                    .await
            }
        };

        TokenStream::from(expanded)
    } else {
        let function_name = &input.sig.ident;

        panic!("{function_name}(): the expected argument \"{CATALOG_ARGUMENT}\" was not found!");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CatalogItem {
    item_name: Ident,
    item_type: Type,
}

impl Parse for CatalogItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_name: Ident = input.parse()?;
        let _colon: Token![:] = input.parse()?;
        let item_type: Type = input.parse()?;

        Ok(CatalogItem {
            item_name,
            item_type,
        })
    }
}

#[proc_macro_attribute]
pub fn transactional_method(attr: TokenStream, item: TokenStream) -> TokenStream {
    let CatalogItem {
        item_name: catalog_item_name,
        item_type: catalog_item_type,
    } = parse_macro_input!(attr as CatalogItem);
    let input = parse_macro_input!(item as ItemFn);

    let method_signature = &input.sig;
    let method_body = &input.block;
    let method_visibility = &input.vis;

    let expanded = quote! {
        #method_visibility #method_signature {
            ::database_common::DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional_with(|#catalog_item_name: #catalog_item_type| async move {
                    #method_body
                })
                .await
                .unwrap();
        }
    };

    TokenStream::from(expanded)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
