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
use syn::{parse_macro_input, Ident, ImplItem, LitStr, Token, Type};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[method_names_consts]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Macro helper, which is convenient to use in combination with
/// `#[tracing::instrument()]` macro.
///
/// `#[tracing::instrument()]` will log only the method name, without the
/// structure name. Using macros together, we can get the name of the structure
/// as well.
///
/// The macro generates constants that, for ease of copying, use the following
/// naming scheme: `{struct_name}_{method_name}`. Thus, the following constant
/// will be generated for the `KamuAuthOso::load_oso()` method:
/// ```
/// #[allow(non_upper_case_globals)]
/// const KamuAuthOso_load_oso: &str = "KamuAuthOso::load_oso";
/// ```
///
/// # Examples
/// ```compile_fail
/// /* 1. Only #[tracing::instrument()]` */
/// #[component(pub)]
/// impl KamuAuthOso {
///     // Logged as: "load_oso"
///     #[tracing::instrument(level = "debug", skip_all)]
///     pub fn load_oso() -> Result<Oso, OsoError> { /* ... */ }
/// }
/// ```
///
/// ```compile_fail
/// /* 2. Macro combination */
/// #[method_names_consts]
/// #[component(pub)]
/// impl KamuAuthOso {
///     // Logged as: "KamuAuthOso::load_oso"
///     #[tracing::instrument(level = "debug", name = KamuAuthOso_load_oso, skip_all)]
///     pub fn load_oso() -> Result<Oso, OsoError> { /* ... */ }
/// }
/// ```
///
/// ```compile_fail
/// /* 3. Macro combination with options */
/// #[method_names_consts(const_value_prefix = "GQL: ")]
/// #[Object]
/// impl Search {
///     // Logged as: "GQL: Search::query"
///     #[tracing::instrument(level = "info", name = Search_query, skip_all)]
///     async fn query(/* ... */) -> Result<SearchResultConnection> {
///         /* ... */
///     }
/// }
#[proc_macro_attribute]
pub fn method_names_consts(attr: TokenStream, item: TokenStream) -> TokenStream {
    let MethodNamesConstsOptions { const_value_prefix } =
        parse_macro_input!(attr as MethodNamesConstsOptions);
    let input = parse_macro_input!(item as syn::ItemImpl);

    let struct_name = if let Type::Path(ref type_path) = *input.self_ty {
        type_path.path.segments.last().unwrap().ident.to_string()
    } else {
        panic!(r#"[method_names_consts] сan be applied only to `impl <Struct> {{}}` block"#);
    };

    let method_names: Vec<_> = input
        .items
        .iter()
        .filter_map(|item| {
            if let ImplItem::Fn(method) = item {
                Some(method.sig.ident.to_string())
            } else {
                None
            }
        })
        .collect();

    assert!(
        !method_names.is_empty(),
        r#"[method_names_consts]: `{struct_name}` struct doesn't contain methods"#
    );

    let name_consts = method_names
        .iter()
        .map(|method_name| {
            let const_name_ident = quote::format_ident!("{struct_name}_{method_name}",);
            let const_value = {
                let mut value = String::new();
                if let Some(const_value_prefix) = &const_value_prefix {
                    value += &const_value_prefix.value();
                };
                value += &format!("{struct_name}::{method_name}");
                value
            };

            println!("!!! {struct_name}: {const_value}");

            quote! {
                #[allow(dead_code)]
                #[allow(non_upper_case_globals)]
                const #const_name_ident: &str = #const_value;
            }
        })
        .collect::<Vec<_>>();

    let expanded = quote! {
        #input

        #(#name_consts)*
    };

    TokenStream::from(expanded)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct MethodNamesConstsOptions {
    const_value_prefix: Option<LitStr>,
}

impl Parse for MethodNamesConstsOptions {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut options = Self::default();

        while !input.is_empty() {
            let key: Ident = input.parse()?;

            input.parse::<Token![=]>()?;

            match key.to_string().as_str() {
                "const_value_prefix" => {
                    let value: LitStr = input.parse()?;
                    options.const_value_prefix = Some(value);
                }
                unexpected_key => panic!(
                    "Unexpected key: {unexpected_key}\nAllowable values: \"const_value_prefix\"."
                ),
            };

            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            }
        }

        Ok(options)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
