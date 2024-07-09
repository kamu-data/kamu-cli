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
use syn::{
    parse_macro_input,
    FnArg,
    GenericArgument,
    Ident,
    ItemFn,
    LitStr,
    Pat,
    PatTupleStruct,
    PathArguments,
    Token,
    Type,
    TypePath,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[transactional_handler]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[proc_macro_attribute]
pub fn transactional_handler(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as ItemFn);

    let has_catalog_argument = input.sig.inputs.iter().any(is_catalog_argument);

    if !has_catalog_argument {
        let function_name = &input.sig.ident;

        const CATALOG_ARGUMENT: &str = "Extension(catalog): Extension<Catalog>";

        panic!("{function_name}(): the expected argument \"{CATALOG_ARGUMENT}\" was not found!");
    }

    let function_signature = &input.sig;
    let function_body = &input.block;
    let function_visibility = &input.vis;

    let updated_function = quote! {
        #function_visibility #function_signature {
            ::database_common::DatabaseTransactionRunner::new(catalog)
                .transactional(|catalog: ::dill::Catalog| async move {
                    #function_body
                })
                .await
        }
    };

    TokenStream::from(updated_function)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn is_catalog_argument(argument: &FnArg) -> bool {
    let FnArg::Typed(typed_pattern) = argument else {
        return false;
    };

    let Pat::TupleStruct(tuple_struct_pattern) = typed_pattern.pat.as_ref() else {
        return false;
    };

    if !is_catalog_tuple_struct(tuple_struct_pattern) {
        return false;
    }

    let Type::Path(ref type_path) = *typed_pattern.ty else {
        return false;
    };

    is_catalog_type(type_path)
}

#[test]
fn test_is_catalog_argument_works_correctly() {
    let mut inputs = vec![];

    for tuple_type in ["Extension", "axum::Extension", "::axum::Extension"] {
        for argument_type in ["Extension", "axum::Extension", "::axum::Extension"] {
            for generic_type in ["Catalog", "dill::Catalog", "::dill::Catalog"] {
                let input: FnArg = syn::parse_str(&format!(
                    "{tuple_type}(catalog): {argument_type}<{generic_type}>"
                ))
                .unwrap();

                inputs.push(input);
            }
        }
    }

    for input in inputs {
        assert!(is_catalog_argument(&input));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
//   ^^^^^^^^^^^^^^^^^^^^^^^^
fn is_catalog_tuple_struct(tuple_struct_pattern: &PatTupleStruct) -> bool {
    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //         ^^^^^^^^^
    let Some(last_type_segment) = tuple_struct_pattern.path.segments.last() else {
        return false;
    };

    if last_type_segment.ident != "Extension" {
        return false;
    }

    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                   ^^^^^^^
    let Some(first_tuple_element) = tuple_struct_pattern.elems.first() else {
        return false;
    };

    let Pat::Ident(tuple_ident) = first_tuple_element else {
        return false;
    };

    tuple_ident.ident == "catalog"
}

// ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
//                               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
fn is_catalog_type(type_path: &TypePath) -> bool {
    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                                     ^^^^^^^^^
    let Some(last_type_segment) = type_path.path.segments.last() else {
        return false;
    };

    if last_type_segment.ident != "Extension" {
        return false;
    }

    // ::axum::Extension(catalog): ::axum::Extension<::dill::Catalog>,
    //                                                       ^^^^^^^
    let PathArguments::AngleBracketed(ref generic_arguments) = last_type_segment.arguments else {
        return false;
    };

    let Some(last_generic_argument) = generic_arguments.args.last() else {
        return false;
    };

    let GenericArgument::Type(type_generic_argument) = last_generic_argument else {
        return false;
    };

    let Type::Path(generic_type_path) = type_generic_argument else {
        return false;
    };

    let Some(last_type_segment) = generic_type_path.path.segments.last() else {
        return false;
    };

    last_type_segment.ident == "Catalog"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// #[transactional_method]
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum ReturnValueAction {
    Unwrap,
    UnwrapAndColon,
    ReturnAsIs,
}

struct CatalogItem {
    item_name: Ident,
    item_type: Type,
    return_value_action: ReturnValueAction,
}

impl CatalogItem {
    fn parse_return_value_action(input: ParseStream) -> syn::Result<ReturnValueAction> {
        if !input.peek(Token![,]) {
            return Ok(ReturnValueAction::UnwrapAndColon);
        }

        input.parse::<Token![,]>()?;

        let return_value_parameter_name: Ident = input.parse()?;

        assert_eq!(
            return_value_parameter_name, "return_value",
            r#"Unexpected parameter: only "return_value" is available"#
        );

        input.parse::<Token![=]>()?;

        let return_value: LitStr = input.parse()?;

        let result = match return_value.value().as_str() {
            "unwrap" => ReturnValueAction::Unwrap,
            "unwrapAndColon" => ReturnValueAction::UnwrapAndColon,
            "asIs" => ReturnValueAction::ReturnAsIs,
            s => panic!(
                "Unexpected \"return_value\" = \"{s}\"! Only \"unwrap\", \"unwrapAndColon\" and \
                 \"asIs\" are available"
            ),
        };

        Ok(result)
    }
}

impl Parse for CatalogItem {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let item_name: Ident = input.parse()?;

        input.parse::<Token![:]>()?;

        let item_type: Type = input.parse()?;
        let return_value_action = Self::parse_return_value_action(input)?;

        Ok(CatalogItem {
            item_name,
            item_type,
            return_value_action,
        })
    }
}

#[proc_macro_attribute]
/// Encrusting the method with a transactional Catalog.
/// The structure must contain the Catalog as a field
///
/// # Examples
/// ```
/// // `service` request from a transactional Catalog
/// #[transactional_method(service: Arc<dyn Service>)]
/// async fn set_system_flow_schedule(&self) {
///     // `service` is available inside the method body
/// }
///
/// // Behavior change when dealing with the result
/// // Available values
/// // - "unwrap"
/// // - "unwrapAndColon" (default)
/// // - "asIs"
/// #[transactional_method(service: Arc<dyn Service>, return_value = "asIs")]
/// async fn set_system_flow_schedule(&self) -> Result<(), InternalError>{
///     // `service` is available inside the method body
///     service.something().await
/// }
/// ```
pub fn transactional_method(attr: TokenStream, item: TokenStream) -> TokenStream {
    let CatalogItem {
        item_name: catalog_item_name,
        item_type: catalog_item_type,
        return_value_action,
    } = parse_macro_input!(attr as CatalogItem);
    let input = parse_macro_input!(item as ItemFn);

    let method_signature = &input.sig;
    let method_body = &input.block;
    let method_visibility = &input.vis;

    let return_value_action = match return_value_action {
        ReturnValueAction::Unwrap => quote! { .unwrap() },
        ReturnValueAction::UnwrapAndColon => quote! { .unwrap(); },
        ReturnValueAction::ReturnAsIs => quote! {},
    };
    let updated_method = quote! {
        #method_visibility #method_signature {
            ::database_common::DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional_with(|#catalog_item_name: #catalog_item_type| async move {
                    #method_body
                })
                .await
                #return_value_action
        }
    };

    TokenStream::from(updated_method)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
