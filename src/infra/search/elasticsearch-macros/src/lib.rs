// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::spanned::Spanned;
use syn::{FnArg, ItemFn, Pat, PatIdent, ReturnType, parse_macro_input};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Attribute macro for ES integration tests.
///
/// Expected forms:
///   #[`kamu_search_elasticsearch::test`]
///   async fn `my_test`(ctx: `std::sync::Arc<EsTestContext>`) { ... }
///
///   #[`kamu_search_elasticsearch::test`]
///   async fn `my_test`(ctx: `std::sync::Arc<EsTestContext>`) -> Result<(), E>
/// { ... }
///
/// The macro expands into:
///   - An inner function `my_test__inner(ctx: Arc<EsTestContext>)` with the
///     original body
///   - An outer `#[tokio::test] async fn my_test()` wrapper that calls
///     `::kamu_search_elasticsearch::testing::es_test(...)`
///
/// Notes:
/// - This macro assumes the runtime harness is located at:
///   `::kamu_search_elasticsearch::testing::es_test`
/// - It assumes `es_test` returns a `Result<(), _>` and will panic on Err.
#[proc_macro_attribute]
pub fn test(_attr: TokenStream, item: TokenStream) -> TokenStream {
    const HARNESS_PATH: &str = "::kamu_search_elasticsearch::testing::es_test";

    let f = parse_macro_input!(item as ItemFn);

    // Basic validation
    if f.sig.asyncness.is_none() {
        return syn::Error::new(
            f.sig.fn_token.span(),
            "kamu ES test macro requires an async fn",
        )
        .to_compile_error()
        .into();
    }

    // Enforce exactly one argument: ctx: Arc<EsTestContext>
    // We keep validation intentionally light: we enforce single arg named `ctx` but
    // do not strictly pattern-match the type path. Your harness provides `ctx`
    // anyway.
    if f.sig.inputs.len() != 1 {
        return syn::Error::new(
            f.sig.inputs.span(),
            "expected exactly one argument: ctx: Arc<EsTestContext>",
        )
        .to_compile_error()
        .into();
    }

    // Extract the parameter pattern name to ensure it's a simple identifier, and
    // also to avoid surprises when we call the inner function.
    let ctx_ident = match f.sig.inputs.first().unwrap() {
        FnArg::Typed(pat_ty) => match pat_ty.pat.as_ref() {
            Pat::Ident(PatIdent { ident, .. }) => ident.clone(),
            other => {
                return syn::Error::new(
                    other.span(),
                    "expected parameter pattern to be an identifier, e.g. `ctx: \
                     Arc<EsTestContext>`",
                )
                .to_compile_error()
                .into();
            }
        },
        FnArg::Receiver(r) => {
            return syn::Error::new(
                r.span(),
                "method receivers are not supported; expected `async fn test(ctx: \
                 Arc<EsTestContext>)`",
            )
            .to_compile_error()
            .into();
        }
    };

    // Build names
    let outer_name = f.sig.ident.clone();
    let inner_name = format_ident!("{}_inner", outer_name);

    // Preserve most attrs on the inner function (doc, allow, etc.).
    // Strip our own attribute if it was written as `#[test]` from this macro crate.
    // In practice, it doesn't matter much, but it's cleaner.
    let mut inner_attrs = Vec::new();
    for a in &f.attrs {
        // Drop only attributes named "test" from this proc-macro invocation context.
        // Keep everything else.
        if a.path().is_ident("test") {
            continue;
        }
        inner_attrs.push(a.clone());
    }

    // Preserve signature parts
    let vis = f.vis.clone();
    let inputs = f.sig.inputs.clone();
    let generics = f.sig.generics.clone();
    let where_clause = f.sig.generics.where_clause.clone();
    let output = f.sig.output.clone();
    let block = f.block.clone();

    // Determine if original function returns Result (any non-unit explicit return
    // type), or unit (no return type).
    let returns_explicit_type = matches!(output, ReturnType::Type(_, _));

    // If the inner returns (), we wrap into Ok(()) so the harness always gets
    // Result<(), _>. If it returns Result<..>, pass through unchanged.
    let call_inner = if returns_explicit_type {
        quote! { #inner_name(#ctx_ident).await }
    } else {
        quote! {{
            #inner_name(#ctx_ident).await;
            Ok(())
        }}
    };

    // Parse HARNESS_PATH into tokens (simple, robust enough for a constant path)
    let harness_tokens: proc_macro2::TokenStream =
        HARNESS_PATH.parse().expect("valid harness path");

    // Expand
    let expanded = quote! {
        #(#inner_attrs)*
        #vis async fn #inner_name #generics ( #inputs ) #output #where_clause
        #block

        #[::tokio::test]
        async fn #outer_name() {
            let r: ::std::result::Result<(), internal_error::InternalError> =
                #harness_tokens(stringify!(#outer_name), |#ctx_ident| async move {
                    #call_inner
                }).await;

            if let Err(e) = r {
                panic!("{e:?}");
            }
        }
    };

    expanded.into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
