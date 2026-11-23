// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proc_macro2::{Span, TokenStream, TokenTree};
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream, Parser};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::visit::Visit;
use syn::visit_mut::{self, VisitMut};
use syn::{
    Attribute,
    Expr,
    ExprAsync,
    ExprBlock,
    ExprClosure,
    ExprReturn,
    Ident,
    Item,
    ItemFn,
    Macro,
    Pat,
    PatIdent,
    ReturnType,
    Token,
    Type,
    parse_quote,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The input of `transaction!([#[catalog(..)] #[when(..)] #[span(..)]] |bindings| [-> R] body)`
pub(crate) struct TransactionBlock {
    maybe_catalog: Option<Expr>,
    maybe_when: Option<Expr>,
    maybe_span: Option<Expr>,
    bindings: Vec<Binding>,
    maybe_result_type: Option<Type>,
    body: Expr,
}

pub(crate) enum Binding {
    /// `name`: the transaction catalog
    Catalog(Ident),
    /// `name: Type`: a component resolved from the transaction catalog
    Component(Ident, Type),
}

impl Parse for TransactionBlock {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let attributes = Attribute::parse_outer(input)?;
        let closure: ExprClosure = input.parse()?;
        if !input.is_empty() {
            return Err(input.error("unexpected input after the transaction body"));
        }

        let mut block = Self::from_closure(closure)?;
        for attribute in attributes {
            block.apply_attribute(&attribute)?;
        }
        Ok(block)
    }
}

impl TransactionBlock {
    fn from_closure(closure: ExprClosure) -> syn::Result<Self> {
        if let Some(attribute) = closure.attrs.first() {
            return Err(Self::unknown_attribute(attribute));
        }
        if let Some(asyncness) = &closure.asyncness {
            return Err(syn::Error::new(
                asyncness.span(),
                "the body is already async; drop `async`",
            ));
        }
        if let Some(capture) = &closure.capture {
            return Err(syn::Error::new(
                capture.span(),
                "the body borrows what it uses and moves only what it consumes; drop `move`",
            ));
        }
        if closure.constness.is_some() || closure.lifetimes.is_some() {
            return Err(syn::Error::new(
                closure.span(),
                "expected `|bindings| body` or `|bindings| -> Result<T, E> { body }`",
            ));
        }

        let bindings = closure
            .inputs
            .iter()
            .map(Binding::from_pat)
            .collect::<syn::Result<Vec<_>>>()?;

        let maybe_result_type = match closure.output {
            ReturnType::Default => None,
            ReturnType::Type(_, result_type) => Some(*result_type),
        };

        let body = *closure.body;
        if maybe_result_type.is_none() {
            ReturnFinder::reject_return(&body)?;
        }

        Ok(Self {
            maybe_catalog: None,
            maybe_when: None,
            maybe_span: None,
            bindings,
            maybe_result_type,
            body,
        })
    }

    fn apply_attribute(&mut self, attribute: &Attribute) -> syn::Result<()> {
        let slot = if attribute.path().is_ident("catalog") {
            &mut self.maybe_catalog
        } else if attribute.path().is_ident("when") {
            &mut self.maybe_when
        } else if attribute.path().is_ident("span") {
            &mut self.maybe_span
        } else {
            return Err(Self::unknown_attribute(attribute));
        };

        if slot.replace(attribute.parse_args()?).is_some() {
            return Err(syn::Error::new(attribute.span(), "attribute given twice"));
        }
        Ok(())
    }

    fn unknown_attribute(attribute: &Attribute) -> syn::Error {
        syn::Error::new(
            attribute.span(),
            "expected `#[catalog(<dill::Catalog>)]`, `#[when(<bool>)]` or \
             `#[span(<tracing::Span>)]`",
        )
    }

    /// Wraps the body of an `async fn` returning `Result<T, E>` in a transaction
    /// opened from `catalog`, inside a debug span named after the function
    pub(crate) fn wrap_function(
        catalog: Expr,
        bindings: Vec<Binding>,
        function: ItemFn,
    ) -> syn::Result<TokenStream> {
        let ItemFn {
            attrs,
            vis,
            sig,
            block,
            ..
        } = function;

        let ReturnType::Type(_, result_type) = &sig.output else {
            return Err(syn::Error::new(
                sig.span(),
                "expected a function returning `Result<T, E>`",
            ));
        };
        // `impl Trait` cannot name the type of a local; the body infers it
        let mut result_type = (**result_type).clone();
        ImplTraitInferrer.visit_type_mut(&mut result_type);

        let name = &sig.ident;
        let transaction_block = Self {
            maybe_catalog: Some(catalog),
            maybe_when: None,
            maybe_span: Some(parse_quote! {
                ::database_common::__tracing::debug_span!(stringify!(#name))
            }),
            bindings,
            maybe_result_type: Some(result_type),
            body: Expr::Block(ExprBlock {
                attrs: Vec::new(),
                label: None,
                block: *block,
            }),
        }
        .expand();

        // Attributes written below the macro (`#[tracing::instrument]`, docs) stay on the
        // function
        Ok(quote! {
            #(#attrs)*
            #vis #sig {
                #transaction_block
            }
        })
    }

    pub(crate) fn expand(&self) -> TokenStream {
        let Self {
            maybe_catalog,
            maybe_when,
            maybe_span,
            bindings,
            maybe_result_type,
            body,
        } = self;

        let scope = Ident::new("transaction_scope", Span::mixed_site());
        let result = Ident::new("result", Span::mixed_site());

        let catalog = maybe_catalog
            .as_ref()
            .map_or_else(|| quote!(self.catalog), ToTokens::to_token_stream);
        let enabled = maybe_when
            .as_ref()
            .map_or_else(|| quote!(true), ToTokens::to_token_stream);
        let span = maybe_span.as_ref().map_or_else(
            || quote!(::database_common::__tracing::debug_span!("transaction")),
            ToTokens::to_token_stream,
        );
        let bindings = bindings.iter().map(|binding| binding.expand(&scope));

        let (result_type, body_result) = match maybe_result_type {
            Some(result_type) => (quote!(: #result_type), quote!(#body)),
            None => (quote!(), quote!(::std::result::Result::Ok(#body))),
        };

        let block = quote! {
            ::database_common::TransactionScope::instrument(
                async {
                    match ::database_common::TransactionScope::begin(&#catalog, #enabled).await {
                        ::std::result::Result::Ok(#scope) => {
                            let #result #result_type = async {
                                #(#bindings)*
                                #body_result
                            }
                            .await;
                            #scope.finish(#result).await
                        }
                        ::std::result::Result::Err(e) => {
                            ::std::result::Result::Err(::std::convert::From::from(e))
                        }
                    }
                },
                #span,
            )
            .await
        };

        match maybe_result_type {
            Some(_) => block,
            // Errors leave the enclosing function, as `?` in straight-line code would
            None => quote! {
                match #block {
                    ::std::result::Result::Ok(value) => value,
                    ::std::result::Result::Err(e) => return ::std::result::Result::Err(e),
                }
            },
        }
    }
}

/// `name` or `name: Type`, as an attribute lists them
impl Parse for Binding {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let name: Ident = input.parse()?;
        if input.parse::<Option<Token![:]>>()?.is_none() {
            return Ok(Self::Catalog(name));
        }
        Ok(Self::Component(name, input.parse()?))
    }
}

impl Binding {
    pub(crate) fn transaction_catalog(name: &str) -> Self {
        Self::Catalog(Ident::new(name, Span::call_site()))
    }

    /// Parses the comma-separated bindings of `#[transactional_methodN(..)]`,
    /// refusing any count other than `arity`
    pub(crate) fn parse_exactly(arity: usize, tokens: TokenStream) -> syn::Result<Vec<Self>> {
        let bindings = Punctuated::<Self, Token![,]>::parse_terminated.parse2(tokens)?;
        if bindings.len() != arity {
            return Err(syn::Error::new(
                Span::call_site(),
                format!(
                    "expected {arity} `name: Type` bindings, found {}",
                    bindings.len()
                ),
            ));
        }
        Ok(bindings.into_iter().collect())
    }

    fn from_pat(input: &Pat) -> syn::Result<Self> {
        if let Pat::Ident(pat_ident) = input {
            return Ok(Self::Catalog(Self::plain_name(pat_ident)?));
        }
        let Pat::Type(pat_type) = input else {
            return Err(Self::unexpected(input));
        };
        let Pat::Ident(pat_ident) = pat_type.pat.as_ref() else {
            return Err(Self::unexpected(pat_type));
        };
        Ok(Self::Component(
            Self::plain_name(pat_ident)?,
            (*pat_type.ty).clone(),
        ))
    }

    fn plain_name(pat_ident: &PatIdent) -> syn::Result<Ident> {
        if pat_ident.by_ref.is_some()
            || pat_ident.mutability.is_some()
            || pat_ident.subpat.is_some()
        {
            return Err(Self::unexpected(pat_ident));
        }
        Ok(pat_ident.ident.clone())
    }

    fn unexpected(pat: &impl Spanned) -> syn::Error {
        syn::Error::new(
            pat.span(),
            "expected `name` for the transaction catalog or `name: Type` for a component",
        )
    }

    fn expand(&self, scope: &Ident) -> TokenStream {
        match self {
            Self::Catalog(name) => quote! { let #name = #scope.catalog().clone(); },
            Self::Component(name, component_type) => {
                quote! { let #name: #component_type = #scope.resolve()?; }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Replaces each `impl Trait` in a type with `_`
struct ImplTraitInferrer;

impl VisitMut for ImplTraitInferrer {
    fn visit_type_mut(&mut self, ty: &mut Type) {
        if matches!(ty, Type::ImplTrait(_)) {
            *ty = parse_quote!(_);
        } else {
            visit_mut::visit_type_mut(self, ty);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Finds a `return` that would leave the transaction body, skipping the nested
/// closures, async blocks and items it does not leave. The tokens of a macro
/// invocation are not parsed, so any `return` among them counts, even one in a
/// closure passed to the macro
struct ReturnFinder {
    maybe_found: Option<Span>,
}

impl ReturnFinder {
    fn reject_return(body: &Expr) -> syn::Result<()> {
        let mut finder = Self { maybe_found: None };
        finder.visit_expr(body);
        match finder.maybe_found {
            None => Ok(()),
            Some(span) => Err(syn::Error::new(
                span,
                "`return` would leave only the transaction body, not the function; return the \
                 value from the body, or give the body a `-> Result<T, E>` type to return from it",
            )),
        }
    }

    fn find_return_token(tokens: TokenStream) -> Option<Span> {
        tokens.into_iter().find_map(|token| match token {
            TokenTree::Ident(ident) if ident == "return" => Some(ident.span()),
            TokenTree::Group(group) => Self::find_return_token(group.stream()),
            TokenTree::Ident(_) | TokenTree::Punct(_) | TokenTree::Literal(_) => None,
        })
    }
}

impl<'ast> Visit<'ast> for ReturnFinder {
    fn visit_expr_return(&mut self, expr: &'ast ExprReturn) {
        self.maybe_found.get_or_insert(expr.return_token.span);
    }

    fn visit_expr_closure(&mut self, _: &'ast ExprClosure) {}

    fn visit_expr_async(&mut self, _: &'ast ExprAsync) {}

    fn visit_item(&mut self, _: &'ast Item) {}

    fn visit_macro(&mut self, mac: &'ast Macro) {
        if self.maybe_found.is_none() {
            self.maybe_found = Self::find_return_token(mac.tokens.clone());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> syn::Result<TransactionBlock> {
        syn::parse_str(input)
    }

    fn error_of(input: &str) -> String {
        match parse(input) {
            Ok(_) => panic!("`{input}` parsed"),
            Err(e) => e.to_string(),
        }
    }

    #[test]
    fn test_parses_bindings_and_attributes() {
        let block = parse(
            "#[catalog(base_catalog)] #[when(enabled)] #[span(make_span())] |catalog, svc: \
             Arc<dyn Svc>| -> Result<_, E> { svc.run(&catalog).await }",
        )
        .unwrap();

        assert!(block.maybe_catalog.is_some());
        assert!(block.maybe_when.is_some());
        assert!(block.maybe_span.is_some());
        assert!(block.maybe_result_type.is_some());
        let binding_kinds: Vec<_> = block
            .bindings
            .iter()
            .map(|binding| match binding {
                Binding::Catalog(_) => "catalog",
                Binding::Component(..) => "component",
            })
            .collect();
        assert_eq!(binding_kinds, ["catalog", "component"]);
    }

    #[test]
    fn test_parses_empty_bindings_with_default_catalog() {
        let block = parse("|| { 1 }").unwrap();

        assert!(block.maybe_catalog.is_none());
        assert!(block.bindings.is_empty());
        assert!(block.maybe_result_type.is_none());
    }

    #[test]
    fn test_refuses_malformed_blocks() {
        let cases = [
            // `return` would leave only the body of a block without a return type
            ("|| { if x { return Ok(()); } }", "`return`"),
            ("|| { match x { _ => return 1 } }", "`return`"),
            ("|| { loop { return; } }", "`return`"),
            // The tokens of a macro call are searched whole, closures in them included
            ("|| { select! { _ = a => return Ok(()), } }", "`return`"),
            ("|| { log!(\"{}\", { return 1; }); }", "`return`"),
            ("|| { outer!(inner!(return)); }", "`return`"),
            ("|| { log!(\"{}\", items.map(|i| return i)); }", "`return`"),
            // The body is already async and borrows what it uses
            ("async || {}", "drop `async`"),
            ("async move || {}", "drop `async`"),
            ("move || {}", "drop `move`"),
            ("for<'a> || {}", "expected `|bindings| body`"),
            // Attributes
            ("#[timeout(5)] || {}", "expected `#[catalog"),
            ("#[catalog(a)] #[catalog(b)] || {}", "given twice"),
            ("#[when(a)] #[when(b)] || {}", "given twice"),
            ("#[span(a)] #[span(b)] || {}", "given twice"),
            // Bindings
            ("|(a, b): (A, B)| {}", "expected `name`"),
            ("|mut catalog| {}", "expected `name`"),
            ("|ref catalog| {}", "expected `name`"),
            ("|_: Arc<dyn Svc>| {}", "expected `name`"),
            (
                "|| {} trailing",
                "unexpected input after the transaction body",
            ),
        ];

        for (input, expected_error) in cases {
            let error = error_of(input);
            assert!(
                error.contains(expected_error),
                "`{input}` failed with `{error}`, expected `{expected_error}`"
            );
        }
    }

    #[test]
    fn test_accepts_well_formed_blocks() {
        let cases = [
            // A body with a return type may return from itself
            "|| -> Result<(), E> { if x { return Ok(()); } Ok(()) }",
            "|| -> Result<(), E> { select! { _ = a => return Ok(()), } }",
            "|svc: Arc<dyn Svc>| -> Result<_, E> { svc.run().await }",
            // A `return` that leaves only a nested closure, async block or item
            "|| { items.iter().map(|i| { return i; }).count() }",
            "|| { async { return 1; }.await }",
            "|| { async move { return 1; }.await }",
            "|| { fn helper() -> u32 { return 1; } helper() }",
            "|| { log!(\"{}\", value) }",
            "#[span(make_span())] #[when(enabled)] #[catalog(base)] |catalog| { catalog }",
        ];

        for input in cases {
            if let Err(e) = parse(input) {
                panic!("`{input}` was refused: {e}");
            }
        }
    }

    #[test]
    fn test_method_bindings_must_match_the_arity() {
        let cases = [
            (1, "svc: Arc<dyn Svc>", Ok(1)),
            (1, "svc: Arc<dyn Svc>,", Ok(1)),
            (2, "a: Arc<dyn A>, b: Arc<dyn B>", Ok(2)),
            (1, "", Err("expected 1 `name: Type` bindings, found 0")),
            (
                1,
                "a: Arc<dyn A>, b: Arc<dyn B>",
                Err("expected 1 `name: Type` bindings, found 2"),
            ),
            (
                3,
                "a: Arc<dyn A>, b: Arc<dyn B>",
                Err("expected 3 `name: Type` bindings, found 2"),
            ),
        ];

        for (arity, input, expected) in cases {
            let actual = Binding::parse_exactly(arity, input.parse().unwrap())
                .map(|bindings| bindings.len())
                .map_err(|e| e.to_string());
            assert_eq!(
                actual,
                expected.map_err(ToString::to_string),
                "arity {arity}, `{input}`"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
