// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

extern crate proc_macro;

use syn::punctuated::Pair;

fn panic_generic() -> ! {
    panic!(
        "Aggregate derive macro is only supported on structs that follow \
         `MyAggregate(Aggregate<MyProjection, MyEventStore>)` structure"
    )
}

#[proc_macro_derive(Aggregate)]
pub fn derive_aggregate(tokens: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input: syn::DeriveInput = syn::parse(tokens).unwrap();

    let type_name = input.ident;

    let syn::Data::Struct(data_struct) = input.data else {
        panic_generic()
    };
    let syn::Fields::Unnamed(mut aggregate) = data_struct.fields else {
        panic_generic()
    };
    let Some(aggregate) = aggregate.unnamed.pop().map(Pair::into_value) else {
        panic_generic()
    };

    let syn::Type::Path(mut aggregate_type) = aggregate.ty else {
        panic_generic()
    };

    let Some(syn::PathArguments::AngleBracketed(mut generic_args)) = aggregate_type
        .path
        .segments
        .pop()
        .map(|s| s.into_value().arguments)
    else {
        panic_generic()
    };

    if generic_args.args.len() != 2 {
        panic_generic()
    }

    let Some(syn::GenericArgument::Type(store_type)) =
        generic_args.args.pop().map(Pair::into_value)
    else {
        panic_generic()
    };
    let Some(syn::GenericArgument::Type(proj_type)) = generic_args.args.pop().map(Pair::into_value)
    else {
        panic_generic()
    };

    quote::quote! {
        impl #type_name {
            #[inline]
            pub async fn load(
                query: <#proj_type as ::event_sourcing::Projection>::Query,
                event_store: &#store_type,
            ) -> Result<Self, LoadError<#proj_type>> {
                let agg = ::event_sourcing::Aggregate::load(query, event_store).await?;
                Ok(Self(agg))
            }

            #[inline]
            pub async fn load_multi(
                queries: Vec<<#proj_type as ::event_sourcing::Projection>::Query>,
                event_store: &#store_type,
            ) -> Result<Vec<Result<Self, LoadError<#proj_type>>>, GetEventsError> {
                let aggs = ::event_sourcing::Aggregate::load_multi(queries, event_store).await?;
                let mut result = vec![];
                for agg in aggs {
                    let res = match agg {
                        Err(e) => Err(e),
                        Ok(a) => Ok(Self(a)),
                    };
                    result.push(res);
                }
                Ok(result)
            }

            #[inline]
            pub async fn try_load(
                query: <#proj_type as ::event_sourcing::Projection>::Query,
                event_store: &#store_type,
            ) -> Result<Option<Self>, TryLoadError<#proj_type>> {
                let maybe_agg = ::event_sourcing::Aggregate::try_load(query, event_store).await?;
                Ok(maybe_agg.map(|agg| Self(agg)))
            }

            #[inline]
            pub async fn load_ext(
                query: <#proj_type as ::event_sourcing::Projection>::Query,
                event_store: &#store_type,
                opts: LoadOpts,
            ) -> Result<Self, LoadError<#proj_type>> {
                let agg = ::event_sourcing::Aggregate::load_ext(query, event_store, opts).await?;
                Ok(Self(agg))
            }

            #[inline]
            pub async fn update(&mut self, event_store: &#store_type) -> Result<(), UpdateError<#proj_type>> {
                self.0.update(event_store).await
            }

            #[inline]
            pub async fn update_ext(
                &mut self,
                event_store: &#store_type,
                opts: LoadOpts,
            ) -> Result<(), UpdateError<#proj_type>> {
                self.0.update_ext(event_store, opts).await
            }

            #[inline]
            pub async fn save(&mut self, event_store: &#store_type) -> Result<(), SaveError> {
                self.0.save(event_store).await
            }

            #[inline]
            pub fn apply<Event>(
                &mut self,
                event: Event,
            ) -> Result<(), ProjectionError<#proj_type>>
            where
                Event: Into<<#proj_type as ::event_sourcing::Projection>::Event>
            {
                self.0.apply(event.into())
            }

            #[inline]
            pub fn has_updates(&self) -> bool {
                !self.0.has_updates()
            }

            #[inline]
            pub fn last_stored_event_id(&self) -> Option<&EventID> {
                self.0.last_stored_event_id()
            }
        }

        impl ::std::ops::Deref for #type_name {
            type Target = #proj_type;

            fn deref(&self) -> &Self::Target {
                &self.0.as_state()
            }
        }


        impl AsRef<#proj_type> for #type_name {
            fn as_ref(&self) -> &#proj_type {
                self.0.as_state()
            }
        }

        impl Into<#proj_type> for #type_name {
            fn into(self) -> #proj_type {
                self.0.into_state()
            }
        }
    }
    .into()
}
