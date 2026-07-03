// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};

use crate::TypeUri;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Registry key for a per-type resource dispatcher.
///
/// `schema` is intentionally a `&'static str` (not a [`TypeUri`]): dill's
/// `#[meta(...)]` emits the metadata as a `const`, so the value must be
/// const-evaluable. It is only ever used as an internal registry key; the
/// public schema identity type is [`TypeUri`] everywhere else.
#[derive(Debug, Clone)]
pub struct ResourceDispatcherMeta {
    pub schema: &'static str,
    pub canonical_selector: &'static str,
    pub selector_aliases: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_dispatcher_from_catalog<TDispatcher: ?Sized + 'static>(
    target_catalog: &Catalog,
    schema: &TypeUri,
    dispatcher_name: &str,
) -> Result<Arc<TDispatcher>, InternalError> {
    let schema_str = schema.as_str();
    let mut dispatchers =
        target_catalog.builders_for_with_meta::<TDispatcher, _>(|meta: &ResourceDispatcherMeta| {
            meta.schema == schema_str
        });

    dispatchers
        .next()
        .map(|builder| {
            if dispatchers.next().is_some() {
                return Err(InternalError::new(format!(
                    "Duplicate {dispatcher_name} registered for schema='{schema}'",
                )));
            }

            builder.get(target_catalog).int_err()
        })
        .transpose()?
        .ok_or_else(|| {
            InternalError::new(format!(
                "No {dispatcher_name} registered for schema='{schema}'",
            ))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
