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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceDispatcherMeta {
    pub schema: &'static str,
    pub name: &'static str,
    pub short_names: &'static [&'static str],
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_dispatcher_from_catalog<TDispatcher: ?Sized + 'static>(
    target_catalog: &Catalog,
    schema: &str,
    dispatcher_name: &str,
) -> Result<Arc<TDispatcher>, InternalError> {
    let mut dispatchers =
        target_catalog.builders_for_with_meta::<TDispatcher, _>(|meta: &ResourceDispatcherMeta| {
            meta.schema == schema
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
