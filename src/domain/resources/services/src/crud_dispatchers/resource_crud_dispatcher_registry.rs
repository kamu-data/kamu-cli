// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceCrudDispatcher,
    ResourceDispatcherMeta,
    ResourceTypeSelectorRaw,
    TypeUri,
    UnsupportedResourceDescriptorError,
    UnsupportedResourceSelectorError,
    resource_selector_parts_match,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_crud_dispatcher<E>(
    target_catalog: &dill::Catalog,
    schema: &str,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, E>
where
    E: From<UnsupportedResourceDescriptorError> + From<InternalError>,
{
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| meta.schema == schema,
    );

    let Some(builder) = handlers.next() else {
        return Err(UnsupportedResourceDescriptorError::NotFound {
            schema: TypeUri::new_unchecked(schema),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(InternalError::new(format!(
            "Multiple CRUD dispatchers registered for schema '{schema}'"
        ))
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves a CRUD dispatcher for a schema that is already known to be valid
/// (e.g. read from a stored snapshot, or resolved from a registered selector).
/// A missing/duplicate dispatcher is therefore a data-integrity catastrophe, so
/// every failure — including "not found" — is surfaced as an [`InternalError`]
/// rather than a user-facing unsupported-descriptor error.
pub fn get_resource_crud_dispatcher_for_trusted_schema(
    target_catalog: &dill::Catalog,
    schema: &str,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, InternalError> {
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| meta.schema == schema,
    );

    let Some(builder) = handlers.next() else {
        return Err(InternalError::new(format!(
            "No CRUD dispatcher registered for trusted schema '{schema}'"
        )));
    };

    if handlers.next().is_some() {
        return Err(InternalError::new(format!(
            "Multiple CRUD dispatchers registered for trusted schema '{schema}'"
        )));
    }

    builder.get(target_catalog).int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_crud_dispatcher_by_raw_selector<E>(
    target_catalog: &dill::Catalog,
    raw_selector: &ResourceTypeSelectorRaw,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, E>
where
    E: From<UnsupportedResourceSelectorError> + From<InternalError>,
{
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| {
            resource_selector_parts_match(
                meta.canonical_selector,
                meta.selector_aliases.iter().copied(),
                raw_selector,
            )
        },
    );

    let Some(builder) = handlers.next() else {
        return Err(UnsupportedResourceSelectorError::NotFound {
            raw_selector: raw_selector.clone(),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(InternalError::new(format!(
            "Multiple CRUD dispatchers registered for selector '{raw_selector}'"
        ))
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
