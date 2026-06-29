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
    UnsupportedResourceDescriptorError,
    resource_kind_matches_selector,
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
            schema: schema.to_string(),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(UnsupportedResourceDescriptorError::Duplicate {
            schema: schema.to_string(),
        }
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_crud_dispatcher_by_selector<E>(
    target_catalog: &dill::Catalog,
    selector: &str,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, E>
where
    E: From<UnsupportedResourceDescriptorError> + From<InternalError>,
{
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| {
            resource_kind_matches_selector(meta.name, meta.short_names, selector)
        },
    );

    let Some(builder) = handlers.next() else {
        return Err(UnsupportedResourceDescriptorError::SelectorNotFound {
            selector: selector.to_string(),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(UnsupportedResourceDescriptorError::SelectorDuplicate {
            selector: selector.to_string(),
        }
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
