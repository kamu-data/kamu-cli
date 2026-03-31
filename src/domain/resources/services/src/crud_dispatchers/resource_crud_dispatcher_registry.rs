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
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_crud_dispatcher<E>(
    target_catalog: &dill::Catalog,
    kind: &str,
    api_version: &str,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, E>
where
    E: From<UnsupportedResourceDescriptorError> + From<InternalError>,
{
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| {
            meta.descriptor.resource_type == kind && meta.descriptor.api_version == api_version
        },
    );

    let Some(builder) = handlers.next() else {
        return Err(UnsupportedResourceDescriptorError::NotFound {
            kind: kind.to_string(),
            api_version: api_version.to_string(),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(UnsupportedResourceDescriptorError::Duplicate {
            kind: kind.to_string(),
            api_version: api_version.to_string(),
        }
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_resource_crud_dispatcher_by_kind<E>(
    target_catalog: &dill::Catalog,
    kind: &str,
) -> Result<std::sync::Arc<dyn ResourceCrudDispatcher>, E>
where
    E: From<UnsupportedResourceDescriptorError> + From<InternalError>,
{
    let mut handlers = target_catalog.builders_for_with_meta::<dyn ResourceCrudDispatcher, _>(
        |meta: &ResourceDispatcherMeta| meta.descriptor.resource_type == kind,
    );

    let Some(builder) = handlers.next() else {
        return Err(UnsupportedResourceDescriptorError::NotFound {
            kind: kind.to_string(),
            api_version: "*".to_string(),
        }
        .into());
    };

    if handlers.next().is_some() {
        return Err(UnsupportedResourceDescriptorError::Duplicate {
            kind: kind.to_string(),
            api_version: "*".to_string(),
        }
        .into());
    }

    builder.get(target_catalog).int_err().map_err(Into::into)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
