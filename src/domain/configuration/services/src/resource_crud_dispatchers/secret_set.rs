// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_configuration::SecretSetResource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources_services::declare_resource_crud_dispatcher!(
    dispatcher = SecretSetResourceCrudDispatcher,
    resource = SecretSetResource
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_secret_set_resource_crud_dispatcher(catalog_builder: &mut dill::CatalogBuilder) {
    catalog_builder.add::<SecretSetResourceCrudDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
