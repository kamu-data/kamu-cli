// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<VariableSetApplyResourceUseCaseImpl>();
    catalog_builder.add::<VariableSetDeleteResourcesUseCaseImpl>();
    catalog_builder.add::<SecretSetDeleteResourcesUseCaseImpl>();
    catalog_builder.add::<StorageDeleteResourcesUseCaseImpl>();

    catalog_builder.add::<VariableSetEventStoreBridge>();
    catalog_builder.add::<VariableSetGetResourceByIdUseCaseImpl>();
    catalog_builder.add::<VariableSetListResourcesByKindUseCaseImpl>();
    catalog_builder.add::<VariableSetReconcileResourceUseCaseImpl>();
    catalog_builder.add::<VariableSetReconcilerImpl>();

    catalog_builder.add::<SecretSetApplyResourceUseCaseImpl>();
    catalog_builder.add::<SecretSetEventStoreBridge>();
    catalog_builder.add::<SecretSetGetResourceByIdUseCaseImpl>();
    catalog_builder.add::<SecretSetListResourcesByKindUseCaseImpl>();
    catalog_builder.add::<SecretSetReconcileResourceUseCaseImpl>();
    catalog_builder.add::<SecretSetReconcilerImpl>();

    catalog_builder.add::<StorageApplyResourceUseCaseImpl>();
    catalog_builder.add::<StorageEventStoreBridge>();
    catalog_builder.add::<StorageGetResourceByIdUseCaseImpl>();
    catalog_builder.add::<StorageListResourcesByKindUseCaseImpl>();
    catalog_builder.add::<StorageReconcileResourceUseCaseImpl>();
    catalog_builder.add::<StorageReconcilerImpl>();

    catalog_builder.add::<ListAllResourcesUseCaseImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
