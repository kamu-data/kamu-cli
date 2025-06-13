// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<UpdateDatasetTaskPlanner>();
    catalog_builder.add::<DeliverWebhookTaskPlanner>();
    catalog_builder.add::<HardCompactDatasetTaskPlanner>();
    catalog_builder.add::<ResetDatasetTaskPlanner>();

    catalog_builder.add::<UpdateDatasetTaskRunner>();
    catalog_builder.add::<DeliverWebhookTaskRunner>();
    catalog_builder.add::<HardCompactDatasetTaskRunner>();
    catalog_builder.add::<ResetDatasetTaskRunner>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
