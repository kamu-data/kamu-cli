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
    catalog_builder.add::<FlowSupportServiceImpl>();

    catalog_builder.add::<FlowDispatcherIngest>();
    catalog_builder.add::<FlowDispatcherTransform>();
    catalog_builder.add::<FlowDispatcherCompact>();
    catalog_builder.add::<FlowDispatcherReset>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
