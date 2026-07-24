// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod annotations;
mod conditions;
mod labels;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_built_in_extension_schema_dispatchers(catalog_builder: &mut dill::CatalogBuilder) {
    annotations::register_built_in_annotation_schema_dispatchers(catalog_builder);
    labels::register_built_in_label_schema_dispatchers(catalog_builder);
    conditions::register_built_in_condition_schema_dispatchers(catalog_builder);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
