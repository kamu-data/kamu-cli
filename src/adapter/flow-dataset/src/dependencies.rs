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

pub fn register_dependencies(
    catalog_builder: &mut CatalogBuilder,
    opts: FlowDatasetAdapterDependencyOpts,
) {
    catalog_builder.add::<FlowControllerIngest>();
    catalog_builder.add::<FlowControllerTransform>();
    catalog_builder.add::<FlowControllerCompact>();
    catalog_builder.add::<FlowControllerReset>();
    catalog_builder.add::<FlowControllerResetToMetadata>();

    catalog_builder.add::<FlowDatasetsEventBridge>();
    if opts.with_default_transform_evaluator {
        catalog_builder.add::<TransformFlowEvaluatorImpl>();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct FlowDatasetAdapterDependencyOpts {
    pub with_default_transform_evaluator: bool,
}

impl Default for FlowDatasetAdapterDependencyOpts {
    fn default() -> Self {
        Self {
            with_default_transform_evaluator: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
