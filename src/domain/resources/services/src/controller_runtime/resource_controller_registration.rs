// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use dill::{Builder as _, BuilderExt as _};
use internal_error::{InternalError, ResultIntoInternal};

use crate::domain::{
    ActionableResourcesQuery,
    ResourceController,
    ResourceControllerMeta,
    ResourceDispatcherMeta,
    ResourceLifecycleEventDispatcher,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A registered controller with its metadata
pub(crate) struct ResourceControllerRegistration {
    meta: ResourceControllerMeta,
    controller: Arc<dyn ResourceController>,
    actionable_query: ActionableResourcesQuery,
}

impl ResourceControllerRegistration {
    /// Every controller registered in the catalog. Two controllers of one
    /// schema, two with one name, or a controller of a schema that also has a
    /// lifecycle dispatcher, are a wiring error: the last would reconcile the
    /// type twice.
    pub fn collect_all(catalog: &dill::Catalog) -> Result<Vec<Self>, InternalError> {
        let mut registrations = Vec::new();
        let mut schemas = HashSet::new();
        let mut names = HashSet::new();

        for builder in catalog.builders_for::<dyn ResourceController>() {
            let metas = builder.metadata_get_all::<ResourceControllerMeta>();
            let [meta] = metas.as_slice() else {
                return Err(InternalError::new(format!(
                    "Resource controller {} must carry exactly one ResourceControllerMeta, found \
                     {}",
                    builder.instance_type().name,
                    metas.len()
                )));
            };
            let meta = (*meta).clone();

            if !schemas.insert(meta.schema) {
                return Err(InternalError::new(format!(
                    "Duplicate resource controller registered for schema='{}'",
                    meta.schema
                )));
            }
            if !names.insert(meta.controller_name.clone()) {
                return Err(InternalError::new(format!(
                    "Duplicate resource controller registered with name='{}'",
                    meta.controller_name.as_str()
                )));
            }
            if Self::has_lifecycle_dispatcher(catalog, meta.schema) {
                return Err(InternalError::new(format!(
                    "Resource controller registered for schema='{}', which also has a lifecycle \
                     dispatcher: register the type's service layer without it",
                    meta.schema
                )));
            }

            let controller = builder.get(catalog).int_err()?;
            registrations.push(Self::new(meta, controller)?);
        }

        Ok(registrations)
    }

    fn has_lifecycle_dispatcher(catalog: &dill::Catalog, schema: &str) -> bool {
        catalog
            .builders_for_with_meta::<dyn ResourceLifecycleEventDispatcher, _>(
                |meta: &ResourceDispatcherMeta| meta.schema == schema,
            )
            .next()
            .is_some()
    }

    fn new(
        meta: ResourceControllerMeta,
        controller: Arc<dyn ResourceController>,
    ) -> Result<Self, InternalError> {
        let actionable_query = ActionableResourcesQuery {
            controller: meta.controller_name.clone(),
            schema: meta.schema.parse::<TypeUri>().int_err()?,
            controller_revision: meta.controller_revision,
            reacts_to_status: meta.reacts_to_status,
        };

        Ok(Self {
            meta,
            controller,
            actionable_query,
        })
    }

    pub fn meta(&self) -> &ResourceControllerMeta {
        &self.meta
    }

    pub fn controller(&self) -> &Arc<dyn ResourceController> {
        &self.controller
    }

    pub fn actionable_query(&self) -> &ActionableResourcesQuery {
        &self.actionable_query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
