// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::Catalog;
use internal_error::InternalError;

use crate::{DatabaseTransactionRunner, ManagedEntity, ManagedOperation, TransactionRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct ManagedOperationImpl {
    catalog: Catalog,
    tracked_entities: Mutex<Vec<Arc<dyn ManagedEntity>>>,
}

impl ManagedOperationImpl {
    pub(crate) fn new(catalog: Catalog) -> Self {
        Self {
            catalog,
            tracked_entities: Mutex::new(vec![]),
        }
    }

    async fn do_commit_managed_entities(
        transactional_catalog: &Catalog,
        tracked_entities: Vec<Arc<dyn ManagedEntity>>,
    ) -> Result<(), InternalError> {
        use futures::stream::{StreamExt, TryStreamExt};
        futures::stream::iter(tracked_entities)
            .map(Ok)
            .try_for_each_concurrent(None, |managed_entity| async move {
                managed_entity.do_commit(transactional_catalog).await
            })
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ManagedOperation for ManagedOperationImpl {
    fn track_entity(&self, managed_entity: Arc<dyn ManagedEntity>) {
        self.tracked_entities.lock().unwrap().push(managed_entity);
    }

    async fn do_commit(self: Arc<Self>) -> Result<(), InternalError> {
        let context =
            Arc::<Self>::into_inner(self).expect("Nobody else should keep managed operation");

        let tracked_entities = context.tracked_entities.into_inner().unwrap();

        if context.catalog.get_one::<TransactionRef>().is_ok() {
            Self::do_commit_managed_entities(&context.catalog, tracked_entities).await
        } else {
            DatabaseTransactionRunner::new(context.catalog.clone())
                .transactional(|transactional_catalog: Catalog| async move {
                    Self::do_commit_managed_entities(&transactional_catalog, tracked_entities).await
                })
                .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
