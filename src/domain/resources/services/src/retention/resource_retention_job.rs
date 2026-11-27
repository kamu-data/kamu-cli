// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use background_agent_host::{HousekeepingBatchReport, HousekeepingJob};
use chrono::{DateTime, Utc};
use database_common_macros::{transactional_method1, transactional_method4};
use dill::{CatalogWeakRef, Singleton, component, interface, scope};
use internal_error::InternalError;
use messaging_outbox::{Outbox, OutboxExt};

use crate::domain::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceControllerStateRepository,
    ResourceDueForPurge,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceRawEventStore,
    ResourceRepository,
    TypeUri,
    resource_type_name,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_RETENTION_JOB_NAME: &str = "dev.kamu.domain.resources.ResourceRetentionJob";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Purges deleted resources whose retention ended, in one transaction per
/// batch: claims the due resources, then deletes their controller bookkeeping,
/// their events and their rows, which take the labels, owners and configuration
/// projections with them by cascade, and what the removal listeners keep.
/// Posts one `Purged` message per type of the batch, and never touches messages
/// already in the Outbox, so a consumer behind still receives the outcomes of a
/// purged resource.
///
/// The claim is a compare-and-set on the deadline, so of two purgers that found
/// the same resources, the second claims none of them and purges nothing.
pub struct ResourceRetentionJob {
    catalog: CatalogWeakRef,
}

#[component(pub)]
#[interface(dyn HousekeepingJob)]
#[scope(Singleton)]
impl ResourceRetentionJob {
    pub fn new(catalog: CatalogWeakRef) -> Self {
        Self { catalog }
    }

    fn ids_by_schema(purged: &[ResourceDueForPurge]) -> BTreeMap<TypeUri, Vec<ResourceID>> {
        let mut ids_by_schema: BTreeMap<TypeUri, Vec<ResourceID>> = BTreeMap::new();
        for resource in purged {
            ids_by_schema
                .entry(resource.schema.clone())
                .or_default()
                .push(resource.id);
        }
        ids_by_schema
    }

    /// The counts by resource type, the `kind` of the removed counts. A
    /// stored schema that is not canonical counts under itself rather than
    /// failing the purge for a label
    fn removed_by_type(
        ids_by_schema: &BTreeMap<TypeUri, Vec<ResourceID>>,
    ) -> BTreeMap<String, u64> {
        let mut removed_by_type = BTreeMap::new();
        for (schema, ids) in ids_by_schema {
            let kind = resource_type_name(schema)
                .map_or_else(|_| schema.to_string(), |type_name| type_name.to_string());
            *removed_by_type.entry(kind).or_default() += u64::try_from(ids.len()).unwrap();
        }
        removed_by_type
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%now, batch_size))]
    #[transactional_method4(
        resource_repository: Arc<dyn ResourceRepository>,
        controller_states: Arc<dyn ResourceControllerStateRepository>,
        event_store: Arc<dyn ResourceRawEventStore>,
        outbox: Arc<dyn Outbox>
    )]
    async fn purge_batch(
        &self,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        // One row past the batch tells whether more is due, without counting
        let mut due = resource_repository
            .find_resources_due_for_purge(now, batch_size.saturating_add(1))
            .await?;
        let more_due = due.len() > batch_size;
        due.truncate(batch_size);

        let due_ids: Vec<ResourceID> = due.iter().map(|resource| resource.id).collect();
        let claimed_ids = resource_repository
            .claim_resources_for_purge(&due_ids, now)
            .await?;
        if claimed_ids.is_empty() {
            return Ok(HousekeepingBatchReport {
                removed_by_kind: BTreeMap::new(),
                more_due,
            });
        }
        let claimed: Vec<ResourceDueForPurge> = due
            .into_iter()
            .filter(|resource| claimed_ids.contains(&resource.id))
            .collect();

        controller_states
            .delete_states_for_resources(&claimed_ids)
            .await?;
        event_store.delete_events_of_resources(&claimed_ids).await?;
        resource_repository.purge_resources(&claimed_ids).await?;

        let ids_by_schema = Self::ids_by_schema(&claimed);
        let removed_by_kind = Self::removed_by_type(&ids_by_schema);
        for (schema, ids) in ids_by_schema {
            outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
                    ResourceLifecycleMessage::purged(now, schema, ids),
                )
                .await?;
        }

        Ok(HousekeepingBatchReport {
            removed_by_kind,
            more_due,
        })
    }

    #[transactional_method1(resource_repository: Arc<dyn ResourceRepository>)]
    async fn count_due_for_purge(&self, now: DateTime<Utc>) -> Result<u64, InternalError> {
        resource_repository.count_resources_due_for_purge(now).await
    }

    #[transactional_method1(resource_repository: Arc<dyn ResourceRepository>)]
    async fn find_next_due(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, InternalError> {
        resource_repository.find_nearest_purge_at(now).await
    }
}

#[async_trait::async_trait]
impl HousekeepingJob for ResourceRetentionJob {
    fn job_name(&self) -> &'static str {
        RESOURCE_RETENTION_JOB_NAME
    }

    async fn run_batch(
        &self,
        now: DateTime<Utc>,
        batch_size: usize,
    ) -> Result<HousekeepingBatchReport, InternalError> {
        self.purge_batch(now, batch_size).await
    }

    async fn count_due(&self, now: DateTime<Utc>) -> Result<u64, InternalError> {
        self.count_due_for_purge(now).await
    }

    async fn next_due(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, InternalError> {
        self.find_next_due(now).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
