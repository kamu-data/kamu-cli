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

use chrono::{DateTime, TimeDelta, Utc};
use database_common::testing::in_committed_transaction;
use dill::{Catalog, CatalogBuilder};
use kamu_resources::{
    DeclarativeResource,
    ResourceAggregateLoader,
    ResourceDeletionCompletion,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceRepository,
    ResourceRetentionConfig,
    ResourceRetentionPolicy,
    ResourceSchemaProvider,
    resource_type_name,
};
use kamu_resources_services::testing::{BaseResourceServiceHarnessOpts, TestResource};
use messaging_outbox::OutboxProvider;

use crate::tests::utils::{ServiceLayerTestHarness, ServiceLayerTestHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completed_deletion_is_purged_after_the_type_retention() {
    use ResourceRetentionPolicy::{PurgeImmediately, RetainFor, RetainForever};

    let days = TimeDelta::days;
    for (default, override_of_type, retention) in [
        (RetainFor(days(7)), None, Some(days(7))),
        (RetainForever, Some(RetainFor(days(30))), Some(days(30))),
        (
            RetainFor(days(7)),
            Some(PurgeImmediately),
            Some(TimeDelta::zero()),
        ),
        (RetainFor(days(7)), Some(RetainForever), None),
        (RetainForever, None, None),
    ] {
        let harness = RetentionHarness::new(retention_config(default, override_of_type)).await;
        let id = harness.apply_and_request_deletion("res").await;

        harness.complete_deletion(harness.catalog(), id).await;

        let deleted_at = harness.deleted_at(&id).await;
        pretty_assertions::assert_eq!(
            harness.purge_at(&id).await,
            retention.map(|retention| deleted_at + retention),
            "default={default:?}, override={override_of_type:?}"
        );
    }
}

#[test_log::test(tokio::test)]
async fn test_resource_being_deleted_has_no_purge_deadline() {
    let harness = RetentionHarness::new(ResourceRetentionConfig {
        default: ResourceRetentionPolicy::PurgeImmediately,
        types: BTreeMap::new(),
    })
    .await;

    let id = harness.apply_and_request_deletion("res").await;

    pretty_assertions::assert_eq!(harness.purge_at(&id).await, None);
}

#[test_log::test(tokio::test)]
async fn test_policy_change_applies_to_later_deletions_only() {
    let harness = RetentionHarness::new_without_retention().await;
    let before = harness.catalog_with(ResourceRetentionConfig::default());
    let after = harness.catalog_with(retention_config(
        ResourceRetentionPolicy::RetainFor(TimeDelta::days(1)),
        None,
    ));

    let earlier = harness
        .apply_and_request_deletion_in(&before, "earlier")
        .await;
    harness.complete_deletion(&before, earlier).await;
    let later = harness.apply_and_request_deletion_in(&after, "later").await;
    harness.complete_deletion(&after, later).await;

    pretty_assertions::assert_eq!(
        harness.purge_at(&earlier).await,
        Some(harness.deleted_at(&earlier).await + TimeDelta::days(7))
    );
    pretty_assertions::assert_eq!(
        harness.purge_at(&later).await,
        Some(harness.deleted_at(&later).await + TimeDelta::days(1))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn retention_config(
    default: ResourceRetentionPolicy,
    override_of_type: Option<ResourceRetentionPolicy>,
) -> ResourceRetentionConfig {
    ResourceRetentionConfig {
        default,
        types: override_of_type
            .map(|policy| {
                (
                    resource_type_name(TestResource::type_uri()).unwrap(),
                    policy,
                )
            })
            .into_iter()
            .collect(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ServiceLayerTestHarness, base)]
struct RetentionHarness {
    base: ServiceLayerTestHarness,
    account: odf::ResourceOwnerHandle,
}

impl RetentionHarness {
    async fn new(retention: ResourceRetentionConfig) -> Self {
        Self::new_with(Some(retention)).await
    }

    /// Each transaction takes the retention configuration of its catalog,
    /// [`Self::catalog_with`]
    async fn new_without_retention() -> Self {
        Self::new_with(None).await
    }

    async fn new_with(retention: Option<ResourceRetentionConfig>) -> Self {
        let base = ServiceLayerTestHarness::new_with_opts(ServiceLayerTestHarnessOpts {
            base_opts: BaseResourceServiceHarnessOpts {
                outbox_provider: OutboxProvider::Dispatching,
                transactional: true,
                retention,
                ..Default::default()
            },
            ..Default::default()
        });
        let account = base.ensure_test_account("test-owner").await;

        Self { base, account }
    }

    /// The harness catalog with a retention configuration
    fn catalog_with(&self, retention: ResourceRetentionConfig) -> Catalog {
        let mut b = CatalogBuilder::new_chained(self.catalog());
        b.add_value(retention);
        b.build()
    }

    async fn apply_and_request_deletion(&self, name: &str) -> ResourceID {
        self.apply_and_request_deletion_in(self.catalog(), name)
            .await
    }

    async fn apply_and_request_deletion_in(&self, catalog: &Catalog, name: &str) -> ResourceID {
        let id = self.apply_and_get_id(&self.account, name).await;
        in_committed_transaction(catalog, |catalog| async move {
            Self::primitives_in(&catalog).request_deletion(&[id]).await
        })
        .await
        .unwrap();
        id
    }

    async fn complete_deletion(&self, catalog: &Catalog, id: ResourceID) {
        let transition = in_committed_transaction(catalog, |catalog| async move {
            Self::primitives_in(&catalog)
                .complete_deletion(&[ResourceDeletionCompletion {
                    id,
                    expected_status_generation: None,
                }])
                .await
        })
        .await
        .unwrap();
        assert_eq!(transition.transitioned.len(), 1);
    }

    async fn deleted_at(&self, id: &ResourceID) -> DateTime<Utc> {
        self.catalog()
            .get_one::<dyn ResourceAggregateLoader<TestResource>>()
            .unwrap()
            .load(id)
            .await
            .unwrap()
            .headers()
            .deleted_at
            .unwrap()
    }

    async fn purge_at(&self, id: &ResourceID) -> Option<DateTime<Utc>> {
        let id = *id;
        in_committed_transaction(self.catalog(), |catalog| async move {
            catalog
                .get_one::<dyn ResourceRepository>()
                .unwrap()
                .find_resources_due_for_purge(DateTime::<Utc>::MAX_UTC, usize::MAX)
                .await
                .unwrap()
                .into_iter()
                .find(|resource| resource.id == id)
                .map(|resource| resource.purge_at)
        })
        .await
    }

    fn primitives_in(catalog: &Catalog) -> Arc<dyn ResourceLifecyclePrimitives<TestResource>> {
        catalog.get_one().unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
