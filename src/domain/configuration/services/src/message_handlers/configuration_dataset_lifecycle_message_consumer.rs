// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use dill::{Catalog, component, interface, meta};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_configuration::{
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    SecretSetResource,
    VariableSetResource,
};
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ResourceCrudDispatcher,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceHeadersExt,
    ResourceHeadersInputExt,
    ResourceID,
    ResourceName,
    ResourceRepository,
    ResourceSchemaProvider,
    ResourceSnapshot,
    TypeRef,
    TypeUri,
};
use kamu_resources_services::ResourceDispatcherFactory;
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionMode,
};

use crate::DatasetEnvVarMutationAdapterImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MESSAGE_CONSUMER_KAMU_CONFIGURATION_DATASET_LIFECYCLE_HANDLER: &str =
    "dev.kamu.domain.configuration.ConfigurationDatasetLifecycleHandler";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Retracts the `legacy-config-target-dataset` association when its dataset is
/// deleted, so no config resource is left pointing at a dataset that is gone.
///
/// Auto-managed sets (named for this dataset) are deleted outright; anything
/// else carrying the label keeps its spec and loses only the label. The sweep
/// spans every account: a dead link must not survive because it lives in
/// someone else's namespace, and the owner is unresolvable here anyway.
#[component(pub)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_CONFIGURATION_DATASET_LIFECYCLE_HANDLER,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
    ],
    consumption_mode: MessageConsumptionMode::TransactionalWrapped,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
pub struct ConfigurationDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ConfigurationDatasetLifecycleMessageConsumer {
    /// One schema's share of the cleanup. Both kinds take the identical shape,
    /// so the two calls differ only by schema and well-known name.
    async fn cleanup_schema(
        target_catalog: &Catalog,
        schema: &TypeUri,
        dataset_id: &odf::DatasetID,
        legacy_name: &ResourceName,
    ) -> Result<(), InternalError> {
        let resource_repo = target_catalog
            .get_one::<dyn ResourceRepository>()
            .map_err(ErrorIntoInternal::int_err)?;

        let ids = resource_repo
            .find_resource_ids_by_schema_and_label_any_account(
                schema,
                RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
                &dataset_id.as_did_str().to_string(),
            )
            .await?;

        if ids.is_empty() {
            return Ok(());
        }

        // Skips anything concurrently deleted, so this may be shorter than
        // `ids` -- in which case there is nothing left to clean up.
        let snapshots = resource_repo
            .find_resource_snapshots_by_schema_and_ids(schema, &ids)
            .await?;

        // Full derived name, never a `legacy-vars-` prefix: a set named after
        // some *other* dataset is not ours to delete.
        let (auto_managed, user_authored): (Vec<_>, Vec<_>) = snapshots
            .into_iter()
            .partition(|snapshot| snapshot.headers.name == *legacy_name);

        let dispatcher = ResourceDispatcherFactory::crud_dispatcher_for_trusted_schema_in(
            target_catalog,
            schema.as_str(),
        )?;

        Self::delete_auto_managed(&dispatcher, auto_managed, dataset_id).await?;
        Self::strip_dangling_label(&dispatcher, user_authored, dataset_id).await?;

        Ok(())
    }

    /// Grouped by owner because `account_id` here is an ownership scope, not an
    /// actor claim -- the use case rejects ids owned by anyone else.
    ///
    /// Projection GC is inherited: the resulting `Deleted` message is already
    /// handled by `ConfigurationResourceLifecycleMessageConsumer`.
    async fn delete_auto_managed(
        dispatcher: &Arc<dyn ResourceCrudDispatcher>,
        snapshots: Vec<ResourceSnapshot>,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        if snapshots.is_empty() {
            return Ok(());
        }

        tracing::info!(
            dataset_id = %dataset_id,
            resource_count = snapshots.len(),
            schema = %snapshots[0].schema,
            "Deleting auto-managed config resources of a deleted dataset"
        );

        let mut ids_by_owner: HashMap<odf::AccountID, Vec<ResourceID>> = HashMap::new();
        for snapshot in snapshots {
            ids_by_owner
                .entry(snapshot.headers.account.did.clone())
                .or_default()
                .push(snapshot.id);
        }

        for (account_id, ids) in ids_by_owner {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest { account_id, ids })
                .await
                // A foreign-owner rejection would mean a snapshot disagrees
                // about its own owner: integrity bug, not a user denial.
                .int_err()?;
        }

        Ok(())
    }

    /// Get-modify-apply, which stays headers-only in practice: an unchanged
    /// spec short-circuits, and `HeadersUpdated` schedules no reconciliation.
    ///
    /// The exception is a `SecretSet` still in the migration's `aes256gcm`
    /// form, which the sanitizer upgrades to `jwe` -- a real spec change. Only
    /// reachable here, since auto-managed sets are deleted rather than
    /// stripped.
    async fn strip_dangling_label(
        dispatcher: &Arc<dyn ResourceCrudDispatcher>,
        snapshots: Vec<ResourceSnapshot>,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        // Registered labels are canonicalized to their schema URI on the way
        // in, so the URI form is the only key that can be present.
        let label_key = TypeRef::Uri(TypeUri::new_unchecked(
            RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
        ));

        for snapshot in snapshots {
            // Surfaced because this edits a resource on its owner's behalf,
            // triggered by a dataset they may not own.
            tracing::warn!(
                resource_id = %snapshot.id,
                resource_name = %snapshot.headers.name,
                account_id = %snapshot.headers.account.did,
                dataset_id = %dataset_id,
                "Retracting dangling legacy-config-target-dataset label from a user-authored \
                 resource: its target dataset was deleted"
            );

            let mut headers = snapshot.headers.into_input();
            headers.remove_label(&label_key);

            let decision = dispatcher
                .apply(ResourceCrudDispatcherApplyRequest {
                    id: Some(snapshot.id),
                    headers,
                    // Verbatim: the dispatcher decodes it back per type, so
                    // both kinds share this path without naming a spec type.
                    spec: snapshot.spec,
                })
                .await
                .int_err()?;

            // A rejection means an already-persisted resource no longer
            // validates -- a data-integrity problem, not user input.
            if let ApplyManifestApplicationDecision::Rejected(rejection) = decision {
                return Err(format!(
                    "Stripping dangling legacy-config-target-dataset label from resource {} was \
                     rejected: {rejection:?}",
                    snapshot.id,
                )
                .int_err());
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MessageConsumer for ConfigurationDatasetLifecycleMessageConsumer {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for ConfigurationDatasetLifecycleMessageConsumer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "ConfigurationDatasetLifecycleMessageConsumer[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        target_catalog: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        let DatasetLifecycleMessage::Deleted(deleted_message) = message else {
            return Ok(());
        };

        let dataset_id = &deleted_message.dataset_id;

        Self::cleanup_schema(
            target_catalog,
            VariableSetResource::schema(),
            dataset_id,
            &DatasetEnvVarMutationAdapterImpl::legacy_variable_set_resource_name(dataset_id),
        )
        .await?;

        Self::cleanup_schema(
            target_catalog,
            SecretSetResource::schema(),
            dataset_id,
            &DatasetEnvVarMutationAdapterImpl::legacy_secret_set_resource_name(dataset_id),
        )
        .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
