// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use dill::*;
use internal_error::InternalError;
use kamu_core::{
    DatasetLifecycleMessage,
    DatasetLifecycleMessageCreated,
    DatasetLifecycleMessageDeleted,
    DatasetVisibility,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};
use opendatafabric as odf;
use tokio::sync::{RwLock, RwLockReadGuard};

use crate::{DatasetResource, UserActor, MESSAGE_CONSUMER_KAMU_AUTH_OSO_OSO_RESOURCE_SERVICE};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type EntityId = String;

#[derive(Debug, Default)]
struct State {
    user_actor_map: HashMap<EntityId, UserActor>,
    dataset_resource_map: HashMap<EntityId, DatasetResource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OsoResourceServiceInMem {
    state: RwLock<State>,
}

// TODO: Private Datasets: Add ReBAC-specific messages to update properties at
//       runtime
#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_AUTH_OSO_OSO_RESOURCE_SERVICE,
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE],
    delivery: MessageDeliveryMechanism::Transactional,
 })]
impl OsoResourceServiceInMem {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(State::default()),
        }
    }

    pub async fn initialize(
        &self,
        dataset_resources: impl IntoIterator<Item = (EntityId, DatasetResource)>,
        user_actors: impl IntoIterator<Item = (EntityId, UserActor)>,
    ) {
        let mut writable_state = self.state.write().await;

        writable_state
            .dataset_resource_map
            .extend(dataset_resources);

        writable_state.user_actor_map.extend(user_actors);
    }

    pub async fn user_dataset_pair(
        &self,
        dataset_id: &odf::DatasetID,
        maybe_account_id: Option<&odf::AccountID>,
    ) -> (Option<UserActor>, Option<DatasetResource>) {
        let readable_state = self.state.read().await;

        let user_actor = self.user_actor(&readable_state, maybe_account_id);
        let dataset_resource = self.dataset_resource(&readable_state, dataset_id);

        (user_actor, dataset_resource)
    }

    fn anonymous_user_actor() -> UserActor {
        UserActor::new("", true, false)
    }

    fn user_actor(
        &self,
        readable_state: &RwLockReadGuard<'_, State>,
        maybe_account_id: Option<&odf::AccountID>,
    ) -> Option<UserActor> {
        if let Some(account_id) = maybe_account_id {
            let account_entity_id = account_id.as_did_str().to_stack_string();

            readable_state
                .user_actor_map
                .get(account_entity_id.as_str())
                .cloned()
        } else {
            Some(Self::anonymous_user_actor())
        }
    }

    fn dataset_resource(
        &self,
        readable_state: &RwLockReadGuard<'_, State>,
        dataset_id: &odf::DatasetID,
    ) -> Option<DatasetResource> {
        let entity_id = dataset_id.as_did_str().to_stack_string();

        readable_state
            .dataset_resource_map
            .get(entity_id.as_str())
            .cloned()
    }

    async fn handle_dataset_lifecycle_created_message(
        &self,
        message: &DatasetLifecycleMessageCreated,
    ) -> Result<(), InternalError> {
        let mut writable_state = self.state.write().await;

        let entity_id = message.dataset_id.to_string();
        let dataset_resource = DatasetResource::new(
            &message.owner_account_id,
            message.dataset_visibility == DatasetVisibility::Public,
        );

        let had_duplicate = writable_state
            .dataset_resource_map
            .insert(entity_id, dataset_resource)
            .is_some();

        if had_duplicate {
            tracing::warn!("There was a duplicate dataset resource");
        }

        Ok(())
    }

    async fn handle_dataset_lifecycle_deleted_message(
        &self,
        message: &DatasetLifecycleMessageDeleted,
    ) -> Result<(), InternalError> {
        let mut writable_state = self.state.write().await;

        let entity_id = message.dataset_id.to_string();

        let was_deletion = writable_state
            .dataset_resource_map
            .remove(&entity_id)
            .is_some();

        if was_deletion {
            tracing::warn!("Dataset resource not found");
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
impl MessageConsumer for OsoResourceServiceInMem {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for OsoResourceServiceInMem {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "OsoResourceServiceInMem[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Created(message) => {
                self.handle_dataset_lifecycle_created_message(message).await
            }

            DatasetLifecycleMessage::Deleted(message) => {
                self.handle_dataset_lifecycle_deleted_message(message).await
            }

            DatasetLifecycleMessage::DependenciesUpdated(_)
            | DatasetLifecycleMessage::Renamed(_) => {
                // No action required
                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
