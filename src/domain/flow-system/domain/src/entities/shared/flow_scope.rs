// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use lang_utils::IntoOwned;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum FlowScope {
    Dataset {
        dataset_id: odf::DatasetID,
    },
    System,
    WebhookSubscription {
        subscription_id: uuid::Uuid,
        dataset_id: Option<odf::DatasetID>,
    },
}

impl FlowScope {
    #[inline]
    pub fn for_dataset(dataset_id: impl IntoOwned<odf::DatasetID>) -> Self {
        FlowScope::Dataset {
            dataset_id: dataset_id.into_owned(),
        }
    }

    #[inline]
    pub fn for_webhook_subscription(
        subscription_id: uuid::Uuid,
        dataset_id: Option<impl IntoOwned<odf::DatasetID>>,
    ) -> Self {
        FlowScope::WebhookSubscription {
            subscription_id,
            dataset_id: dataset_id.map(IntoOwned::into_owned),
        }
    }

    pub fn dataset_id(&self) -> Option<&odf::DatasetID> {
        match self {
            FlowScope::Dataset { dataset_id } => Some(dataset_id),
            FlowScope::WebhookSubscription { dataset_id, .. } => dataset_id.as_ref(),
            FlowScope::System => None,
        }
    }

    pub fn webhook_subscription_id(&self) -> Option<uuid::Uuid> {
        match self {
            FlowScope::WebhookSubscription {
                subscription_id, ..
            } => Some(*subscription_id),
            FlowScope::Dataset { .. } | FlowScope::System => None,
        }
    }

    #[inline]
    pub fn get_dataset_id_or_die(&self) -> Result<odf::DatasetID, InternalError> {
        self.dataset_id().cloned().ok_or_else(|| {
            InternalError::new("Expecting dataset or webhook flow binding scope with dataset_id")
        })
    }

    #[inline]
    pub fn get_webhook_subscription_id_or_die(&self) -> Result<uuid::Uuid, InternalError> {
        self.webhook_subscription_id().ok_or_else(|| {
            InternalError::new("Expecting webhook flow binding scope with subscription_id")
        })
    }

    pub fn matches_query(&self, query: &FlowScopeQuery) -> bool {
        if let Some((_, type_values)) = query.attributes.iter().find(|(key, _)| key == "type") {
            let self_type = match self {
                FlowScope::Dataset { .. } => "Dataset",
                FlowScope::System => "System",
                FlowScope::WebhookSubscription { .. } => "WebhookSubscription",
            };
            if !type_values.contains(&self_type.to_string()) {
                return false;
            }
        }

        query
            .attributes
            .iter()
            .filter(|(key, _)| key != "type")
            .all(|(key, values)| match self {
                FlowScope::Dataset { dataset_id } => {
                    key == "dataset_id" && values.contains(&dataset_id.to_string())
                }
                FlowScope::System => false,
                FlowScope::WebhookSubscription {
                    subscription_id,
                    dataset_id,
                } => {
                    (key == "subscription_id" && values.contains(&subscription_id.to_string()))
                        || (key == "dataset_id"
                            && dataset_id
                                .as_ref()
                                .is_some_and(|id| values.contains(&id.to_string())))
                }
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowScopeQuery {
    pub attributes: Vec<(String, Vec<String>)>,
}

impl FlowScopeQuery {
    pub fn build_for_single_dataset(dataset_id: &odf::DatasetID) -> Self {
        Self {
            attributes: vec![("dataset_id".to_string(), vec![dataset_id.to_string()])],
        }
    }

    pub fn build_for_multiple_datasets(dataset_ids: &[&odf::DatasetID]) -> Self {
        Self {
            attributes: vec![(
                "dataset_id".to_string(),
                dataset_ids.iter().map(ToString::to_string).collect(),
            )],
        }
    }

    pub fn build_for_system_scope() -> Self {
        Self {
            attributes: vec![("type".to_string(), vec!["System".to_string()])],
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
