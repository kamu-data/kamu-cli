// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FlowBinding {
    pub flow_type: String,
    pub scope: FlowScope,
}

impl FlowBinding {
    pub fn for_dataset(dataset_id: odf::DatasetID, flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::Dataset { dataset_id },
        }
    }

    pub fn for_system(flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::System,
        }
    }

    pub fn for_webhook_subscription(
        subscription_id: uuid::Uuid,
        dataset_id: Option<odf::DatasetID>,
        flow_type: &str,
    ) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::WebhookSubscription {
                subscription_id,
                dataset_id,
            },
        }
    }

    pub fn dataset_id(&self) -> Option<&odf::DatasetID> {
        match &self.scope {
            FlowScope::Dataset { dataset_id }
            | FlowScope::WebhookSubscription {
                dataset_id: Some(dataset_id),
                ..
            } => Some(dataset_id),
            _ => None,
        }
    }

    pub fn webhook_subscription_id(&self) -> Option<uuid::Uuid> {
        match &self.scope {
            FlowScope::WebhookSubscription {
                subscription_id, ..
            } => Some(*subscription_id),
            _ => None,
        }
    }

    pub fn get_dataset_id_or_die(&self) -> Result<odf::DatasetID, InternalError> {
        self.dataset_id().cloned().ok_or_else(|| {
            InternalError::new("Expecting dataset or webhook flow binding scope with dataset_id")
        })
    }
}

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowBindingStream<'a> = std::pin::Pin<
    Box<dyn tokio_stream::Stream<Item = Result<FlowBinding, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
