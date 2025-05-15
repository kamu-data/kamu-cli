// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::{Projection, ProjectionError, ProjectionEvent};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookSubscriptionState {
    id: WebhookSubscriptionID,
    target_url: url::Url,
    label: WebhookSubscriptionLabel,
    dataset_id: Option<odf::DatasetID>,
    status: WebhookSubscriptionStatus,
    event_types: Vec<WebhookEventType>,
    secret: WebhookSubscriptionSecret,
    created_at: DateTime<Utc>,
}

impl WebhookSubscriptionState {
    pub fn id(&self) -> WebhookSubscriptionID {
        self.id
    }

    pub fn status(&self) -> WebhookSubscriptionStatus {
        self.status
    }

    pub fn target_url(&self) -> &url::Url {
        &self.target_url
    }

    pub fn label(&self) -> &WebhookSubscriptionLabel {
        &self.label
    }

    pub fn dataset_id(&self) -> Option<&odf::DatasetID> {
        self.dataset_id.as_ref()
    }

    pub fn event_types(&self) -> &[WebhookEventType] {
        &self.event_types
    }

    pub fn secret(&self) -> &WebhookSubscriptionSecret {
        &self.secret
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Projection for WebhookSubscriptionState {
    type Query = WebhookSubscriptionID;
    type Event = WebhookSubscriptionEvent;

    fn apply(state: Option<Self>, event: Self::Event) -> Result<Self, ProjectionError<Self>> {
        use WebhookSubscriptionEvent as E;

        match (state, event) {
            (None, event) => match event {
                E::Created(WebhookSubscriptionEventCreated {
                    event_time,
                    subscription_id,
                    dataset_id,
                    event_types,
                    label,
                    target_url,
                    secret,
                    ..
                }) => Ok(Self {
                    id: subscription_id,
                    dataset_id,
                    event_types,
                    label,
                    target_url,
                    secret,
                    status: WebhookSubscriptionStatus::Unverified,
                    created_at: event_time,
                }),
                _ => Err(ProjectionError::new(None, event)),
            },
            (Some(s), event) => {
                assert_eq!(&s.id, event.subscription_id());

                match &event {
                    E::Created(_) => Err(ProjectionError::new(Some(s), event)),

                    E::Enabled(_) => {
                        if s.status == WebhookSubscriptionStatus::Unverified {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Enabled,
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::Paused(_) => {
                        if s.status == WebhookSubscriptionStatus::Enabled {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Paused,
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::Resumed(_) => {
                        if s.status == WebhookSubscriptionStatus::Paused {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Enabled,
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::MarkedUnreachable(_) => match s.status {
                        WebhookSubscriptionStatus::Enabled | WebhookSubscriptionStatus::Paused => {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Unreachable,
                                ..s
                            })
                        }
                        _ => Err(ProjectionError::new(Some(s), event)),
                    },

                    E::Reactivated(_) => {
                        if s.status == WebhookSubscriptionStatus::Unreachable {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Enabled,
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::SecretRotated(WebhookSubscriptionEventSecretRotated {
                        new_secret, ..
                    }) => {
                        if s.status != WebhookSubscriptionStatus::Removed {
                            Ok(WebhookSubscriptionState {
                                secret: new_secret.clone(),
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::Modified(WebhookSubscriptionEventModified {
                        new_target_url,
                        new_label,
                        new_event_types,
                        ..
                    }) => {
                        if s.status != WebhookSubscriptionStatus::Removed {
                            Ok(WebhookSubscriptionState {
                                label: new_label.clone(),
                                event_types: new_event_types.clone(),
                                target_url: new_target_url.clone(),
                                ..s
                            })
                        } else {
                            Err(ProjectionError::new(Some(s), event))
                        }
                    }

                    E::Removed(_) => {
                        if s.status == WebhookSubscriptionStatus::Removed {
                            Ok(s) // idempotent DELETE
                        } else {
                            Ok(WebhookSubscriptionState {
                                status: WebhookSubscriptionStatus::Removed,
                                ..s
                            })
                        }
                    }
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ProjectionEvent<WebhookSubscriptionID> for WebhookSubscriptionEvent {
    fn matches_query(&self, query: &WebhookSubscriptionID) -> bool {
        self.subscription_id() == query
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
