// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_task_system as ts;

use crate::{
    WebhookDelivery,
    WebhookEventId,
    WebhookRequest,
    WebhookResponse,
    WebhookSubscriptionId,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(sqlx::FromRow)]
pub struct WebhookDeliveryRecord {
    pub task_id: i64,
    pub attempt_number: i32,
    pub event_id: uuid::Uuid,
    pub subscription_id: uuid::Uuid,
    pub request_headers: serde_json::Value,
    pub requested_at: DateTime<Utc>,
    pub response_code: Option<i16>,
    pub response_body: Option<String>,
    pub response_headers: Option<serde_json::Value>,
    pub response_at: Option<DateTime<Utc>>,
}

#[cfg(feature = "sqlx")]
impl WebhookDeliveryRecord {
    pub fn serialize_http_headers(
        headers: &http::HeaderMap,
    ) -> Result<serde_json::Value, InternalError> {
        let serialized_headers: Vec<(String, String)> = headers
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
            .collect();

        serde_json::to_value(serialized_headers).map_err(ErrorIntoInternal::int_err)
    }

    pub fn deserialize_http_headers(
        headers: serde_json::Value,
    ) -> Result<http::HeaderMap, InternalError> {
        let deserialized_headers: Vec<(String, String)> =
            serde_json::from_value(headers).map_err(ErrorIntoInternal::int_err)?;

        use std::str::FromStr;

        let mut res_headers = http::HeaderMap::new();
        for (header_name, value) in deserialized_headers {
            let http_header = http::header::HeaderName::from_str(&header_name)
                .map_err(ErrorIntoInternal::int_err)?;
            let http_header_value =
                http::header::HeaderValue::from_str(&value).map_err(ErrorIntoInternal::int_err)?;
            res_headers.insert(http_header, http_header_value);
        }

        Ok(res_headers)
    }

    pub fn try_into_webhook_delivery(self) -> Result<WebhookDelivery, InternalError> {
        let request = WebhookRequest {
            started_at: self.requested_at,
            headers: Self::deserialize_http_headers(self.request_headers)?,
        };

        let response = if self.response_code.is_some() {
            let response_code =
                http::StatusCode::from_u16(u16::try_from(self.response_code.unwrap()).unwrap())
                    .map_err(ErrorIntoInternal::int_err)?;

            let response_headers = Self::deserialize_http_headers(self.response_headers.unwrap())?;

            let response_body = self.response_body.unwrap();
            let response_at = self.response_at.unwrap();

            Some(WebhookResponse {
                status_code: response_code,
                headers: response_headers,
                body: response_body,
                finished_at: response_at,
            })
        } else {
            None
        };

        Ok(WebhookDelivery {
            task_attempt_id: ts::TaskAttemptID::new(
                ts::TaskID::try_from(self.task_id).unwrap(),
                u32::try_from(self.attempt_number).unwrap(),
            ),
            webhook_subscription_id: WebhookSubscriptionId::new(self.subscription_id),
            webhook_event_id: WebhookEventId::new(self.event_id),
            request,
            response,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
