// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_core::*;
use kamu_datasets::DatasetEnvVar;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FetchService {
    pub(crate) async fn fetch_mqtt(
        &self,
        dataset_handle: &odf::DatasetHandle,
        fetch: &odf::metadata::FetchStepMqtt,
        target_path: &Path,
        dataset_env_vars: &HashMap<String, DatasetEnvVar>,
        listener: &Arc<dyn FetchProgressListener>,
    ) -> Result<FetchResult, PollingIngestError> {
        use std::io::Write as _;

        use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};

        // TODO: Reconsider assignment of client identity on which QoS session data
        // rentention is based
        let client_id = format!("kamu-ingest-{}", dataset_handle.id.as_multibase());

        let mut opts = MqttOptions::new(client_id, &fetch.host, u16::try_from(fetch.port).unwrap());
        opts.set_clean_session(false);

        // TODO: Reconsider password propagation
        if let (Some(username), Some(password)) = (&fetch.username, &fetch.password) {
            let password = self.template_string(password, dataset_env_vars)?;
            opts.set_credentials(username, password);
        }

        tracing::debug!("Connecting to the MQTT broker and subscribing to the topic");

        let (client, mut event_loop) = AsyncClient::new(opts, 1000);
        client
            .subscribe_many(fetch.topics.clone().into_iter().map(|s| {
                rumqttc::SubscribeFilter::new(
                    s.path,
                    match s.qos {
                        None | Some(odf::metadata::MqttQos::AtMostOnce) => QoS::AtMostOnce,
                        Some(odf::metadata::MqttQos::AtLeastOnce) => QoS::AtLeastOnce,
                        Some(odf::metadata::MqttQos::ExactlyOnce) => QoS::ExactlyOnce,
                    },
                )
            }))
            .await
            .int_err()?;

        let mut fetched_bytes = 0;
        let mut fetched_records = 0;
        let mut file = std::fs::File::create(target_path).int_err()?;

        let max_records = self.source_config.target_records_per_slice;
        let poll_timeout =
            std::time::Duration::from_millis(self.mqtt_source_config.broker_idle_timeout_ms);

        loop {
            // Limit number of records read if they keep flowing faster that we timeout
            // TODO: Manual ACKs to prevent lost records
            if fetched_records >= max_records {
                break;
            }

            if let Ok(poll) = tokio::time::timeout(poll_timeout, event_loop.poll()).await {
                match poll.int_err()? {
                    Event::Incoming(Packet::Publish(publish)) => {
                        // TODO: Assuming that payload is JSON and formatting it as line-delimited
                        if fetched_bytes != 0 {
                            file.write_all(b" ").int_err()?;
                        }
                        let json = std::str::from_utf8(&publish.payload).int_err()?.trim();
                        file.write_all(json.as_bytes()).int_err()?;

                        fetched_bytes += publish.payload.len() as u64 + 1;
                        fetched_records += 1;

                        listener.on_progress(&FetchProgress {
                            fetched_bytes,
                            total_bytes: TotalBytes::Unknown,
                        });
                    }
                    event => tracing::debug!(?event, "Received"),
                }
            } else {
                break;
            }
        }

        tracing::debug!(
            fetched_bytes,
            fetched_records,
            "Disconnecting from the MQTT broker"
        );

        file.flush().int_err()?;

        if fetched_bytes == 0 {
            Ok(FetchResult::UpToDate)
        } else {
            // TODO: Store last packet ID / client ID in the source state?
            Ok(FetchResult::Updated(FetchResultUpdated {
                source_state: None,
                source_event_time: None,
                has_more: false,
                zero_copy_path: None,
            }))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
