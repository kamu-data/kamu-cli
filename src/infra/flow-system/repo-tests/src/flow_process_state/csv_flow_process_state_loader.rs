// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use csv::ReaderBuilder;
use dill::Catalog;
use event_sourcing::EventID;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_flow_system::*;
use kamu_task_system::{TaskError, TaskResult};
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookSubscriptionID};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a single row from the CSV file
#[derive(Debug, serde::Deserialize)]
pub(crate) struct CsvFlowProcessRecord {
    #[serde(rename = "#")]
    pub id: u32,
    pub flow_type: String,
    pub scope_type: String,
    pub dataset_alias: String,
    pub subscription_label: Option<String>,
    pub paused_manual: bool,
    pub stop_policy_kind: String,
    pub stop_policy_data: Option<String>,
    pub consecutive_failures: u32,
    #[serde(deserialize_with = "deserialize_optional_datetime")]
    pub last_success_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_optional_datetime")]
    pub last_failure_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_optional_datetime")]
    #[allow(dead_code)]
    pub last_attempt_at: Option<DateTime<Utc>>,
    #[serde(deserialize_with = "deserialize_optional_datetime")]
    pub next_planned_at: Option<DateTime<Utc>>,
    pub auto_stopped_reason: Option<String>,
    #[serde(deserialize_with = "deserialize_optional_datetime")]
    pub auto_stopped_at: Option<DateTime<Utc>>,
    pub effective_state: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Helper for loading flow process state data from CSV files and populating
/// repositories
pub(crate) struct CsvFlowProcessStateLoader {
    flow_process_repository: Arc<dyn FlowProcessStateRepository>,
    flow_process_query: Arc<dyn FlowProcessStateQuery>,
    event_id_counter: i64,
}

impl CsvFlowProcessStateLoader {
    pub fn new(catalog: &Catalog) -> Self {
        Self {
            flow_process_repository: catalog.get_one::<dyn FlowProcessStateRepository>().unwrap(),
            flow_process_query: catalog.get_one::<dyn FlowProcessStateQuery>().unwrap(),
            event_id_counter: 1,
        }
    }

    /// Load CSV data and populate the repository
    pub async fn load_from_csv(&mut self, csv_content: &str) {
        let records = self.parse_csv(csv_content);

        for record in records {
            self.populate_process_state(record).await;
        }
    }

    /// Load CSV data from the hardcoded file path and populate the repository
    pub async fn populate_from_csv(&mut self) {
        let csv_path = include_str!("flow_process_states.csv");
        self.load_from_csv(csv_path).await;
    }

    /// Parse CSV content into structured records
    fn parse_csv(&self, csv_content: &str) -> Vec<CsvFlowProcessRecord> {
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv_content.as_bytes());

        let mut records = Vec::new();
        for result in reader.deserialize() {
            let record: CsvFlowProcessRecord = result.expect("Failed to parse CSV record");
            records.push(record);
        }

        records
    }

    /// Creates a binding from a CSV record
    fn create_flow_binding(&self, record: &CsvFlowProcessRecord) -> FlowBinding {
        // Generate dataset ID from alias using seeded construction
        let dataset_id = odf::DatasetID::new_seeded_ed25519(record.dataset_alias.as_bytes());

        let flow_type = self.map_flow_type(&record.flow_type);
        let scope = match record.scope_type.as_str() {
            "Dataset" => FlowScopeDataset::make_scope(&dataset_id),
            "WebhookSubscription" => {
                let subscription_id = if let Some(ref sub_label) = record.subscription_label {
                    // Generate a deterministic UUID based on subscription label
                    let uuid_bytes = format!("sub-{sub_label}",).into_bytes();
                    let mut uuid_array = [0u8; 16];
                    for (i, byte) in uuid_bytes.iter().take(16).enumerate() {
                        uuid_array[i] = *byte;
                    }
                    WebhookSubscriptionID::new(Uuid::from_bytes(uuid_array))
                } else {
                    panic!("Missing subscription_label for subscription scope");
                };

                let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
                FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&dataset_id))
            }
            _ => {
                panic!("Unknown scope type: {}", record.scope_type);
            }
        };

        FlowBinding::new(&flow_type, scope)
    }

    /// Populate flow process state from a CSV record
    async fn populate_process_state(&mut self, record: CsvFlowProcessRecord) {
        // Create flow binding
        let flow_binding = self.create_flow_binding(&record);

        // Parse stop policy
        let stop_policy = self.parse_stop_policy(&record);

        // Generate event ID
        let trigger_event_id = self.next_event_id();

        // Insert the process
        self.flow_process_repository
            .upsert_process_state_on_trigger_event(
                trigger_event_id,
                flow_binding.clone(),
                record.paused_manual,
                stop_policy,
            )
            .await
            .expect("Failed to insert flow process");

        // Apply timing-based results if any
        self.apply_results_from_record(&flow_binding, &record).await;

        // Apply auto-stop result if needed
        self.apply_auto_stop_from_record(&flow_binding, &record)
            .await;

        // Verify effective state matches
        let current_state = self
            .flow_process_query
            .try_get_process_state(&flow_binding)
            .await
            .expect("Failed to get flow process state")
            .expect("Flow process state not found");
        assert_eq!(
            current_state.effective_state().to_string(),
            record.effective_state,
            "Effective state mismatch for record ID {}",
            record.id
        );
    }

    /// Map CSV flow types to actual flow type constants
    fn map_flow_type(&self, csv_flow_type: &str) -> String {
        match csv_flow_type {
            "INGEST" => FLOW_TYPE_DATASET_INGEST.to_string(),
            "EXECUTE_TRANSFORM" => FLOW_TYPE_DATASET_TRANSFORM.to_string(),
            "WEBHOOK_DELIVER" => FLOW_TYPE_WEBHOOK_DELIVER.to_string(),
            _ => panic!("Unknown flow type: {csv_flow_type}",),
        }
    }

    /// Parse stop policy from CSV record
    fn parse_stop_policy(&self, record: &CsvFlowProcessRecord) -> FlowTriggerStopPolicy {
        match record.stop_policy_kind.as_str() {
            "never" => FlowTriggerStopPolicy::Never,
            "after_consecutive_failures" => {
                if let Some(ref policy_data) = record.stop_policy_data {
                    let data: serde_json::Value = serde_json::from_str(policy_data)
                        .expect("Failed to parse stop policy JSON");
                    if let Some(max_failures) =
                        data.get("maxFailures").and_then(serde_json::Value::as_u64)
                    {
                        let failures_count =
                            ConsecutiveFailuresCount::try_new(u32::try_from(max_failures).unwrap())
                                .expect("Invalid consecutive failures count");
                        FlowTriggerStopPolicy::AfterConsecutiveFailures { failures_count }
                    } else {
                        panic!("Invalid stop policy data: missing maxFailures");
                    }
                } else {
                    panic!("Missing stop policy data for after_consecutive_failures policy");
                }
            }
            _ => panic!("Unknown stop policy kind: {}", record.stop_policy_kind),
        }
    }

    /// Apply success/failure results based on record timing data
    async fn apply_results_from_record(
        &mut self,
        flow_binding: &FlowBinding,
        record: &CsvFlowProcessRecord,
    ) {
        // Determine the sequence of events from the record data
        let mut events = Vec::new();

        // Add success event if we have last_success_at
        if let Some(success_time) = record.last_success_at {
            events.push((success_time, true));
        }

        // Add failure events if we have last_failure_at and consecutive failures
        if let Some(failure_time) = record.last_failure_at {
            // Generate failure events based on consecutive_failures count
            for i in 0..record.consecutive_failures {
                // Space out failures over time, ending at last_failure_at
                let failure_offset = chrono::Duration::hours(i64::from(i));
                let event_time = failure_time - failure_offset;
                events.push((event_time, false));
            }
        }

        // Sort events by time
        events.sort_by_key(|(time, _)| *time);

        // Apply events in chronological order
        for (event_time, success) in events {
            let next_planned = if success {
                record
                    .next_planned_at
                    .or_else(|| Some(event_time + chrono::Duration::hours(1)))
            } else {
                record.next_planned_at
            };

            let flow_event_id = self.next_event_id();

            // Apply the appropriate flow result
            let flow_outcome = if success {
                FlowOutcome::Success(TaskResult::empty())
            } else {
                FlowOutcome::Failed(TaskError::empty_recoverable())
            };

            self.flow_process_repository
                .apply_flow_result(flow_event_id, flow_binding, &flow_outcome, event_time)
                .await
                .expect("Failed to apply flow result");

            // Then schedule if needed
            if let Some(planned_at) = next_planned {
                let schedule_event_id = self.next_event_id();
                self.flow_process_repository
                    .on_flow_scheduled(schedule_event_id, flow_binding, planned_at)
                    .await
                    .expect("Failed to schedule flow");
            }
        }
    }

    /// Apply auto-stop result based on record data
    async fn apply_auto_stop_from_record(
        &mut self,
        flow_binding: &FlowBinding,
        record: &CsvFlowProcessRecord,
    ) {
        if let Some(ref auto_stopped_reason) = record.auto_stopped_reason {
            if let Some(auto_stopped_at) = record.auto_stopped_at {
                match auto_stopped_reason.as_str() {
                    "unrecoverable_failure" => {
                        // For unrecoverable failure, we need to apply an unrecoverable failure
                        // at the auto_stopped_at time to set the auto-stop state
                        let event_id = self.next_event_id();
                        self.flow_process_repository
                            .apply_flow_result(
                                event_id,
                                flow_binding,
                                &FlowOutcome::Failed(TaskError::empty_unrecoverable()),
                                auto_stopped_at,
                            )
                            .await
                            .expect("Failed to apply unrecoverable failure");
                    }
                    "stop_policy" => {
                        // For stop policy, the auto-stop should have been
                        // triggered by the consecutive
                        // failures. No additional action needed.
                    }
                    _ => panic!("Unknown auto stopped reason: {auto_stopped_reason}",),
                }
            }
        }
    }

    /// Get next sequential event ID
    fn next_event_id(&mut self) -> EventID {
        let id = EventID::new(self.event_id_counter);
        self.event_id_counter += 1;
        id
    }
}

/// Custom deserializer for optional datetime values from CSV
fn deserialize_optional_datetime<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;
    if s == "NULL" || s.is_empty() {
        Ok(None)
    } else {
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| Some(dt.with_timezone(&Utc)))
            .map_err(serde::de::Error::custom)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
