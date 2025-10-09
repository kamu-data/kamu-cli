// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use kamu_accounts::Account as AccountEntity;
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
};
use kamu_adapter_flow_webhook::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};
use kamu_datasets::{DatasetEntryService, DatasetEntryServiceExt};
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{
    Account,
    DatasetFlowProcess,
    DatasetFlowProcessConnection,
    DatasetRequestState,
    WebhookFlowSubProcess,
    WebhookFlowSubProcessConnection,
    build_dataset_id_handle_mapping_from_processes_listing,
    build_webhook_id_subscription_mapping_from_processes_listing,
};
use crate::scalars::FlowProcessGroupRollup;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountFlowProcesses<'a> {
    account: &'a AccountEntity,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountFlowProcesses<'a> {
    const DEFAULT_PER_PAGE: usize = 15;

    #[graphql(skip)]
    pub fn new(account: &'a AccountEntity) -> Self {
        Self { account }
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_primary_rollup, skip_all)]
    pub async fn primary_rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let scope_query = self.build_scope_query(ctx).await?;

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
        let rollup = flow_process_state_query
            .rollup_by_scope(
                scope_query,
                Some(&[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM]),
                None,
            )
            .await?;

        Ok(rollup.into())
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_webhook_rollup, skip_all)]
    pub async fn webhook_rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let scope_query = self.build_scope_query(ctx).await?;

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
        let rollup = flow_process_state_query
            .rollup_by_scope(scope_query, Some(&[FLOW_TYPE_WEBHOOK_DELIVER]), None)
            .await?;

        Ok(rollup.into())
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_full_rollup, skip_all)]
    pub async fn full_rollup(&self, ctx: &Context<'_>) -> Result<FlowProcessGroupRollup> {
        let scope_query = self.build_scope_query(ctx).await?;

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
        let rollup = flow_process_state_query
            .rollup_by_scope(scope_query, None, None)
            .await?;

        Ok(rollup.into())
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_primary_cards, skip_all, fields(?page, ?per_page))]
    pub async fn primary_cards(
        &self,
        ctx: &Context<'_>,
        filters: Option<FlowProcessFilters>,
        ordering: Option<FlowProcessOrdering>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetFlowProcessConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let pagination = PaginationOpts::from_page(page, per_page);

        let listing = self
            .get_process_state_listing(
                ctx,
                &[FLOW_TYPE_DATASET_INGEST, FLOW_TYPE_DATASET_TRANSFORM],
                filters,
                ordering,
                pagination,
            )
            .await?;

        let listing_state = self
            .form_shared_state_from_listing(ctx, &listing, true /* skip webhooks */)
            .await?;

        let dataset_cards: Vec<_> = listing
            .processes
            .into_iter()
            .map(|process_state| listing_state.build_dataset_card(process_state))
            .collect();

        Ok(DatasetFlowProcessConnection::new(
            dataset_cards,
            page,
            per_page,
            listing.total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_webhook_cards, skip_all, fields(?page, ?per_page))]
    pub async fn webhook_cards(
        &self,
        ctx: &Context<'_>,
        filters: Option<FlowProcessFilters>,
        ordering: Option<FlowProcessOrdering>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<WebhookFlowSubProcessConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let pagination = PaginationOpts::from_page(page, per_page);

        let listing = self
            .get_process_state_listing(
                ctx,
                &[FLOW_TYPE_WEBHOOK_DELIVER],
                filters,
                ordering,
                pagination,
            )
            .await?;

        let listing_state = self
            .form_shared_state_from_listing(ctx, &listing, false /* skip webhooks */)
            .await?;

        let webhook_cards: Vec<_> = listing
            .processes
            .into_iter()
            .map(
                |process_state| match process_state.flow_binding().flow_type.as_str() {
                    FLOW_TYPE_WEBHOOK_DELIVER => listing_state.build_webhook_card(process_state),
                    _ => unreachable!("Unsupported flow type"),
                },
            )
            .collect();

        Ok(WebhookFlowSubProcessConnection::new(
            webhook_cards,
            page,
            per_page,
            listing.total_count,
        ))
    }

    #[tracing::instrument(level = "info", name = AccountFlowProcesses_all_cards, skip_all, fields(?page, ?per_page))]
    pub async fn all_cards(
        &self,
        ctx: &Context<'_>,
        filters: Option<FlowProcessFilters>,
        ordering: Option<FlowProcessOrdering>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<AccountFlowProcessCardConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let pagination = PaginationOpts::from_page(page, per_page);

        let listing = self
            .get_process_state_listing(
                ctx,
                &[
                    FLOW_TYPE_DATASET_INGEST,
                    FLOW_TYPE_DATASET_TRANSFORM,
                    FLOW_TYPE_WEBHOOK_DELIVER,
                ],
                filters,
                ordering,
                pagination,
            )
            .await?;

        let listing_state = self
            .form_shared_state_from_listing(ctx, &listing, false /* skip webhooks */)
            .await?;

        let flat_cards: Vec<_> = listing
            .processes
            .into_iter()
            .map(
                |process_state| match process_state.flow_binding().flow_type.as_str() {
                    FLOW_TYPE_DATASET_INGEST | FLOW_TYPE_DATASET_TRANSFORM => {
                        let dataset_card = listing_state.build_dataset_card(process_state);
                        AccountFlowProcessCard::Dataset(dataset_card)
                    }

                    FLOW_TYPE_WEBHOOK_DELIVER => {
                        let webhook_card = listing_state.build_webhook_card(process_state);
                        AccountFlowProcessCard::Webhook(webhook_card)
                    }

                    _ => unreachable!("Unsupported flow type"),
                },
            )
            .collect();

        Ok(AccountFlowProcessCardConnection::new(
            flat_cards,
            page,
            per_page,
            listing.total_count,
        ))
    }

    #[graphql(skip)]
    async fn get_process_state_listing(
        &self,
        ctx: &Context<'_>,
        flow_types: &[&'static str],
        filters: Option<FlowProcessFilters>,
        ordering: Option<FlowProcessOrdering>,
        pagination: PaginationOpts,
    ) -> Result<fs::FlowProcessStateListing> {
        let filters = filters.unwrap_or_default();

        let scope_query = self.build_scope_query(ctx).await?;

        let effective_state_in_converted: Option<Vec<fs::FlowProcessEffectiveState>> = filters
            .effective_state_in
            .as_ref()
            .map(|v| v.iter().map(|s| (*s).into()).collect());

        let filter = fs::FlowProcessListFilter::for_scope(scope_query)
            .for_flow_types(flow_types)
            .with_effective_states_opt(effective_state_in_converted.as_deref())
            .with_last_attempt_between_opt(
                filters
                    .last_attempt_between
                    .as_ref()
                    .map(|r| (r.start, r.end)),
            )
            .with_last_failure_since_opt(filters.last_failure_since)
            .with_next_planned_after_opt(filters.next_planned_after)
            .with_next_planned_before_opt(filters.next_planned_before)
            .with_min_consecutive_failures_opt(filters.min_consecutive_failures);

        let ordering = self.convert_ordering(ordering);

        let flow_process_state_query = from_catalog_n!(ctx, dyn fs::FlowProcessStateQuery);
        let listing = flow_process_state_query
            .list_processes(filter, ordering, Some(pagination))
            .await?;

        Ok(listing)
    }

    #[graphql(skip)]
    async fn form_shared_state_from_listing(
        &self,
        ctx: &Context<'_>,
        matched_processes_listing: &fs::FlowProcessStateListing,
        skip_webhooks: bool,
    ) -> Result<FlowProcessListingState> {
        let account = Account::new(
            self.account.id.clone().into(),
            self.account.account_name.clone().into(),
        );

        let dataset_handles_by_id =
            build_dataset_id_handle_mapping_from_processes_listing(ctx, matched_processes_listing)
                .await?;

        let webhook_subscriptions_by_id = if skip_webhooks {
            HashMap::new()
        } else {
            build_webhook_id_subscription_mapping_from_processes_listing(
                ctx,
                matched_processes_listing,
            )
            .await?
        };

        Ok(FlowProcessListingState {
            account,
            dataset_handles_by_id,
            webhook_subscriptions_by_id,
        })
    }

    #[graphql(skip)]
    async fn build_scope_query(&self, ctx: &Context<'_>) -> Result<fs::FlowScopeQuery> {
        let dataset_entry_service = from_catalog_n!(ctx, dyn DatasetEntryService);

        let owned_dataset_ids = dataset_entry_service
            .get_owned_dataset_ids(&self.account.id)
            .await
            .int_err()?;

        let owned_dataset_id_refs = owned_dataset_ids.iter().collect::<Vec<_>>();

        Ok(FlowScopeDataset::query_for_multiple_datasets_only(
            &owned_dataset_id_refs,
        ))
    }

    #[graphql(skip)]
    fn convert_ordering(&self, ordering: Option<FlowProcessOrdering>) -> fs::FlowProcessOrder {
        ordering
            .map(|ordering| fs::FlowProcessOrder {
                field: ordering.field.into(),
                desc: match ordering.direction {
                    OrderingDirection::Asc => false,
                    OrderingDirection::Desc => true,
                },
            })
            .unwrap_or_else(fs::FlowProcessOrder::recent)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum AccountFlowProcessCard {
    Dataset(DatasetFlowProcess),
    Webhook(WebhookFlowSubProcess),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    AccountFlowProcessCard,
    AccountFlowProcessCardConnection,
    AccountFlowProcessCardEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Default)]
pub struct FlowProcessFilters {
    /// State filter
    pub effective_state_in: Option<Vec<FlowProcessEffectiveState>>,

    /// All processes with last attempt between these times, inclusive
    pub last_attempt_between: Option<FlowProcessFiltersTimeRange>,

    /// All processes with last failure since this time, inclusive
    pub last_failure_since: Option<DateTime<Utc>>,

    /// All processes with next planned before this time, inclusive
    pub next_planned_before: Option<DateTime<Utc>>,

    /// All processes with next planned after this time, inclusive
    pub next_planned_after: Option<DateTime<Utc>>,

    /// Minimum number of consecutive failures
    pub min_consecutive_failures: Option<u32>,
}

#[derive(InputObject, Debug)]
pub struct FlowProcessFiltersTimeRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug, Clone)]
pub struct FlowProcessOrdering {
    pub field: FlowProcessOrderField,
    pub direction: OrderingDirection,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Debug)]
#[graphql(remote = "fs::FlowProcessOrderField")]
pub enum FlowProcessOrderField {
    /// Default for “recent activity”.
    LastAttemptAt,

    /// “What’s next”
    NextPlannedAt,

    /// Triage hot spots.
    LastFailureAt,

    /// Chronic issues first.
    ConsecutiveFailures,

    /// Severity bucketing
    EffectiveState,

    /// By flow type
    FlowType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowProcessListingState {
    account: Account,
    dataset_handles_by_id: HashMap<odf::DatasetID, odf::DatasetHandle>,
    webhook_subscriptions_by_id:
        HashMap<kamu_webhooks::WebhookSubscriptionID, kamu_webhooks::WebhookSubscription>,
}

impl FlowProcessListingState {
    fn build_dataset_card(&self, process_state: fs::FlowProcessState) -> DatasetFlowProcess {
        let dataset_id = FlowScopeDataset::new(&process_state.flow_binding().scope).dataset_id();
        let hdl = self
            .dataset_handles_by_id
            .get(&dataset_id)
            .expect("Inconsistent state: all datasets with flows must exist")
            .clone();

        DatasetFlowProcess::new(
            DatasetRequestState::new(hdl).with_owner(self.account.clone()),
            process_state,
        )
    }

    fn build_webhook_card(&self, process_state: fs::FlowProcessState) -> WebhookFlowSubProcess {
        let scope = &process_state.flow_binding().scope;
        let subscription_id = FlowScopeSubscription::new(scope).subscription_id();
        let webhook_subscription = self
            .webhook_subscriptions_by_id
            .get(&subscription_id)
            .expect("Inconsistent state: all webhook subscriptions with flows must exist");

        let parent_dataset_request_state =
            if let Some(dataset_id) = webhook_subscription.dataset_id() {
                let hdl = self
                    .dataset_handles_by_id
                    .get(dataset_id)
                    .expect("Inconsistent state: all datasets with flows must exist")
                    .clone();
                Some(DatasetRequestState::new(hdl).with_owner(self.account.clone()))
            } else {
                None
            };

        WebhookFlowSubProcess::new(
            webhook_subscription,
            parent_dataset_request_state,
            process_state,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
