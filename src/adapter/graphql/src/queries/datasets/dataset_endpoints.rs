// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_core::ServerUrlConfig;

use crate::prelude::*;
use crate::queries::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEndpoints<'a> {
    dataset_request_state: &'a DatasetRequestState,
    config: Arc<ServerUrlConfig>,
}

#[Object]
impl<'a> DatasetEndpoints<'a> {
    #[graphql(skip)]
    pub fn new(
        dataset_request_state: &'a DatasetRequestState,
        config: Arc<ServerUrlConfig>,
    ) -> Self {
        Self {
            dataset_request_state,
            config,
        }
    }

    #[graphql(skip)]
    #[inline]
    fn account(&self) -> &str {
        self.dataset_request_state
            .owner()
            .account_name_internal()
            .as_str()
    }

    #[graphql(skip)]
    #[inline]
    fn dataset(&self) -> &str {
        self.dataset_request_state
            .dataset_handle()
            .alias
            .dataset_name
            .as_str()
    }

    #[expect(clippy::unused_async)]
    async fn web_link(&self) -> Result<LinkProtocolDesc> {
        let url = format!(
            "{}{}/{}",
            self.config.protocols.base_url_platform,
            self.account(),
            self.dataset()
        );

        Ok(LinkProtocolDesc { url })
    }

    #[expect(clippy::unused_async)]
    async fn cli(&self) -> Result<CliProtocolDesc> {
        let url = format!(
            "odf+{}{}",
            self.config.protocols.base_url_rest,
            // to respect both kinds of workspaces: single-tenant & multi-tenant
            self.dataset_request_state.dataset_handle().alias
        );

        let pull_command = format!("kamu pull {url}");
        let push_command = format!("kamu push {} --to {url}", self.dataset());

        Ok(CliProtocolDesc {
            pull_command,
            push_command,
        })
    }

    #[expect(clippy::unused_async)]
    async fn rest(&self) -> Result<RestProtocolDesc> {
        let dataset_base_url = format!(
            "{}{}",
            self.config.protocols.base_url_rest,
            // to respect both kinds of workspaces: single-tenant & multi-tenant
            self.dataset_request_state.dataset_handle().alias
        );

        let tail_url = format!("{dataset_base_url}/tail?limit=10");
        let push_url = format!("{dataset_base_url}/ingest");
        let query_url = format!(
            "{}query?query=SELECT 42 as answer",
            self.config.protocols.base_url_rest
        );

        Ok(RestProtocolDesc {
            tail_url,
            query_url,
            push_url,
        })
    }

    #[expect(clippy::unused_async)]
    async fn flightsql(&self) -> Result<FlightSqlDesc> {
        Ok(FlightSqlDesc {
            url: self.config.protocols.base_url_flightsql.to_string(),
        })
    }

    #[expect(clippy::unused_async)]
    async fn jdbc(&self) -> Result<JdbcDesc> {
        let mut url = self.config.protocols.base_url_flightsql.clone();

        url.set_scheme("arrow-flight-sql").unwrap();

        Ok(JdbcDesc {
            url: format!("jdbc:{url}"),
        })
    }

    #[expect(clippy::unused_async)]
    async fn postgresql(&self) -> Result<PostgreSqlDesl> {
        Ok(PostgreSqlDesl {
            url: "- coming soon -".to_string(),
        })
    }

    #[expect(clippy::unused_async)]
    async fn kafka(&self) -> Result<KafkaProtocolDesc> {
        Ok(KafkaProtocolDesc {
            url: "- coming soon -".to_string(),
        })
    }

    #[expect(clippy::unused_async)]
    async fn websocket(&self) -> Result<WebSocketProtocolDesc> {
        Ok(WebSocketProtocolDesc {
            url: "- coming soon -".to_string(),
        })
    }

    #[expect(clippy::unused_async)]
    async fn odata(&self) -> Result<OdataProtocolDesc> {
        let mut url = format!("{}odata", self.config.protocols.base_url_rest);
        // to respect both kinds of workspaces: single-tenant & multi-tenant
        let collection_url = format!(
            "{url}/{}",
            self.dataset_request_state.dataset_handle().alias
        );

        let service_url = {
            // Optional for single-tenant workspaces
            let maybe_account_url_segment = self
                .dataset_request_state
                .dataset_handle()
                .alias
                .account_name
                .as_ref()
                .map(odf::AccountName::as_str);
            if let Some(account_url_segment) = maybe_account_url_segment {
                url.push('/');
                url.push_str(account_url_segment);
            };

            url
        };

        Ok(OdataProtocolDesc {
            service_url,
            collection_url,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
