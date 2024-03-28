// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;
use url::Url;

use crate::prelude::*;
use crate::queries::*;

pub struct DatasetEndpoints<'a> {
    owner: &'a Account,
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl<'a> DatasetEndpoints<'a> {
    #[graphql(skip)]
    fn get_base_url() -> std::result::Result<Url, InternalError> {
        // TODO: Use value from config not envvar
        //       https://github.com/kamu-data/kamu-node/issues/45
        let raw_base_url =
            std::env::var("KAMU_BASE_URL").unwrap_or_else(|_| "http://127.0.0.1:8080".to_string());

        Url::parse(&raw_base_url).int_err()
    }

    #[graphql(skip)]
    pub fn new(owner: &'a Account, dataset_handle: odf::DatasetHandle) -> Self {
        Self {
            owner,
            dataset_handle,
        }
    }

    #[allow(clippy::unused_async)]
    async fn web_link(&self) -> Result<LinkProtocolDesc> {
        let base_url = Self::get_base_url()?;
        let url = format!("{base_url}{}", self.dataset_handle.alias);

        Ok(LinkProtocolDesc { url })
    }

    #[allow(clippy::unused_async)]
    async fn cli(&self) -> Result<CliProtocolDesc> {
        let base_url = Self::get_base_url()?;
        let url = format!("{base_url}{}", self.dataset_handle.alias);

        let push_command = format!("kamu push {url}");
        let pull_command = format!("kamu pull {url}");

        Ok(CliProtocolDesc {
            pull_command,
            push_command,
        })
    }

    #[allow(clippy::unused_async)]
    async fn rest(&self) -> Result<RestProtocolDesc> {
        let base_url = Self::get_base_url()?;
        let url = format!("{base_url}{}", self.dataset_handle.alias);

        let tail_url = format!("{url}/tail?limit=10");
        let query_url = format!("{base_url}graphql?query=query {{%0A%20 apiVersion%0A}}%0A");
        let push_url = format!("{url}/push");

        Ok(RestProtocolDesc {
            tail_url,
            query_url,
            push_url,
        })
    }

    #[allow(clippy::unused_async)]
    async fn flightsql(&self) -> Result<FlightSqlDesc> {
        // TODO: replace hardcoded value
        Ok(FlightSqlDesc {
            url: "datafusion+flightsql://node.demo.kamu.dev:50050".to_string(),
        })
    }

    #[allow(clippy::unused_async)]
    async fn jdbc(&self) -> Result<JdbcDesc> {
        // TODO: replace hardcoded value
        Ok(JdbcDesc {
            url: "jdbc:arrow-flight-sql://node.demo.kamu.dev:50050".to_string(),
        })
    }

    #[allow(clippy::unused_async)]
    async fn postgresql(&self) -> Result<PostgreSqlDesl> {
        Ok(PostgreSqlDesl {
            url: "- coming soon -".to_string(),
        })
    }

    #[allow(clippy::unused_async)]
    async fn kafka(&self) -> Result<KafkaProtocolDesc> {
        Ok(KafkaProtocolDesc {
            url: "- coming soon -".to_string(),
        })
    }

    #[allow(clippy::unused_async)]
    async fn websocket(&self) -> Result<WebSocketProtocolDesc> {
        Ok(WebSocketProtocolDesc {
            url: "- coming soon -".to_string(),
        })
    }

    #[allow(clippy::unused_async)]
    async fn odata(&self) -> Result<OdataProtocolDesc> {
        let base_url = Self::get_base_url()?;
        let url = format!("{base_url}odata");

        let service_url = format!("{url}/{}", self.owner.account_name_internal().as_str());
        let collection_url = format!("{url}/{}", self.dataset_handle.alias);

        Ok(OdataProtocolDesc {
            service_url,
            collection_url,
        })
    }
}
