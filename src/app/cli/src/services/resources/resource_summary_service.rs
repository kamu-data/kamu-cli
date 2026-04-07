// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use graphql_http::{GraphqlHttpClient, GraphqlHttpRequestError};
use kamu_accounts::CurrentAccountSubject;
use kamu_resources_facade::{
    RemoteGraphqlResourceFacadeImpl,
    ResourceFacade,
    ResourcesSummaryRequest,
};
use serde::Deserialize;
use url::Url;

use crate::CLIError;
use crate::odf_server::AccessTokenRegistryService;
use crate::resource_context::{
    LOCAL_CONTEXT_NAME,
    ResolvedResourceContext,
    ResourceContextResolver,
};
use crate::resources::{ResourceContextSummaryView, ResourceContextTypeView, ResourceSummaryView};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceSummaryService {
    local_resource_facade: Arc<dyn ResourceFacade>,
    resource_context_resolver: Arc<ResourceContextResolver>,
    access_token_registry_service: Arc<AccessTokenRegistryService>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl ResourceSummaryService {
    pub fn new(
        local_resource_facade: Arc<dyn ResourceFacade>,
        resource_context_resolver: Arc<ResourceContextResolver>,
        access_token_registry_service: Arc<AccessTokenRegistryService>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            local_resource_facade,
            resource_context_resolver,
            access_token_registry_service,
            current_account_subject,
        }
    }

    pub async fn summary(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<ResourceSummaryView, CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(explicit_context_name)?;

        match resolved_context {
            ResolvedResourceContext::LocalWorkspace => self.local_summary().await,
            ResolvedResourceContext::RemoteWorkspace { name, backend_url } => {
                self.remote_summary(name, backend_url).await
            }
        }
    }

    async fn local_summary(&self) -> Result<ResourceSummaryView, CLIError> {
        let summary = self
            .local_resource_facade
            .summary(ResourcesSummaryRequest::default())
            .await?;

        Ok(ResourceSummaryView {
            context: ResourceContextSummaryView {
                active_context_name: LOCAL_CONTEXT_NAME.to_string(),
                context_type: ResourceContextTypeView::Local,
                endpoint_url: None,
                server_version: None,
                account_name: self
                    .current_account_subject
                    .maybe_account_name()
                    .map(ToString::to_string),
            },
            resource_counts: summary.resource_counts,
        })
    }

    async fn remote_summary(
        &self,
        name: String,
        backend_url: Url,
    ) -> Result<ResourceSummaryView, CLIError> {
        let maybe_access_token = self
            .access_token_registry_service
            .find_access_token_by_backend_url(&backend_url);

        let remote_resource_facade =
            RemoteGraphqlResourceFacadeImpl::new(&backend_url, maybe_access_token.clone());

        let summary = remote_resource_facade
            .summary(ResourcesSummaryRequest::default())
            .await?;

        let context_metadata = self
            .fetch_remote_context_metadata(&backend_url, maybe_access_token.as_deref())
            .await?;

        Ok(ResourceSummaryView {
            context: ResourceContextSummaryView {
                active_context_name: name,
                context_type: ResourceContextTypeView::Remote,
                endpoint_url: Some(backend_url.to_string()),
                server_version: context_metadata.server_version,
                account_name: context_metadata.account_name,
            },
            resource_counts: summary.resource_counts,
        })
    }

    async fn fetch_remote_context_metadata(
        &self,
        backend_url: &Url,
        maybe_access_token: Option<&str>,
    ) -> Result<RemoteContextMetadata, GraphqlHttpRequestError> {
        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RemoteContextMetadataQueryData {
            build_info: RemoteBuildInfoFragment,
            accounts: RemoteAccountsFragment,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RemoteBuildInfoFragment {
            app_version: String,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RemoteAccountsFragment {
            me: RemoteCurrentAccountFragment,
        }

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct RemoteCurrentAccountFragment {
            account_name: String,
        }

        let graphql_client =
            GraphqlHttpClient::from_backend_url(backend_url, maybe_access_token.map(str::to_owned));
        let data: RemoteContextMetadataQueryData = graphql_client
            .execute(
                r#"
                query {
                  buildInfo {
                    appVersion
                  }
                  accounts {
                    me {
                      accountName
                    }
                  }
                }
                "#,
            )
            .await?;

        Ok(RemoteContextMetadata {
            server_version: Some(data.build_info.app_version),
            account_name: Some(data.accounts.me.account_name),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct RemoteContextMetadata {
    server_version: Option<String>,
    account_name: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
