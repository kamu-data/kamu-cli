// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use url::Url;

use crate::{CLIError, odf_server, resource_context};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourceContextTestService {
    access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
    resource_context_resolver: Arc<resource_context::ResourceContextResolver>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl ResourceContextTestService {
    pub fn new(
        access_token_registry_service: Arc<odf_server::AccessTokenRegistryService>,
        resource_context_resolver: Arc<resource_context::ResourceContextResolver>,
    ) -> Self {
        Self {
            access_token_registry_service,
            resource_context_resolver,
        }
    }

    pub async fn test_context(
        &self,
        explicit_context_name: Option<&str>,
    ) -> Result<resource_context::ResourceContextTestResult, CLIError> {
        let resolved_context = self
            .resource_context_resolver
            .resolve(explicit_context_name)?;

        match resolved_context {
            resource_context::ResolvedResourceContext::LocalWorkspace => {
                Ok(resource_context::ResourceContextTestResult {
                    name: resource_context::LOCAL_CONTEXT_NAME.to_string(),
                    backend_url: None,
                    reachable: true,
                    auth_status: resource_context::ResourceContextTestAuthStatus::NotChecked,
                    recommendation: None,
                    failure: None,
                })
            }
            resource_context::ResolvedResourceContext::RemoteWorkspace { name, backend_url } => {
                self.test_remote_context(&name, &backend_url).await
            }
        }
    }

    pub async fn test_remote_context(
        &self,
        name: &str,
        backend_url: &Url,
    ) -> Result<resource_context::ResourceContextTestResult, CLIError> {
        if let Err(reason) = self.probe_backend(backend_url).await {
            return Ok(resource_context::ResourceContextTestResult {
                name: name.to_string(),
                backend_url: Some(backend_url.clone()),
                reachable: false,
                auth_status: resource_context::ResourceContextTestAuthStatus::NotChecked,
                recommendation: None,
                failure: Some(reason),
            });
        }

        let recommendation = Some(format!("Run `kamu login {backend_url}`"));

        let Some(token_find_report) = self
            .access_token_registry_service
            .find_by_backend_url(backend_url)
        else {
            return Ok(resource_context::ResourceContextTestResult {
                name: name.to_string(),
                backend_url: Some(backend_url.clone()),
                reachable: true,
                auth_status: resource_context::ResourceContextTestAuthStatus::Missing,
                recommendation,
                failure: None,
            });
        };

        match self
            .validate_access_token(backend_url, &token_find_report.access_token)
            .await
        {
            Ok(resource_context::ResourceContextTestAuthStatus::Valid) => {
                Ok(resource_context::ResourceContextTestResult {
                    name: name.to_string(),
                    backend_url: Some(backend_url.clone()),
                    reachable: true,
                    auth_status: resource_context::ResourceContextTestAuthStatus::Valid,
                    recommendation: None,
                    failure: None,
                })
            }
            Ok(resource_context::ResourceContextTestAuthStatus::Expired) => {
                Ok(resource_context::ResourceContextTestResult {
                    name: name.to_string(),
                    backend_url: Some(backend_url.clone()),
                    reachable: true,
                    auth_status: resource_context::ResourceContextTestAuthStatus::Expired,
                    recommendation,
                    failure: None,
                })
            }
            Ok(resource_context::ResourceContextTestAuthStatus::Invalid) => {
                Ok(resource_context::ResourceContextTestResult {
                    name: name.to_string(),
                    backend_url: Some(backend_url.clone()),
                    reachable: true,
                    auth_status: resource_context::ResourceContextTestAuthStatus::Invalid,
                    recommendation,
                    failure: None,
                })
            }
            Ok(
                resource_context::ResourceContextTestAuthStatus::Missing
                | resource_context::ResourceContextTestAuthStatus::NotChecked,
            ) => unreachable!(),
            Err(failure) => Ok(resource_context::ResourceContextTestResult {
                name: name.to_string(),
                backend_url: Some(backend_url.clone()),
                reachable: false,
                auth_status: resource_context::ResourceContextTestAuthStatus::NotChecked,
                recommendation: None,
                failure: Some(failure),
            }),
        }
    }

    async fn validate_access_token(
        &self,
        backend_url: &Url,
        access_token: &odf_server::AccessToken,
    ) -> Result<
        resource_context::ResourceContextTestAuthStatus,
        resource_context::ResourceContextTestFailure,
    > {
        let client = reqwest::Client::new();
        let validation_url = Self::validation_url(backend_url)?;

        let response = client
            .get(validation_url.clone())
            .bearer_auth(access_token.access_token.clone())
            .send()
            .await
            .map_err(
                |e| resource_context::ResourceContextTestFailure::RequestFailed {
                    url: validation_url.clone(),
                    message: e.to_string(),
                },
            )?;

        match response.status() {
            http::StatusCode::OK => Ok(resource_context::ResourceContextTestAuthStatus::Valid),
            http::StatusCode::UNAUTHORIZED => {
                Ok(resource_context::ResourceContextTestAuthStatus::Expired)
            }
            http::StatusCode::BAD_REQUEST => {
                Ok(resource_context::ResourceContextTestAuthStatus::Invalid)
            }
            status => Err(
                resource_context::ResourceContextTestFailure::UnexpectedResponse {
                    url: validation_url,
                    status_code: status.as_u16(),
                    status_text: status.canonical_reason().unwrap_or("Unknown").to_string(),
                },
            ),
        }
    }

    async fn probe_backend(
        &self,
        backend_url: &Url,
    ) -> Result<(), resource_context::ResourceContextTestFailure> {
        let client = reqwest::Client::new();
        let validation_url = Self::validation_url(backend_url)?;

        let response = client
            .get(validation_url.clone())
            .send()
            .await
            .map_err(
                |e| resource_context::ResourceContextTestFailure::RequestFailed {
                    url: validation_url.clone(),
                    message: e.to_string(),
                },
            )?;

        match response.status() {
            http::StatusCode::OK
            | http::StatusCode::UNAUTHORIZED
            | http::StatusCode::BAD_REQUEST => Ok(()),
            status => Err(
                resource_context::ResourceContextTestFailure::UnexpectedResponse {
                    url: validation_url,
                    status_code: status.as_u16(),
                    status_text: status.canonical_reason().unwrap_or("Unknown").to_string(),
                },
            ),
        }
    }

    fn validation_url(
        backend_url: &Url,
    ) -> Result<Url, resource_context::ResourceContextTestFailure> {
        backend_url.join("platform/token/validate").map_err(|e| {
            resource_context::ResourceContextTestFailure::RequestFailed {
                url: backend_url.clone(),
                message: e.to_string(),
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
