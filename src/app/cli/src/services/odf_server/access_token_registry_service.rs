// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::auth::{OdfServerAccessTokenResolveError, OdfServerLoginRequiredError};
use kamu::domain::CurrentAccountSubject;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::AccountName;
use url::Url;

use crate::odf_server::models::*;
use crate::WorkspaceLayout;

////////////////////////////////////////////////////////////////////////////////////////

type OdfServerAccessTokenRegistry = Vec<AccessTokenMap>;

pub struct AccessTokenRegistryService {
    storage: Arc<dyn AccessTokenStore>,
    current_account_subject: Arc<CurrentAccountSubject>,
    workspace_registry: Mutex<OdfServerAccessTokenRegistry>,
    user_registry: Mutex<OdfServerAccessTokenRegistry>,
}

#[component(pub)]
impl AccessTokenRegistryService {
    pub fn new(
        storage: Arc<dyn AccessTokenStore>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        let user_registry = storage
            .read_access_tokens_registry(AccessTokenStoreScope::User)
            .unwrap();

        let workspace_registry = storage
            .read_access_tokens_registry(AccessTokenStoreScope::Workspace)
            .unwrap();

        Self {
            storage,
            current_account_subject,
            user_registry: Mutex::new(user_registry),
            workspace_registry: Mutex::new(workspace_registry),
        }
    }

    fn account_name<'a>(&'a self) -> &'a AccountName {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => &l.account_name,
            CurrentAccountSubject::Anonymous(_) => panic!("Anonymous current account unexpected"),
        }
    }

    pub fn find_by_frontend_url(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_frontend_url: &Url,
    ) -> Option<AccessTokenFindReport> {
        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        if let Some(token_map) = registry
            .iter()
            .find(|c| &c.frontend_url == odf_server_frontend_url)
        {
            token_map
                .token_for_account(self.account_name())
                .map(|ac| AccessTokenFindReport {
                    backend_url: token_map.backend_url.clone(),
                    frontend_url: token_map.frontend_url.clone(),
                    access_token: ac.clone(),
                })
        } else {
            None
        }
    }

    pub fn find_by_backend_url(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_backend_url: &Url,
    ) -> Option<AccessTokenFindReport> {
        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        if let Some(token_map) = registry
            .iter()
            .find(|c| &c.backend_url == odf_server_backend_url)
        {
            token_map
                .token_for_account(self.account_name())
                .map(|ac| AccessTokenFindReport {
                    backend_url: token_map.backend_url.clone(),
                    frontend_url: token_map.frontend_url.clone(),
                    access_token: ac.clone(),
                })
        } else {
            None
        }
    }

    pub fn save_access_token(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_frontend_url: &Url,
        odf_server_backend_url: &Url,
        access_token: String,
    ) -> Result<(), InternalError> {
        let account_name = self.account_name();

        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let mut registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        let access_token = AccessToken::new(account_name.clone(), access_token);

        if let Some(token_map) = registry
            .iter_mut()
            .find(|c| &c.frontend_url == odf_server_frontend_url)
        {
            token_map.add_account_token(access_token);
        } else {
            let mut token_map = AccessTokenMap::new(
                odf_server_frontend_url.clone(),
                odf_server_backend_url.clone(),
            );
            token_map.add_account_token(access_token);

            registry.push(token_map);
        }

        self.storage.write_access_tokens_registry(scope, &registry)
    }

    pub fn drop_access_token(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_frontend_url: &Url,
    ) -> Result<(), InternalError> {
        let account_name = self.account_name();

        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let mut registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        if let Some(token_map) = registry
            .iter_mut()
            .find(|c| &c.frontend_url == odf_server_frontend_url)
        {
            if let Some(_) = token_map.drop_account_token(account_name) {
                return self.storage.write_access_tokens_registry(scope, &registry);
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu::domain::auth::OdfServerAccessTokenResolver for AccessTokenRegistryService {
    async fn resolve_odf_dataset_access_token(
        &self,
        odf_dataset_http_url: &Url,
    ) -> Result<String, OdfServerAccessTokenResolveError> {
        let origin = odf_dataset_http_url.origin().unicode_serialization();
        let odf_server_backend_url = Url::parse(origin.as_str()).unwrap();

        if let Some(token_find_report) =
            self.find_by_backend_url(AccessTokenStoreScope::Workspace, &odf_server_backend_url)
        {
            Ok(token_find_report.access_token.access_token)
        } else if let Some(token_find_report) =
            self.find_by_backend_url(AccessTokenStoreScope::User, &odf_server_backend_url)
        {
            Ok(token_find_report.access_token.access_token)
        } else {
            Err(OdfServerAccessTokenResolveError::LoginRequired(
                OdfServerLoginRequiredError {
                    odf_server_backend_url,
                },
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

pub trait AccessTokenStore: Send + Sync {
    fn read_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
    ) -> Result<OdfServerAccessTokenRegistry, InternalError>;

    fn write_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
        registry: &OdfServerAccessTokenRegistry,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////

const KAMU_TOKEN_STORE: &str = ".kamutokenstore";
const KAMU_TOKEN_STORE_VERSION: i32 = 1;
const KAMU_TOKEN_STORE_MANIFEST_KIND: &str = "KamuTokenStore";

pub struct CLIAccessTokenStore {
    user_token_store_path: PathBuf,
    workspace_token_store_path: PathBuf,
}

#[component(pub)]
impl CLIAccessTokenStore {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        let user_token_store_path = dirs::home_dir()
            .expect("Cannot determine user home directory")
            .join(KAMU_TOKEN_STORE);

        let workspace_token_store_path: PathBuf = workspace_layout.root_dir.join(KAMU_TOKEN_STORE);

        Self {
            user_token_store_path,
            workspace_token_store_path,
        }
    }

    fn token_store_path_for_scope(&self, scope: AccessTokenStoreScope) -> &PathBuf {
        match scope {
            AccessTokenStoreScope::User => &self.user_token_store_path,
            AccessTokenStoreScope::Workspace => &self.workspace_token_store_path,
        }
    }
}

impl AccessTokenStore for CLIAccessTokenStore {
    fn read_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
    ) -> Result<OdfServerAccessTokenRegistry, InternalError> {
        let token_store_path = self.token_store_path_for_scope(scope);
        if !token_store_path.exists() {
            return Ok(vec![]);
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(token_store_path)
            .int_err()?;

        let manifest: Manifest<OdfServerAccessTokenRegistry> =
            serde_yaml::from_reader(file).int_err()?;

        assert_eq!(manifest.kind, KAMU_TOKEN_STORE_MANIFEST_KIND);
        assert_eq!(manifest.version, KAMU_TOKEN_STORE_VERSION);
        Ok(manifest.content)
    }

    fn write_access_tokens_registry(
        &self,
        scope: AccessTokenStoreScope,
        registry: &OdfServerAccessTokenRegistry,
    ) -> Result<(), InternalError> {
        let token_store_path = self.token_store_path_for_scope(scope);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(token_store_path)
            .unwrap();

        let manifest = Manifest {
            kind: KAMU_TOKEN_STORE_MANIFEST_KIND.to_owned(),
            version: KAMU_TOKEN_STORE_VERSION,
            content: registry,
        };

        serde_yaml::to_writer(file, &manifest).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////
