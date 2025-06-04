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

use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::CurrentAccountSubject;
use odf::metadata::serde::yaml::Manifest;
use url::Url;

use crate::WorkspaceLayout;
use crate::odf_server::models::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type OdfServerAccessTokenRegistry = Vec<ServerAccessTokensRecord>;

pub struct AccessTokenRegistryService {
    storage: Arc<dyn AccessTokenStore>,
    current_account_subject: Arc<CurrentAccountSubject>,
    workspace_registry: Mutex<OdfServerAccessTokenRegistry>,
    user_registry: Mutex<OdfServerAccessTokenRegistry>,
}

#[component(pub)]
#[interface(dyn odf::dataset::OdfServerAccessTokenResolver)]
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

    fn account_name(&self) -> &odf::AccountName {
        match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Logged(l) => &l.account_name,
            CurrentAccountSubject::Anonymous(_) => panic!("Anonymous current account unexpected"),
        }
    }

    pub fn find_by_frontend_or_backend_url(
        &self,
        odf_server_url: &Url,
    ) -> Option<AccessTokenFindReport> {
        for registry_ptr in [&self.workspace_registry, &self.user_registry] {
            let registry = registry_ptr
                .lock()
                .expect("Could not lock access tokens registry");
            let server_record_maybe = registry.iter().find(|c| {
                if &c.backend_url == odf_server_url {
                    true
                } else {
                    match &c.frontend_url {
                        Some(frontend_url) => frontend_url == odf_server_url,
                        _ => false,
                    }
                }
            });
            if let Some(server_record) = server_record_maybe {
                return server_record
                    .token_for_account(self.account_name())
                    .map(|ac| AccessTokenFindReport {
                        backend_url: server_record.backend_url.clone(),
                        frontend_url: server_record.frontend_url.clone(),
                        access_token: ac.clone(),
                    });
            }
        }
        None
    }

    pub fn find_by_frontend_url(
        &self,
        odf_server_frontend_url: &Url,
    ) -> Option<AccessTokenFindReport> {
        for registry_ptr in [&self.workspace_registry, &self.user_registry] {
            let registry = registry_ptr
                .lock()
                .expect("Could not lock access tokens registry");

            let server_record_maybe = registry.iter().find(|c| match &c.frontend_url {
                Some(frontend_url) => frontend_url == odf_server_frontend_url,
                _ => false,
            });
            if let Some(server_record) = server_record_maybe {
                return server_record
                    .token_for_account(self.account_name())
                    .map(|ac| AccessTokenFindReport {
                        backend_url: server_record.backend_url.clone(),
                        frontend_url: server_record.frontend_url.clone(),
                        access_token: ac.clone(),
                    });
            }
        }
        None
    }

    pub fn find_by_backend_url(
        &self,
        odf_server_backend_url: &Url,
    ) -> Option<AccessTokenFindReport> {
        for registry_ptr in [&self.workspace_registry, &self.user_registry] {
            let registry = registry_ptr
                .lock()
                .expect("Could not lock access tokens registry");

            if let Some(token_map) = registry
                .iter()
                .find(|c| &c.backend_url == odf_server_backend_url)
            {
                return token_map.token_for_account(self.account_name()).map(|ac| {
                    AccessTokenFindReport {
                        backend_url: token_map.backend_url.clone(),
                        frontend_url: token_map.frontend_url.clone(),
                        access_token: ac.clone(),
                    }
                });
            }
        }
        None
    }

    pub fn save_access_token(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_frontend_url: Option<&Url>,
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

        if let Some(server_record) = registry.iter_mut().find(|c| {
            if &c.backend_url == odf_server_backend_url {
                true
            } else {
                match (&c.frontend_url, odf_server_frontend_url) {
                    (Some(frontend_a), Some(frontend_b)) => frontend_a == frontend_b,
                    _ => false,
                }
            }
        }) {
            server_record.add_account_token(access_token);
            if server_record.frontend_url.is_none() && odf_server_frontend_url.is_some() {
                server_record.frontend_url = odf_server_frontend_url.cloned();
            }
        } else {
            let mut server_record = ServerAccessTokensRecord::new(
                odf_server_frontend_url.cloned(),
                odf_server_backend_url.clone(),
            );
            server_record.add_account_token(access_token);

            registry.push(server_record);
        }

        self.storage.write_access_tokens_registry(scope, &registry)
    }

    pub fn drop_all_access_tokens_in_scope(
        &self,
        scope: AccessTokenStoreScope,
    ) -> Result<bool, InternalError> {
        let account_name = self.account_name();

        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let mut registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        let mut modified = false;
        for record in registry.iter_mut() {
            if record.drop_account_token(account_name).is_some() {
                modified = true;
            }
        }

        if modified {
            self.storage
                .write_access_tokens_registry(scope, &registry)?;
        }

        Ok(modified)
    }

    pub fn drop_access_token(
        &self,
        scope: AccessTokenStoreScope,
        odf_server_url: &Url,
    ) -> Result<bool, InternalError> {
        let account_name = self.account_name();

        let registry_ptr = match scope {
            AccessTokenStoreScope::User => &self.user_registry,
            AccessTokenStoreScope::Workspace => &self.workspace_registry,
        };

        let mut registry = registry_ptr
            .lock()
            .expect("Could not lock access tokens registry");

        if let Some(token_map) = registry.iter_mut().find(|c| {
            &c.backend_url == odf_server_url
                || c.frontend_url
                    .as_ref()
                    .is_some_and(|frontend_url| frontend_url == odf_server_url)
        }) && token_map.drop_account_token(account_name).is_some()
        {
            self.storage
                .write_access_tokens_registry(scope, &registry)?;
            return Ok(true);
        }

        Ok(false)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::dataset::OdfServerAccessTokenResolver for AccessTokenRegistryService {
    fn resolve_odf_dataset_access_token(&self, odf_dataset_http_url: &Url) -> Option<String> {
        assert!(!odf_dataset_http_url.scheme().starts_with("odf+"));

        let origin = odf_dataset_http_url.origin().unicode_serialization();
        if let Ok(odf_server_backend_url) = Url::parse(origin.as_str()) {
            return self
                .find_by_backend_url(&odf_server_backend_url)
                .map(|token_report| token_report.access_token.access_token);
        }
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const KAMU_TOKEN_STORE: &str = ".kamutokenstore";
const KAMU_TOKEN_STORE_VERSION: i32 = 1;
const KAMU_TOKEN_STORE_MANIFEST_KIND: &str = "KamuTokenStore";

pub struct CLIAccessTokenStore {
    user_token_store_path: PathBuf,
    workspace_token_store_path: PathBuf,
}

#[component(pub)]
#[interface(dyn AccessTokenStore)]
impl CLIAccessTokenStore {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        // TODO: Respect `XDG_CONFIG_HOME` when working with configs
        //       https://github.com/kamu-data/kamu-cli/issues/848
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
