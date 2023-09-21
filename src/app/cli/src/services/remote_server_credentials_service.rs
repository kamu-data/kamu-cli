// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;

use dill::component;
use internal_error::InternalError;
use kamu::domain::auth::{DatasetCredentials, ResolveDatasetCredentialsError};
use opendatafabric::DatasetAlias;
use url::Url;

use crate::DEFAULT_LOGIN_URL;

////////////////////////////////////////////////////////////////////////////////////////

const KAMU_CREDENTIALS: &str = ".kamucredentials";

////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteServerCredentialsService {
    user_credentials_path: PathBuf,
    workspace_credentials_path: PathBuf,

    workspace_credentials: HashMap<Url, RemoteServerCredentials>,
    user_credentials: HashMap<Url, RemoteServerCredentials>,
}

#[component(pub)]
impl RemoteServerCredentialsService {
    pub fn new(workspace_kamu_dir: PathBuf) -> Self {
        let user_credentials_path = dirs::home_dir()
            .expect("Cannot determine user home directory")
            .join(KAMU_CREDENTIALS);

        let user_credentials = Self::read_credentials_file(&user_credentials_path);

        let workspace_credentials_path: PathBuf = workspace_kamu_dir.join(KAMU_CREDENTIALS);

        let workspace_credentials = Self::read_credentials_file(&workspace_credentials_path);

        Self {
            user_credentials_path,
            workspace_credentials_path,
            user_credentials,
            workspace_credentials,
        }
    }

    fn read_credentials_file(_path: &PathBuf) -> HashMap<Url, RemoteServerCredentials> {
        // TODO
        unimplemented!()
    }

    fn write_credentials_file(
        _path: &PathBuf,
        _credentials: &HashMap<Url, RemoteServerCredentials>,
    ) -> Result<(), InternalError> {
        // TODO
        unimplemented!()
    }

    fn resolve_server_url(server: Option<Url>) -> Url {
        if let Some(server_url) = server {
            server_url
        } else {
            Url::parse(DEFAULT_LOGIN_URL).unwrap()
        }
    }

    pub fn find_existing_credentials(
        &self,
        scope: Option<RemoteServerCredentialsScope>,
        server: Option<Url>,
    ) -> Option<RemoteServerCredentials> {
        let server_url = Self::resolve_server_url(server);

        match scope {
            Some(RemoteServerCredentialsScope::User) => {
                self.user_credentials.get(&server_url).map(|c| c.clone())
            }

            Some(RemoteServerCredentialsScope::Workspace) => self
                .workspace_credentials
                .get(&server_url)
                .map(|c| c.clone()),

            None => {
                if let Some(credentials) = self.workspace_credentials.get(&server_url) {
                    Some(credentials.clone())
                } else {
                    self.user_credentials.get(&server_url).map(|c| c.clone())
                }
            }
        }
    }

    pub fn save_credentials(
        &mut self,
        scope: RemoteServerCredentialsScope,
        credentials: RemoteServerCredentials,
    ) -> Result<(), InternalError> {
        let (credentials_table, target_path) = match scope {
            RemoteServerCredentialsScope::User => {
                (&mut self.user_credentials, &self.user_credentials_path)
            }
            RemoteServerCredentialsScope::Workspace => (
                &mut self.workspace_credentials,
                &self.workspace_credentials_path,
            ),
        };

        credentials_table.insert(credentials.server.clone(), credentials);

        Self::write_credentials_file(target_path, credentials_table)
    }

    pub fn drop_credentials(
        &mut self,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Result<(), InternalError> {
        let (credentials_table, target_path) = match scope {
            RemoteServerCredentialsScope::User => {
                (&mut self.user_credentials, &self.user_credentials_path)
            }
            RemoteServerCredentialsScope::Workspace => (
                &mut self.workspace_credentials,
                &self.workspace_credentials_path,
            ),
        };

        let server_url = Self::resolve_server_url(server);

        credentials_table.remove(&server_url);

        Self::write_credentials_file(target_path, credentials_table)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu::domain::auth::DatasetCredentialsResolver for RemoteServerCredentialsService {
    async fn resolve_dataset_credentials(
        &self,
        _dataset_alias: &DatasetAlias,
    ) -> Result<DatasetCredentials, ResolveDatasetCredentialsError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum RemoteServerCredentialsScope {
    Workspace,
    User,
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RemoteServerCredentials {
    pub server: Url,
    pub access_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////
