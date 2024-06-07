// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::*;
use kamu_accounts::AccessToken;
use opendatafabric::AccountID;
use uuid::Uuid;

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccessTokenRepositoryInMemory {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    token_hashes_by_account_id: HashMap<AccountID, Uuid>,
    token_ids_by_name: HashMap<String, Uuid>,
    tokens_by_id: HashMap<Uuid, AccessToken>,
}

impl State {
    fn new() -> Self {
        Self {
            token_hashes_by_account_id: HashMap::new(),
            token_ids_by_name: HashMap::new(),
            tokens_by_id: HashMap::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccessTokenRepository)]
#[scope(Singleton)]
impl AccessTokenRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccessTokenRepository for AccessTokenRepositoryInMemory {
    async fn create_access_token(
        &self,
        access_token: &AccessToken,
    ) -> Result<(), CreateAccessTokenError> {
        let mut guard = self.state.lock().unwrap();
        if guard.tokens_by_id.get(&access_token.id).is_some() {
            return Err(CreateAccessTokenError::Duplicate(
                CreateAccessTokenErrorDuplicate {
                    access_token_name: access_token.token_name.clone(),
                },
            ));
        }
        if let Some(existing_token_id) = guard.token_ids_by_name.get(&access_token.token_name)
            && guard.tokens_by_id.get(existing_token_id).is_some()
        {
            return Err(CreateAccessTokenError::Duplicate(
                CreateAccessTokenErrorDuplicate {
                    access_token_name: access_token.token_name.clone(),
                },
            ));
        }
        guard
            .tokens_by_id
            .insert(access_token.id, access_token.clone());
        guard
            .token_ids_by_name
            .insert(access_token.token_name.clone(), access_token.id);
        guard
            .token_hashes_by_account_id
            .insert(access_token.account_id.clone(), access_token.id);

        Ok(())
    }

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError> {
        let guard = self.state.lock().unwrap();
        if let Some(account_data) = guard.tokens_by_id.get(token_id) {
            Ok(account_data.clone())
        } else {
            Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }))
        }
    }

    async fn get_access_tokens(&self) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        let guard = self.state.lock().unwrap();

        let access_tokens: Vec<AccessToken> = guard.tokens_by_id.values().cloned().collect();

        Ok(access_tokens)
    }

    async fn mark_revoked(
        &self,
        token_id: &Uuid,
        revoked_time: DateTime<Utc>,
    ) -> Result<(), GetAccessTokenError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(existing_account) = guard.tokens_by_id.get(token_id) {
            let revoked_access_token = AccessToken {
                revoked_at: Some(revoked_time),
                ..existing_account.clone()
            };
            guard.tokens_by_id.insert(*token_id, revoked_access_token);

            return Ok(());
        }
        Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
            access_token_id: *token_id,
        }))
    }
}
