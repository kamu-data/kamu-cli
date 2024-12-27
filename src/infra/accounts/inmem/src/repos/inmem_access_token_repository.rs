// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use dill::*;
use internal_error::ErrorIntoInternal;
use kamu_accounts::AccessToken;
use uuid::Uuid;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryAccessTokenRepository {
    state: Arc<Mutex<State>>,
    account_repository: Arc<dyn AccountRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    token_hashes_by_account_id: HashMap<odf::AccountID, Vec<Uuid>>,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccessTokenRepository)]
#[scope(Singleton)]
impl InMemoryAccessTokenRepository {
    pub fn new(account_repository: Arc<dyn AccountRepository>) -> Self {
        Self {
            account_repository,
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccessTokenRepository for InMemoryAccessTokenRepository {
    async fn save_access_token(
        &self,
        access_token: &AccessToken,
    ) -> Result<(), CreateAccessTokenError> {
        let mut guard = self.state.lock().unwrap();
        if guard.tokens_by_id.contains_key(&access_token.id) {
            return Err(CreateAccessTokenError::Duplicate(
                CreateAccessTokenErrorDuplicate {
                    access_token_name: access_token.token_name.clone(),
                },
            ));
        }
        if let Some(existing_token_id) = guard.token_ids_by_name.get(&access_token.token_name)
            && let Some(existing_token) = guard.tokens_by_id.get(existing_token_id)
            && existing_token.revoked_at.is_none()
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

        let token_hashes_entries = match guard
            .token_hashes_by_account_id
            .entry(access_token.account_id.clone())
        {
            Entry::Occupied(v) => v.into_mut(),
            Entry::Vacant(v) => v.insert(Vec::default()),
        };
        token_hashes_entries.push(access_token.id);

        Ok(())
    }

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError> {
        let guard = self.state.lock().unwrap();
        if let Some(access_token_data) = guard.tokens_by_id.get(token_id) {
            Ok(access_token_data.clone())
        } else {
            Err(GetAccessTokenError::NotFound(AccessTokenNotFoundError {
                access_token_id: *token_id,
            }))
        }
    }

    async fn get_access_tokens_count_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<usize, GetAccessTokenError> {
        let guard = self.state.lock().unwrap();

        if let Some(access_token_ids) = guard.token_hashes_by_account_id.get(account_id) {
            return Ok(access_token_ids.len());
        }
        Ok(0)
    }

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &odf::AccountID,
        pagination: &PaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError> {
        let guard = self.state.lock().unwrap();

        if let Some(access_token_ids) = guard.token_hashes_by_account_id.get(account_id) {
            let access_tokens: Vec<_> = access_token_ids
                .iter()
                .map(|token_id| guard.tokens_by_id.get(token_id).unwrap().clone())
                .skip(pagination.offset)
                .take(pagination.limit)
                .collect();
            return Ok(access_tokens);
        }
        Ok(vec![])
    }

    async fn mark_revoked(
        &self,
        token_id: &Uuid,
        revoked_time: DateTime<Utc>,
    ) -> Result<(), RevokeTokenError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(existing_token) = guard.tokens_by_id.get(token_id) {
            if existing_token.revoked_at.is_some() {
                return Err(RevokeTokenError::AlreadyRevoked);
            }
            let revoked_access_token = AccessToken {
                revoked_at: Some(revoked_time),
                ..existing_token.clone()
            };
            guard.tokens_by_id.insert(*token_id, revoked_access_token);

            return Ok(());
        }
        Err(RevokeTokenError::NotFound(AccessTokenNotFoundError {
            access_token_id: *token_id,
        }))
    }

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
        token_hash: [u8; 32],
    ) -> Result<Account, FindAccountByTokenError> {
        let access_token = self
            .get_token_by_id(token_id)
            .await
            .map_err(|err| match err {
                GetAccessTokenError::NotFound(err) => FindAccountByTokenError::NotFound(err),
                GetAccessTokenError::Internal(err) => FindAccountByTokenError::Internal(err),
            })?;

        if access_token.revoked_at.is_some() {
            return Err(FindAccountByTokenError::NotFound(
                AccessTokenNotFoundError {
                    access_token_id: *token_id,
                },
            ));
        }
        if token_hash != access_token.token_hash {
            return Err(FindAccountByTokenError::InvalidTokenHash);
        }

        let account = self
            .account_repository
            .get_account_by_id(&access_token.account_id)
            .await
            .map_err(|err| FindAccountByTokenError::Internal(err.int_err()))?;

        Ok(account)
    }
}
