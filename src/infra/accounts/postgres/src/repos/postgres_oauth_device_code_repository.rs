// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{TransactionRef, TransactionRefT};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresOAuthDeviceCodeRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[dill::component(pub)]
#[dill::interface(dyn OAuthDeviceCodeRepository)]
impl PostgresOAuthDeviceCodeRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl OAuthDeviceCodeRepository for PostgresOAuthDeviceCodeRepository {
    async fn save_device_code(
        &self,
        device_code_created: &DeviceTokenCreated,
    ) -> Result<(), CreateDeviceCodeError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO oauth_device_codes(device_code, device_code_created_at, device_code_expires_at)
            VALUES ($1, $2, $3);
            "#,
            device_code_created.device_code.as_ref(),
            device_code_created.created_at,
            device_code_created.expires_at,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| {
            use CreateDeviceCodeError as E;
            match e {
                sqlx::Error::Database(e) if e.is_unique_violation() => {
                    E::Duplicate(DeviceCodeDuplicateError { device_code: device_code_created.device_code.clone() })
                }
                _ => E::Internal(e.int_err())
            }
        })?;

        Ok(())
    }

    async fn update_device_token_with_token_params_part(
        &self,
        device_code: &DeviceCode,
        token_params_part: &DeviceTokenParamsPart,
    ) -> Result<(), UpdateDeviceCodeWithTokenParamsPartError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        let account_id = token_params_part.account_id.as_stack_string();
        let token_iat: i64 = token_params_part.iat.try_into().int_err()?;
        let token_exp: i64 = token_params_part.exp.try_into().int_err()?;

        let update_result = sqlx::query!(
            r#"
            UPDATE oauth_device_codes
            SET token_iat  = $2,
                token_exp  = $3,
                account_id = $4
            WHERE device_code = $1
            "#,
            device_code.as_ref(),
            token_iat,
            token_exp,
            account_id.as_str(),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if update_result.rows_affected() == 0 {
            return Err(DeviceTokenNotFoundError::new(device_code.clone()).into());
        }

        Ok(())
    }

    async fn find_device_token_by_device_code(
        &self,
        device_code: &DeviceCode,
    ) -> Result<DeviceToken, FindDeviceTokenByDeviceCodeError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_device_token_row_model = sqlx::query_as!(
            DeviceTokenRowModel,
            r#"
            SELECT device_code,
                   device_code_created_at,
                   device_code_expires_at,
                   token_iat,
                   token_exp,
                   token_last_used_at,
                   account_id
            FROM oauth_device_codes
            WHERE device_code = $1
            "#,
            device_code.as_ref()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(device_token_row_model) = maybe_device_token_row_model {
            let device_token = device_token_row_model.try_into()?;

            Ok(device_token)
        } else {
            Err(DeviceTokenNotFoundError::new(device_code.clone()).into())
        }
    }

    async fn cleanup_expired_device_codes(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredDeviceCodesError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            DELETE
            FROM oauth_device_codes
            WHERE device_code_expires_at <= $1
            "#,
            now
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
