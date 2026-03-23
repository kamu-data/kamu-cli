// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{ResourceValidateSpec, StorageProviderKind, ValueRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageSpec {
    pub provider: StorageProviderSpec,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageProviderSpec {
    LocalFs(StorageProviderSpecLocalFs),
    S3(StorageProviderSpecS3),
    Ipfs(StorageProviderSpecIpfs),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProviderSpecLocalFs {
    pub workspace_path: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProviderSpecS3 {
    pub bucket: ValueRef,
    pub region: ValueRef,
    pub prefix: Option<ValueRef>,
    pub endpoint: Option<ValueRef>,
    pub credentials: S3CredentialsSpec,
}

impl StorageProviderSpec {
    pub fn kind(&self) -> StorageProviderKind {
        match self {
            Self::LocalFs(_) => StorageProviderKind::LocalFs,
            Self::S3(_) => StorageProviderKind::S3,
            Self::Ipfs(_) => StorageProviderKind::Ipfs,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum S3CredentialsSpec {
    Anonymous,
    AccessKey {
        access_key: ValueRef,
        secret_key: ValueRef,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageProviderSpecIpfs {
    pub gateway: Option<ValueRef>,
    pub api_endpoint: Option<ValueRef>,
    pub auth_token: Option<ValueRef>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageSpec {
    pub fn total_references(&self) -> usize {
        match &self.provider {
            StorageProviderSpec::LocalFs(_) => 0,
            StorageProviderSpec::S3(spec) => spec.total_references(),
            StorageProviderSpec::Ipfs(spec) => spec.total_references(),
        }
    }

    fn validate_reference_name(name: &str) -> Result<(), StorageValidationError> {
        let valid = !name.is_empty()
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.');

        if valid {
            Ok(())
        } else {
            Err(StorageValidationError::InvalidReferenceName {
                name: name.to_string(),
            })
        }
    }

    fn validate_value_ref(
        value: &ValueRef,
        empty_literal_error: StorageValidationError,
    ) -> Result<(), StorageValidationError> {
        match value {
            ValueRef::Literal(v) => {
                if v.is_empty() {
                    Err(empty_literal_error)
                } else {
                    Ok(())
                }
            }
            ValueRef::VariableRef { variable_set, name } => {
                Self::validate_reference_name(variable_set)?;
                Self::validate_reference_name(name)
            }
            ValueRef::SecretRef { secret_set, name } => {
                Self::validate_reference_name(secret_set)?;
                Self::validate_reference_name(name)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageProviderSpecS3 {
    pub fn total_references(&self) -> usize {
        let mut total = 0;

        if self.bucket.is_reference() {
            total += 1;
        }

        if self.region.is_reference() {
            total += 1;
        }

        if let Some(prefix) = &self.prefix
            && prefix.is_reference()
        {
            total += 1;
        }

        if let Some(endpoint) = &self.endpoint
            && endpoint.is_reference()
        {
            total += 1;
        }

        match &self.credentials {
            S3CredentialsSpec::Anonymous => {}
            S3CredentialsSpec::AccessKey {
                access_key,
                secret_key,
            } => {
                if access_key.is_reference() {
                    total += 1;
                }
                if secret_key.is_reference() {
                    total += 1;
                }
            }
        }

        total
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl StorageProviderSpecIpfs {
    pub fn total_references(&self) -> usize {
        let mut total = 0;

        if let Some(gateway) = &self.gateway
            && gateway.is_reference()
        {
            total += 1;
        }

        if let Some(api_endpoint) = &self.api_endpoint
            && api_endpoint.is_reference()
        {
            total += 1;
        }

        if let Some(auth_token) = &self.auth_token
            && auth_token.is_reference()
        {
            total += 1;
        }

        total
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum StorageValidationError {
    #[error("storage provider spec is invalid: {0}")]
    InvalidProvider(String),

    #[error("local filesystem root path must not be empty")]
    EmptyLocalFsRootPath,

    #[error("s3 bucket literal must not be empty")]
    EmptyS3Bucket,

    #[error("s3 region literal must not be empty")]
    EmptyS3Region,

    #[error("s3 access key literal must not be empty")]
    EmptyS3AccessKey,

    #[error("s3 secret key literal must not be empty")]
    EmptyS3SecretKey,

    #[error("ipfs auth token literal must not be empty")]
    EmptyIpfsAuthToken,

    #[error("invalid reference name '{name}'")]
    InvalidReferenceName { name: String },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceValidateSpec for StorageSpec {
    type ValidationError = StorageValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError> {
        match &self.provider {
            StorageProviderSpec::LocalFs(spec) => {
                if spec.workspace_path.is_empty() {
                    return Err(StorageValidationError::EmptyLocalFsRootPath);
                }
            }

            StorageProviderSpec::S3(spec) => {
                Self::validate_value_ref(&spec.bucket, StorageValidationError::EmptyS3Bucket)?;
                Self::validate_value_ref(&spec.region, StorageValidationError::EmptyS3Region)?;

                if let Some(prefix) = &spec.prefix {
                    Self::validate_value_ref(prefix, StorageValidationError::EmptyS3Bucket)?;
                }

                if let Some(endpoint) = &spec.endpoint {
                    Self::validate_value_ref(endpoint, StorageValidationError::EmptyS3Region)?;
                }

                match &spec.credentials {
                    S3CredentialsSpec::Anonymous => {}
                    S3CredentialsSpec::AccessKey {
                        access_key,
                        secret_key,
                    } => {
                        Self::validate_value_ref(
                            access_key,
                            StorageValidationError::EmptyS3AccessKey,
                        )?;
                        Self::validate_value_ref(
                            secret_key,
                            StorageValidationError::EmptyS3SecretKey,
                        )?;
                    }
                }
            }

            StorageProviderSpec::Ipfs(spec) => {
                if let Some(gateway) = &spec.gateway {
                    Self::validate_value_ref(gateway, StorageValidationError::EmptyS3Region)?;
                }

                if let Some(api_endpoint) = &spec.api_endpoint {
                    Self::validate_value_ref(api_endpoint, StorageValidationError::EmptyS3Region)?;
                }

                if let Some(auth_token) = &spec.auth_token {
                    Self::validate_value_ref(
                        auth_token,
                        StorageValidationError::EmptyIpfsAuthToken,
                    )?;
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
