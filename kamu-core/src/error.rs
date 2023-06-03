// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Test

use std::backtrace::Backtrace;
use std::convert::From;

use kamu_domain::DomainError;
use thiserror::Error;

#[deprecated]
#[derive(Error, Debug)]
pub enum InfraError {
    #[error("IO error: {source}")]
    IOError {
        #[from]
        source: std::io::Error,
        backtrace: Backtrace,
    },
    #[error("{0}")]
    SerdeError(#[from] SerdeError),
}

#[deprecated]
#[derive(Error, Debug)]
pub enum SerdeError {
    #[error("Yaml serialization error: {source}")]
    SerdeYamlError {
        #[from]
        source: serde_yaml::Error,
        backtrace: Backtrace,
    },
}

impl From<serde_yaml::Error> for InfraError {
    fn from(v: serde_yaml::Error) -> Self {
        Self::from(SerdeError::from(v))
    }
}

impl std::convert::Into<DomainError> for InfraError {
    fn into(self) -> DomainError {
        DomainError::InfraError(Box::new(self))
    }
}
