// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use thiserror::Error;

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub enum ResourceKind {
    Dataset,
    Repository,
}

#[derive(Error, Debug)]
pub enum DomainError {
    #[error("{kind:?} {id} does not exist")]
    DoesNotExist {
        kind: ResourceKind,
        id: String,
        backtrace: Backtrace,
    },
    #[error("{kind:?} {id} already exists")]
    AlreadyExists {
        kind: ResourceKind,
        id: String,
        backtrace: Backtrace,
    },
    #[error("{from_kind:?} {from_id} references non existent {to_kind:?} {to_id}")]
    MissingReference {
        from_kind: ResourceKind,
        from_id: String,
        to_kind: ResourceKind,
        to_id: String,
        backtrace: Backtrace,
    },
    #[error("{to_kind:?} {to_id} is referenced by {from_kinds_ids:?}")]
    DanglingReference {
        from_kinds_ids: Vec<(ResourceKind, String)>,
        to_kind: ResourceKind,
        to_id: String,
        backtrace: Backtrace,
    },
    #[error("Bad input: {message}")]
    BadInput {
        message: String,
        backtrace: Backtrace,
    },
    #[error("Underlying storage is read-only")]
    ReadOnly,
    #[error("{0}")]
    InfraError(BoxedError),
}

impl DomainError {
    pub fn already_exists(kind: ResourceKind, id: String) -> DomainError {
        DomainError::AlreadyExists {
            kind: kind,
            id: id,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn does_not_exist(kind: ResourceKind, id: String) -> DomainError {
        DomainError::DoesNotExist {
            kind: kind,
            id: id,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn missing_reference(
        from_kind: ResourceKind,
        from_id: String,
        to_kind: ResourceKind,
        to_id: String,
    ) -> DomainError {
        DomainError::MissingReference {
            from_kind: from_kind,
            from_id: from_id,
            to_kind: to_kind,
            to_id: to_id,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn dangling_reference(
        from_kinds_ids: Vec<(ResourceKind, String)>,
        to_kind: ResourceKind,
        to_id: String,
    ) -> Self {
        Self::DanglingReference {
            from_kinds_ids: from_kinds_ids,
            to_kind: to_kind,
            to_id: to_id,
            backtrace: Backtrace::capture(),
        }
    }

    pub fn bad_input<S: Into<String>>(msg: S) -> Self {
        Self::BadInput {
            message: msg.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
