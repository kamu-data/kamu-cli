// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error(transparent)]
    SqlxError(#[from] sqlx::Error),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatabaseInitError {
    #[error(transparent)]
    SchemaTooNew(#[from] DatabaseSchemaTooNewError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The database has migrations applied that this build does not know, so it
/// was created or upgraded by a newer version of the application.
#[derive(Error, Debug)]
#[error(
    "Database schema version {latest_applied_version} is newer than the latest version \
     {latest_known_version} supported by this build"
)]
pub struct DatabaseSchemaTooNewError {
    pub latest_applied_version: i64,
    pub latest_known_version: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
