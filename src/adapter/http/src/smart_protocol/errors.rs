// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display};

use internal_error::{BoxedError, InternalError};
use kamu_core::OverwriteSeedBlockError;
use kamu_datasets::NameCollisionError;
use thiserror::Error;

use super::phases::*;
use crate::ws_common::{ReadMessageError, WriteMessageError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct PullReadError {
    pub read_error: ReadMessageError,
    pub pull_phase: PullPhase,
}

impl PullReadError {
    pub fn new(read_error: ReadMessageError, pull_phase: PullPhase) -> Self {
        Self {
            read_error,
            pull_phase,
        }
    }
}

impl Display for PullReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read_error: {}, pull_phase: {:?})",
            self.read_error, self.pull_phase
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct PushReadError {
    pub read_error: ReadMessageError,
    pub push_phase: PushPhase,
}

impl PushReadError {
    pub fn new(read_error: ReadMessageError, push_phase: PushPhase) -> Self {
        Self {
            read_error,
            push_phase,
        }
    }
}

impl Display for PushReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "read_error: {}, push_phase: {:?})",
            self.read_error, self.push_phase
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct PullWriteError {
    write_error: WriteMessageError,
    pull_phase: PullPhase,
}

impl PullWriteError {
    pub fn new(write_error: WriteMessageError, pull_phase: PullPhase) -> Self {
        Self {
            write_error,
            pull_phase,
        }
    }
}

impl Display for PullWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "write_error: {}, pull_phase: {:?})",
            self.write_error, self.pull_phase
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct PushWriteError {
    write_error: WriteMessageError,
    push_phase: PushPhase,
}

impl PushWriteError {
    pub fn new(write_error: WriteMessageError, push_phase: PushPhase) -> Self {
        Self {
            write_error,
            push_phase,
        }
    }
}

impl Display for PushWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "write_error: {}, push_phase: {:?})",
            self.write_error, self.push_phase
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PullServerError {
    #[error(transparent)]
    ReadFailed(PullReadError),

    #[error(transparent)]
    WriteFailed(PullWriteError),

    #[error(transparent)]
    Internal(PhaseInternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PullClientError {
    #[error("Overwriting dataset id or kind is restricted")]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(PullReadError),

    #[error(transparent)]
    WriteFailed(PullWriteError),

    #[error(transparent)]
    InvalidInterval(odf::dataset::InvalidIntervalError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PushServerError {
    #[error(transparent)]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(PushReadError),

    #[error(transparent)]
    WriteFailed(PushWriteError),

    #[error(transparent)]
    RefCollision(odf::dataset::RefCollisionError),

    #[error(transparent)]
    NameCollision(NameCollisionError),

    #[error(transparent)]
    Internal(PhaseInternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PushClientError {
    #[error(transparent)]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(PushReadError),

    #[error(transparent)]
    WriteFailed(PushWriteError),

    #[error(transparent)]
    InvalidHead(odf::dataset::RefCASError),

    #[error(transparent)]
    RefCollision(odf::dataset::RefCollisionError),

    #[error(transparent)]
    NameCollision(NameCollisionError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ClientInternalError {
    phase: TransferPhase,
    details: String,
}

impl ClientInternalError {
    pub fn new(msg: &str, phase: TransferPhase) -> Self {
        Self {
            phase,
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ClientInternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} during phase: {}", self.details, self.phase)
    }
}

impl std::error::Error for ClientInternalError {
    fn description(&self) -> &str {
        &self.details
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct ObjectUploadError {
    pub response: reqwest::Response,
}

impl fmt::Display for ObjectUploadError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "ObjectUploadError: status={}", self.response.status())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct PhaseInternalError {
    pub phase: TransferPhase,
    pub error: InternalError,
}

impl fmt::Display for PhaseInternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Smart protocol phase internal error: phase={}, error={}",
            self.phase, self.error
        )
    }
}

impl From<PhaseInternalError> for PushServerError {
    fn from(value: PhaseInternalError) -> Self {
        PushServerError::Internal(value)
    }
}

impl From<PhaseInternalError> for PullServerError {
    fn from(value: PhaseInternalError) -> Self {
        PullServerError::Internal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ErrorIntoProtocolInternal {
    fn protocol_int_err<P>(self, phase: P) -> PhaseInternalError
    where
        P: Into<TransferPhase>;
}

impl<E> ErrorIntoProtocolInternal for E
where
    E: Into<BoxedError>,
{
    fn protocol_int_err<P>(self, phase: P) -> PhaseInternalError
    where
        P: Into<TransferPhase>,
    {
        PhaseInternalError {
            phase: phase.into(),
            error: InternalError::new(self),
        }
    }
}

pub trait ResultIntoProtocolInternal<OK> {
    fn protocol_int_err<P>(self, phase: P) -> Result<OK, PhaseInternalError>
    where
        P: Into<TransferPhase>;
}

impl<OK, E> ResultIntoProtocolInternal<OK> for Result<OK, E>
where
    E: Into<BoxedError>,
{
    fn protocol_int_err<P>(self, phase: P) -> Result<OK, PhaseInternalError>
    where
        P: Into<TransferPhase>,
    {
        match self {
            Ok(ok) => Ok(ok),
            Err(e) => Err(e.protocol_int_err(phase)),
        }
    }
}
