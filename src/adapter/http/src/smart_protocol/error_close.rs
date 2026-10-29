// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::Serialize;
use serde::de::DeserializeOwned;

use super::messages::*;
use super::phases::{PushPhase, TransferPhase};
use crate::ws_common::{self, ReadMessageError, WriteMessageError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Close code of a session that ends with an error reply in the close reason
pub const ERROR_REPLY_CLOSE_CODE: u16 = 1011;

/// Close code of a push whose completion failed. A client that predates
/// `DatasetPushCompleteResponse` reads the reason of an
/// [`ERROR_REPLY_CLOSE_CODE`] close as its confirmation, but this code as a
/// disconnect, so it reports the push as failed
pub const PUSH_NOT_COMPLETED_CLOSE_CODE: u16 = 1013;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A close frame that carries an error reply as its reason
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorReplyClose {
    pub code: u16,
    pub reason: String,
}

impl ErrorReplyClose {
    /// The internal error any phase may end with. Its reason has the shape of
    /// the `Err` of the first phase's response
    pub fn internal(phase: TransferPhase) -> Result<Self, WriteMessageError> {
        let error = TransferInternalError {
            phase,
            error_message: "Internal error".to_string(),
        };
        if error.phase == TransferPhase::Push(PushPhase::CompleteRequest) {
            Self::push_not_completed(DatasetPushCompleteError::Internal(error))
        } else {
            Self::new(
                ERROR_REPLY_CLOSE_CODE,
                TransferInternalErrorReply::Err(TransferInternalErrorKind::Internal(error)),
            )
        }
    }

    /// A push whose completion failed
    pub fn push_not_completed(error: DatasetPushCompleteError) -> Result<Self, WriteMessageError> {
        Self::new(
            PUSH_NOT_COMPLETED_CLOSE_CODE,
            DatasetPushCompleteResponse::Err(error),
        )
    }

    /// Any other error reply of a phase
    pub fn reply<TPayload: Serialize>(payload: TPayload) -> Result<Self, WriteMessageError> {
        Self::new(ERROR_REPLY_CLOSE_CODE, payload)
    }

    fn new<TPayload: Serialize>(code: u16, payload: TPayload) -> Result<Self, WriteMessageError> {
        Ok(Self {
            code,
            reason: ws_common::payload_to_json(payload)?,
        })
    }

    /// Reads the reply a client expects from a close frame. A close that
    /// carries no error reply is a disconnect. A reason that is not the
    /// expected reply is read as the internal error any phase may end with,
    /// so the client learns the phase that failed; otherwise the raw reason is
    /// kept in the error
    pub fn read<TPayload: DeserializeOwned>(
        code: u16,
        reason: &str,
    ) -> Result<TPayload, ReadMessageError> {
        if code != ERROR_REPLY_CLOSE_CODE && code != PUSH_NOT_COMPLETED_CLOSE_CODE {
            return Err(ReadMessageError::ServerDisconnected);
        }

        serde_json::from_str::<TPayload>(reason).map_err(|source| {
            match serde_json::from_str::<TransferInternalErrorReply>(reason) {
                Ok(TransferInternalErrorReply::Err(TransferInternalErrorKind::Internal(e))) => {
                    ReadMessageError::ServerFailed {
                        phase: e.phase,
                        message: e.error_message,
                    }
                }
                Err(_) => ReadMessageError::UnreadableCloseReason {
                    reason: reason.to_string(),
                    source,
                },
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use super::*;

    // Error replies in a close frame cross versions in both directions, so these
    // tests pin how each side reads them

    #[test]
    fn test_failed_completion_fails_clients_that_predate_the_typed_reply() {
        for close in [
            ErrorReplyClose::push_not_completed(DatasetPushCompleteError::UpdatedConcurrently(
                DatasetPushUpdatedConcurrentlyError {},
            ))
            .unwrap(),
            ErrorReplyClose::push_not_completed(DatasetPushCompleteError::Internal(
                Replies::internal_error(r#"{"Push":"CompleteRequest"}"#),
            ))
            .unwrap(),
        ] {
            assert_matches!(ClientBeforeTypedReply::read_completion(&close), Err(_));

            // The same reply in an `Error` close would have read as a confirmation
            let error_close = ErrorReplyClose {
                code: ERROR_REPLY_CLOSE_CODE,
                reason: close.reason,
            };
            assert_matches!(ClientBeforeTypedReply::read_completion(&error_close), Ok(_));
        }
    }

    #[test]
    fn test_failed_completion_is_read_as_a_typed_error() {
        let close = ErrorReplyClose::push_not_completed(
            DatasetPushCompleteError::UpdatedConcurrently(DatasetPushUpdatedConcurrentlyError {}),
        )
        .unwrap();

        assert_matches!(
            ErrorReplyClose::read::<DatasetPushCompleteReply>(close.code, &close.reason),
            Ok(DatasetPushCompleteReply::Response(Err(
                DatasetPushCompleteError::UpdatedConcurrently(_)
            )))
        );
    }

    #[test]
    fn test_completion_internal_error_of_servers_that_predate_its_close_code_is_read() {
        let reason = r#"{"Err":{"Internal":{"phase":{"Push":"CompleteRequest"},"error_message":"Internal error"}}}"#;

        assert_matches!(
            ErrorReplyClose::read::<DatasetPushCompleteReply>(ERROR_REPLY_CLOSE_CODE, reason),
            Ok(DatasetPushCompleteReply::Response(Err(
                DatasetPushCompleteError::Internal(_)
            )))
        );
    }

    #[test]
    fn test_internal_error_reply_has_the_shape_of_a_first_phase_response() {
        let error = Replies::internal_error(r#"{"Pull":"MetadataRequest"}"#);

        assert_eq!(
            serde_json::to_string(&TransferInternalErrorReply::Err(
                TransferInternalErrorKind::Internal(error.clone())
            ))
            .unwrap(),
            serde_json::to_string(&DatasetPullResponse::Err(
                DatasetPullRequestError::Internal(error)
            ))
            .unwrap()
        );
    }

    #[test]
    fn test_internal_error_in_a_phase_without_one_names_the_server_phase() {
        let reason = r#"{"Err":{"Internal":{"phase":{"Pull":"MetadataRequest"},"error_message":"Internal error"}}}"#;

        assert_matches!(
            ErrorReplyClose::read::<DatasetMetadataPullResponse>(ERROR_REPLY_CLOSE_CODE, reason),
            Err(e) if e.to_string() == "Server failed in phase Pull(MetadataRequest): Internal error"
        );

        let reason = r#"{"Err":{"Internal":{"phase":{"Push":"MetadataRequest"},"error_message":"Internal error"}}}"#;

        assert_matches!(
            ErrorReplyClose::read::<DatasetPushMetadataResponse>(ERROR_REPLY_CLOSE_CODE, reason),
            Err(e) if e.to_string() == "Server failed in phase Push(MetadataRequest): Internal error"
        );
    }

    #[test]
    fn test_unreadable_close_reason_is_kept_in_the_error() {
        assert_matches!(
            ErrorReplyClose::read::<DatasetMetadataPullResponse>(ERROR_REPLY_CLOSE_CODE, "not json"),
            Err(e) if e.to_string().contains("\"not json\"")
        );
    }

    #[test]
    fn test_close_without_an_error_reply_is_a_disconnect() {
        assert_matches!(
            ErrorReplyClose::read::<DatasetMetadataPullResponse>(1000, ""),
            Err(e) if e.to_string() == "Server has disconnected unexpectedly"
        );
    }

    struct Replies;

    impl Replies {
        fn internal_error(phase_json: &str) -> TransferInternalError {
            serde_json::from_str(&format!(
                r#"{{"phase":{phase_json},"error_message":"Internal error"}}"#
            ))
            .unwrap()
        }
    }

    /// How a client that predates `DatasetPushCompleteResponse` reads the reply to
    /// its complete request: the reason of an `Error` close as the reply, and any
    /// other close as a disconnect
    struct ClientBeforeTypedReply;

    #[derive(serde::Deserialize, Debug)]
    struct ConfirmedBeforeTypedReply {}

    impl ClientBeforeTypedReply {
        fn read_completion(close: &ErrorReplyClose) -> Result<ConfirmedBeforeTypedReply, String> {
            if close.code == ERROR_REPLY_CLOSE_CODE {
                serde_json::from_str(&close.reason).map_err(|e| e.to_string())
            } else {
                Err("Client has disconnected unexpectedly".to_string())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
