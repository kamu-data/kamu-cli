// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::*;
use thiserror::Error;

use super::grpc_generated::engine_client::EngineClient as EngineClientGRPC;
use super::grpc_generated::ExecuteQueryRequest as ExecuteQueryRequestGRPC;
use crate::serde::flatbuffers::FlatbuffersEngineProtocol;
use crate::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use crate::{
    ExecuteQueryRequest,
    ExecuteQueryResponse,
    ExecuteQueryResponseInternalError,
    ExecuteQueryResponseInvalidQuery,
    ExecuteQueryResponseSuccess,
};

pub struct EngineGrpcClient {
    client: EngineClientGRPC<tonic::transport::Channel>,
}

impl EngineGrpcClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self, tonic::transport::Error> {
        let client = EngineClientGRPC::connect(format!("http://{}:{}", host, port)).await?;

        Ok(Self { client })
    }

    pub async fn execute_query(
        &mut self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, ExecuteQueryError> {
        let fb = FlatbuffersEngineProtocol
            .write_execute_query_request(&request)
            .int_err()?;

        let request_grpc = tonic::Request::new(ExecuteQueryRequestGRPC {
            flatbuffer: fb.collapse_vec(),
        });

        let mut stream = self
            .client
            .execute_query(request_grpc)
            .await
            .int_err()?
            .into_inner();

        while let Some(response_grpc) = stream.message().await.int_err()? {
            let response = FlatbuffersEngineProtocol
                .read_execute_query_response(&response_grpc.flatbuffer)
                .int_err()?;

            match response {
                ExecuteQueryResponse::Progress => (),
                ExecuteQueryResponse::Success(success) => return Ok(success),
                ExecuteQueryResponse::InvalidQuery(error) => return Err(error.into()),
                ExecuteQueryResponse::InternalError(error) => return Err(error.into()),
            }
        }

        Err(
            "Engine did not transmit Success or Error message in the response stream"
                .int_err()
                .into(),
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ExecuteQueryError {
    #[error("Invalid query: {0}")]
    InvalidQuery(ExecuteQueryResponseInvalidQuery),
    #[error("Engine internal error: {0}")]
    EngineInternalError(ExecuteQueryResponseInternalError),
    #[error(transparent)]
    InternalError(#[from] InternalError),
}

impl From<ExecuteQueryResponseInvalidQuery> for ExecuteQueryError {
    fn from(error: ExecuteQueryResponseInvalidQuery) -> Self {
        Self::InvalidQuery(error)
    }
}

impl From<ExecuteQueryResponseInternalError> for ExecuteQueryError {
    fn from(error: ExecuteQueryResponseInternalError) -> Self {
        Self::EngineInternalError(error)
    }
}
