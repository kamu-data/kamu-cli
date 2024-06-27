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
use super::grpc_generated::{
    RawQueryRequest as RawQueryRequestGRPC,
    TransformRequest as TransformRequestGRPC,
};
use crate::serde::flatbuffers::FlatbuffersEngineProtocol;
use crate::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use crate::{
    RawQueryRequest,
    RawQueryResponse,
    RawQueryResponseInternalError,
    RawQueryResponseInvalidQuery,
    RawQueryResponseSuccess,
    TransformRequest,
    TransformResponse,
    TransformResponseInternalError,
    TransformResponseInvalidQuery,
    TransformResponseSuccess,
};

pub struct EngineGrpcClient {
    client: EngineClientGRPC<tonic::transport::Channel>,
}

impl EngineGrpcClient {
    pub async fn connect(host: &str, port: u16) -> Result<Self, tonic::transport::Error> {
        let client = EngineClientGRPC::connect(format!("http://{host}:{port}")).await?;

        Ok(Self { client })
    }

    pub async fn execute_raw_query(
        &mut self,
        request: RawQueryRequest,
    ) -> Result<RawQueryResponseSuccess, ExecuteRawQueryError> {
        let fb = FlatbuffersEngineProtocol
            .write_raw_query_request(&request)
            .int_err()?;

        let request_grpc = tonic::Request::new(RawQueryRequestGRPC {
            flatbuffer: fb.collapse_vec(),
        });

        let mut stream = self
            .client
            .execute_raw_query(request_grpc)
            .await
            .int_err()?
            .into_inner();

        while let Some(response_grpc) = stream.message().await.int_err()? {
            let response = FlatbuffersEngineProtocol
                .read_raw_query_response(&response_grpc.flatbuffer)
                .int_err()?;

            match response {
                RawQueryResponse::Progress(_progress) => (),
                RawQueryResponse::Success(success) => return Ok(success),
                RawQueryResponse::InvalidQuery(error) => return Err(error.into()),
                RawQueryResponse::InternalError(error) => return Err(error.into()),
            }
        }

        Err(
            "Engine did not transmit Success or Error message in the response stream"
                .int_err()
                .into(),
        )
    }

    pub async fn execute_transform(
        &mut self,
        request: TransformRequest,
    ) -> Result<TransformResponseSuccess, ExecuteTransformError> {
        let fb = FlatbuffersEngineProtocol
            .write_transform_request(&request)
            .int_err()?;

        let request_grpc = tonic::Request::new(TransformRequestGRPC {
            flatbuffer: fb.collapse_vec(),
        });

        let mut stream = self
            .client
            .execute_transform(request_grpc)
            .await
            .int_err()?
            .into_inner();

        while let Some(response_grpc) = stream.message().await.int_err()? {
            let response = FlatbuffersEngineProtocol
                .read_transform_response(&response_grpc.flatbuffer)
                .int_err()?;

            match response {
                TransformResponse::Progress(_progress) => (),
                TransformResponse::Success(success) => return Ok(success),
                TransformResponse::InvalidQuery(error) => return Err(error.into()),
                TransformResponse::InternalError(error) => return Err(error.into()),
            }
        }

        Err(
            "Engine did not transmit Success or Error message in the response stream"
                .int_err()
                .into(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ExecuteRawQueryError {
    #[error("Invalid query: {0}")]
    InvalidQuery(RawQueryResponseInvalidQuery),
    #[error("Engine internal error: {0}")]
    EngineInternalError(RawQueryResponseInternalError),
    #[error(transparent)]
    InternalError(#[from] InternalError),
}

impl From<RawQueryResponseInvalidQuery> for ExecuteRawQueryError {
    fn from(error: RawQueryResponseInvalidQuery) -> Self {
        Self::InvalidQuery(error)
    }
}

impl From<RawQueryResponseInternalError> for ExecuteRawQueryError {
    fn from(error: RawQueryResponseInternalError) -> Self {
        Self::EngineInternalError(error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ExecuteTransformError {
    #[error("Invalid query: {0}")]
    InvalidQuery(TransformResponseInvalidQuery),
    #[error("Engine internal error: {0}")]
    EngineInternalError(TransformResponseInternalError),
    #[error(transparent)]
    InternalError(#[from] InternalError),
}

impl From<TransformResponseInvalidQuery> for ExecuteTransformError {
    fn from(error: TransformResponseInvalidQuery) -> Self {
        Self::InvalidQuery(error)
    }
}

impl From<TransformResponseInternalError> for ExecuteTransformError {
    fn from(error: TransformResponseInternalError) -> Self {
        Self::EngineInternalError(error)
    }
}
