use crate::serde::flatbuffers::FlatbuffersEngineProtocol;
use crate::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use crate::{
    ExecuteQueryRequest, ExecuteQueryResponse, ExecuteQueryResponseError,
    ExecuteQueryResponseSuccess,
};

use super::generated::engine_client::EngineClient as EngineClientGRPC;
use super::generated::ExecuteQueryRequest as ExecuteQueryRequestGRPC;

use thiserror::Error;

pub struct EngineClient {
    client: EngineClientGRPC<tonic::transport::Channel>,
}

impl EngineClient {
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
            .unwrap();

        let request_grpc = tonic::Request::new(ExecuteQueryRequestGRPC {
            flatbuffer: fb.collapse_vec(),
        });

        let mut stream = self.client.execute_query(request_grpc).await?.into_inner();

        while let Some(response_grpc) = stream.message().await? {
            let response = FlatbuffersEngineProtocol
                .read_execute_query_response(&response_grpc.flatbuffer)
                .unwrap();

            match response {
                ExecuteQueryResponse::Progress => (),
                ExecuteQueryResponse::Success(success) => return Ok(success),
                ExecuteQueryResponse::Error(error) => return Err(error.into()),
            }
        }

        unreachable!("Engine did not transmit Success or Error message in the response stream")
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ExecuteQueryError {
    #[error("Engine error: {0}")]
    ErrorResponse(ExecuteQueryResponseError),
    #[error("Rpc error: {0}")]
    RpcError(#[from] tonic::Status),
}

impl From<ExecuteQueryResponseError> for ExecuteQueryError {
    fn from(error: ExecuteQueryResponseError) -> Self {
        Self::ErrorResponse(error)
    }
}
