use crate::serde::flatbuffers::FlatbuffersEngineProtocol;
use crate::serde::{EngineProtocolDeserializer, EngineProtocolSerializer};
use crate::ExecuteQueryRequest;

use super::generated::engine_client::EngineClient as EngineClientGRPC;
use super::generated::ExecuteQueryRequest as ExecuteQueryRequestGRPC;

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
    ) -> Result<(), tonic::Status> {
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

            println!("RESPONSE={:?}", response);
        }

        Ok(())
    }
}
