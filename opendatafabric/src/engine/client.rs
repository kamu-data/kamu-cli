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

        let response_grpc = self.client.execute_query(request_grpc).await?;

        println!("RESPONSE={:?}", response_grpc);

        let response = FlatbuffersEngineProtocol
            .read_execute_query_response(&response_grpc.get_ref().flatbuffer)
            .unwrap();

        println!("RESPONSE={:?}", response);

        Ok(())
    }
}
