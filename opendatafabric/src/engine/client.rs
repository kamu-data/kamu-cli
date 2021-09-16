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

    pub async fn say_hello(&mut self) -> Result<(), tonic::Status> {
        let request = tonic::Request::new(ExecuteQueryRequestGRPC {
            flatbuffer: Vec::new(),
        });

        let response = self.client.execute_query(request).await?;

        println!("RESPONSE={:?}", response);

        Ok(())
    }
}
