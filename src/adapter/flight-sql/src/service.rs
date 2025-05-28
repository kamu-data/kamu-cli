// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::pin::Pin;
use std::string::ToString;
use std::sync::Arc;

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::sql::metadata::SqlInfoData;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginSavepointRequest,
    ActionBeginSavepointResult,
    ActionBeginTransactionRequest,
    ActionBeginTransactionResult,
    ActionCancelQueryRequest,
    ActionCancelQueryResult,
    ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest,
    ActionEndTransactionRequest,
    CommandGetCatalogs,
    CommandGetCrossReference,
    CommandGetDbSchemas,
    CommandGetExportedKeys,
    CommandGetImportedKeys,
    CommandGetPrimaryKeys,
    CommandGetSqlInfo,
    CommandGetTableTypes,
    CommandGetTables,
    CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate,
    CommandStatementQuery,
    CommandStatementSubstraitPlan,
    CommandStatementUpdate,
    DoPutPreparedStatementResult,
    ProstMessageExt,
    SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action,
    FlightDescriptor,
    FlightEndpoint,
    FlightInfo,
    HandshakeRequest,
    HandshakeResponse,
    Ticket,
};
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, BinaryArray, Int32Array, StringArray, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DFSchema;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use prost::Message;
use prost::bytes::Bytes;
use tonic::codegen::tokio_stream::Stream;
use tonic::metadata::MetadataValue;
use tonic::{Request, Response, Status, Streaming};

use crate::{PlanId, SessionAuth, SessionManager};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TABLE_TYPES: [&str; 2] = ["TABLE", "VIEW"];

const CLOSE_SESSION: &str = "CloseSession";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// KamuFlightSqlService
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct KamuFlightSqlService {
    sql_info: Arc<SqlInfoData>,
    // LazyOnce<T> ensures that these objects are instantiated once but only when they are needed -
    // this is important because during some operations like `handshake` the `SessionId` is not
    // available so an attempt to instantiate a `SessionManager` may fail
    session_auth: LazyOnce<Arc<dyn SessionAuth>>,
    session_manager: LazyOnce<Arc<dyn SessionManager>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
impl KamuFlightSqlService {
    pub fn new(
        sql_info: Arc<SqlInfoData>,
        session_auth: dill::Lazy<Arc<dyn SessionAuth>>,
        session_manager: dill::Lazy<Arc<dyn SessionManager>>,
    ) -> Self {
        Self {
            sql_info,
            session_auth: LazyOnce::new(session_auth),
            session_manager: LazyOnce::new(session_manager),
        }
    }

    fn get_sql_info(
        &self,
        query: &CommandGetSqlInfo,
        _schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        self.sql_info
            .record_batch(query.info.clone())
            .map_err(|e| Status::internal(format!("Error: {e}")))
    }

    fn get_table_types(&self, schema_only: bool) -> Result<RecordBatch, Status> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "table_type",
            DataType::Utf8,
            false,
        )]));

        let col_table_type = if schema_only {
            Vec::new()
        } else {
            TABLE_TYPES.to_vec()
        };

        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(col_table_type))])
            .map_err(|e| Status::internal(format!("Error: {e}")))
    }

    #[expect(clippy::trivially_copy_pass_by_ref)]
    fn get_catalogs(
        &self,
        ctx: &SessionContext,
        _query: &CommandGetCatalogs,
        schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let batch_schema = Arc::new(Schema::new(vec![Field::new(
            "catalog_name",
            DataType::Utf8,
            true,
        )]));

        let col_catalog_name = if schema_only {
            Vec::new()
        } else {
            ctx.catalog_names()
        };

        RecordBatch::try_new(
            batch_schema,
            vec![Arc::new(StringArray::from(col_catalog_name))],
        )
        .map_err(|e| Status::internal(format!("Error: {e}")))
    }

    fn get_schemas(
        &self,
        ctx: &SessionContext,
        query: &CommandGetDbSchemas,
        schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let db_schema_filter_pattern = if let Some(pat) = &query.db_schema_filter_pattern {
            pat.as_str()
        } else {
            "%"
        };

        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, false),
        ]));

        let mut col_catalog_name = Vec::new();
        let mut col_db_schema_name = Vec::new();

        if !schema_only {
            for catalog_name in ctx.catalog_names() {
                if let Some(catalog_name_filter) = &query.catalog {
                    if catalog_name != *catalog_name_filter {
                        continue;
                    }
                }
                let catalog = ctx.catalog(&catalog_name).unwrap();
                for schema_name in catalog.schema_names() {
                    if like::Like::<false>::not_like(schema_name.as_str(), db_schema_filter_pattern)
                        .unwrap()
                    {
                        continue;
                    }

                    col_catalog_name.push(catalog_name.clone());
                    col_db_schema_name.push(schema_name.clone());
                }
            }
        }

        RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(col_catalog_name)),
                Arc::new(StringArray::from(col_db_schema_name)),
            ],
        )
        .map_err(|e| Status::internal(format!("Error: {e}")))
    }

    async fn get_tables(
        &self,
        ctx: Arc<SessionContext>,
        query: &CommandGetTables,
        schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let db_schema_filter_pattern = if let Some(pat) = &query.db_schema_filter_pattern {
            pat.as_str()
        } else {
            "%"
        };
        let table_name_filter_pattern = if let Some(pat) = &query.table_name_filter_pattern {
            pat.as_str()
        } else {
            "%"
        };

        let mut fields = vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
        ];
        if query.include_schema {
            fields.push(Field::new("table_schema", DataType::Binary, false));
        }
        let batch_schema = Arc::new(Schema::new(fields));

        // TODO: PERF: Use query.table_name_filter_pattern to reduce search space

        // TODO: Can significantly reduce allocations using dict builders
        let mut col_catalog_name = Vec::new();
        let mut col_db_schema_name = Vec::new();
        let mut col_table_name = Vec::new();
        let mut col_table_type = Vec::new();
        let mut col_table_schema = Vec::new();

        if !schema_only {
            for catalog_name in ctx.catalog_names() {
                if let Some(catalog_name_filter) = &query.catalog {
                    if catalog_name != *catalog_name_filter {
                        continue;
                    }
                }
                let catalog = ctx.catalog(&catalog_name).unwrap();
                for schema_name in catalog.schema_names() {
                    if like::Like::<false>::not_like(schema_name.as_str(), db_schema_filter_pattern)
                        .unwrap()
                    {
                        continue;
                    }
                    let schema = catalog.schema(&schema_name).unwrap();
                    for table_name in schema.table_names() {
                        if like::Like::<false>::not_like(
                            table_name.as_str(),
                            table_name_filter_pattern,
                        )
                        .unwrap()
                        {
                            continue;
                        }

                        let table = schema.table(&table_name).await.unwrap().unwrap();

                        col_catalog_name.push(catalog_name.clone());
                        col_db_schema_name.push(schema_name.clone());
                        col_table_name.push(table_name.clone());
                        col_table_type.push("TABLE");

                        if query.include_schema {
                            let schema_bytes = self.schema_to_arrow(&table.schema())?;
                            col_table_schema.push(schema_bytes);
                        }
                    }
                }
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(col_catalog_name)),
            Arc::new(StringArray::from(col_db_schema_name)),
            Arc::new(StringArray::from(col_table_name)),
            Arc::new(StringArray::from(col_table_type)),
        ];
        if query.include_schema {
            columns.push(Arc::new(BinaryArray::from_vec(
                col_table_schema.iter().map(|v| &v[..]).collect(),
            )));
        }

        let rb = RecordBatch::try_new(batch_schema, columns)
            .map_err(|e| Status::internal(format!("Error getting tables: {e}")))?;

        Ok(rb)
    }

    // TODO: Get keys from externalized metadata
    fn get_primary_keys(
        &self,
        _ctx: &Arc<SessionContext>,
        _query: &CommandGetPrimaryKeys,
        _schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("catalog_name", DataType::Utf8, true),
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, false),
            Field::new("key_name", DataType::Utf8, true),
            Field::new("key_sequence", DataType::Int32, false),
        ]));

        let rb = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
            ],
        )
        .map_err(|e| Status::internal(format!("Error: {e}")))?;

        Ok(rb)
    }

    // TODO: Get keys from externalized metadata
    fn get_exported_keys(
        &self,
        _ctx: &Arc<SessionContext>,
        _query: &CommandGetExportedKeys,
        _schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("pk_catalog_name", DataType::Utf8, true),
            Field::new("pk_db_schema_name", DataType::Utf8, true),
            Field::new("pk_table_name", DataType::Utf8, false),
            Field::new("pk_column_name", DataType::Utf8, false),
            Field::new("fk_catalog_name", DataType::Utf8, true),
            Field::new("fk_db_schema_name", DataType::Utf8, true),
            Field::new("fk_table_name", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("fk_key_name", DataType::Utf8, true),
            Field::new("pk_key_name", DataType::Utf8, true),
            Field::new("update_rule", DataType::UInt8, false),
            Field::new("delete_rule", DataType::UInt8, false),
        ]));

        let rb = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(UInt8Array::from(Vec::<u8>::new())),
                Arc::new(UInt8Array::from(Vec::<u8>::new())),
            ],
        )
        .map_err(|e| Status::internal(format!("Error: {e}")))?;

        Ok(rb)
    }

    // TODO: Get keys from externalized metadata
    fn get_imported_keys(
        &self,
        _ctx: &Arc<SessionContext>,
        _query: &CommandGetImportedKeys,
        _schema_only: bool,
    ) -> Result<RecordBatch, Status> {
        let batch_schema = Arc::new(Schema::new(vec![
            Field::new("pk_catalog_name", DataType::Utf8, true),
            Field::new("pk_db_schema_name", DataType::Utf8, true),
            Field::new("pk_table_name", DataType::Utf8, false),
            Field::new("pk_column_name", DataType::Utf8, false),
            Field::new("fk_catalog_name", DataType::Utf8, true),
            Field::new("fk_db_schema_name", DataType::Utf8, true),
            Field::new("fk_table_name", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
            Field::new("key_sequence", DataType::Int32, false),
            Field::new("fk_key_name", DataType::Utf8, true),
            Field::new("pk_key_name", DataType::Utf8, true),
            Field::new("update_rule", DataType::UInt8, false),
            Field::new("delete_rule", DataType::UInt8, false),
        ]));

        let rb = RecordBatch::try_new(
            batch_schema,
            vec![
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(StringArray::from(Vec::<String>::new())),
                Arc::new(Int32Array::from(Vec::<i32>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(StringArray::from(Vec::<Option<String>>::new())),
                Arc::new(UInt8Array::from(Vec::<u8>::new())),
                Arc::new(UInt8Array::from(Vec::<u8>::new())),
            ],
        )
        .map_err(|e| Status::internal(format!("Error: {e}")))?;

        Ok(rb)
    }

    async fn prepare_statement(query: &str, ctx: &SessionContext) -> Result<LogicalPlan, Status> {
        let plan = ctx
            .sql(query)
            .await
            .and_then(DataFrame::into_optimized_plan)
            .map_err(|e| Status::internal(format!("Error building plan: {e}")))?;
        Ok(plan)
    }

    fn df_schema_to_arrow(&self, schema: &DFSchema) -> Result<Vec<u8>, Status> {
        let arrow_schema: Schema = schema.clone().into();
        let schema_bytes = self.schema_to_arrow(&arrow_schema)?;
        Ok(schema_bytes)
    }

    fn schema_to_arrow(&self, arrow_schema: &Schema) -> Result<Vec<u8>, Status> {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let write_options = IpcWriteOptions::default();
        let data_gen = IpcDataGenerator::default();
        let encoded_data = data_gen.schema_to_bytes_with_dictionary_tracker(
            arrow_schema,
            &mut dictionary_tracker,
            &write_options,
        );
        let mut schema_bytes = vec![];
        arrow::ipc::writer::write_message(&mut schema_bytes, encoded_data, &write_options)
            .map_err(|e| Status::internal(format!("Error encoding schema: {e}")))?;
        Ok(schema_bytes)
    }

    fn record_batch_to_flight_info(
        &self,
        data: &RecordBatch,
        ticket: &arrow_flight::sql::Any,
        schema_only: bool,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket: prost::bytes::Bytes = ticket.encode_to_vec().into();

        let mut total_records = -1;
        let mut total_bytes = -1;

        if !schema_only {
            total_records = i64::try_from(data.num_rows())
                .map_err(|e| Status::internal(format!("\"num_rows\" convert error: {e}")))?;

            total_bytes = i64::try_from(data.get_array_memory_size()).map_err(|e| {
                Status::internal(format!("\"get_array_memory_size\" convert error: {e}"))
            })?;
        }

        let schema = data.schema();
        let schema_bytes = self.schema_to_arrow(&schema)?;

        // Note: Leaving location empty per documentation:
        //
        // If the list is empty, the expectation is that the ticket can only be redeemed
        // on the current service where the ticket was generated.
        //
        // See: https://github.com/apache/arrow-datafusion/blob/01ff53771a5e866813ef9636e3f7eec6b88ce4a4/datafusion-examples/examples/flight/flight_sql_server.rs#L300

        let fieps = vec![FlightEndpoint {
            ticket: Some(Ticket { ticket }),
            location: vec![],
            expiration_time: None,
            app_metadata: Bytes::new(),
        }];

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Vec::new().into(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes.into(),
            flight_descriptor: Some(flight_desc),
            endpoint: fieps,
            total_records,
            total_bytes,
            ordered: false,
            app_metadata: Bytes::new(),
        };
        tracing::debug!(
            schema = ?schema.as_ref(),
            flight_info = ?info,
            "Prepared FlightInfo for record batch",
        );
        Ok(Response::new(info))
    }

    fn df_to_flight_info(
        &self,
        df: &DataFrame,
        ticket: &arrow_flight::sql::Any,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket: prost::bytes::Bytes = ticket.encode_to_vec().into();

        let schema_bytes = self.df_schema_to_arrow(df.schema())?;

        // Note: Leaving location empty per documentation:
        //
        // If the list is empty, the expectation is that the ticket can only be redeemed
        // on the current service where the ticket was generated.
        //
        // See: https://github.com/apache/arrow-datafusion/blob/01ff53771a5e866813ef9636e3f7eec6b88ce4a4/datafusion-examples/examples/flight/flight_sql_server.rs#L300

        let fieps = vec![FlightEndpoint {
            ticket: Some(Ticket { ticket }),
            location: vec![],
            expiration_time: None,
            app_metadata: Bytes::new(),
        }];

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Vec::new().into(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes.into(),
            flight_descriptor: Some(flight_desc),
            endpoint: fieps,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
            app_metadata: Bytes::new(),
        };
        tracing::debug!(
            schema = ?df.schema(),
            flight_info = ?info,
            "Prepared FlightInfo for data frame",
        );
        Ok(Response::new(info))
    }

    fn record_batch_to_stream(
        &self,
        rb: RecordBatch,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let flights = batches_to_flight_data(&rb.schema(), vec![rb])
            .map_err(|_| Status::internal("Error encoding batches".to_string()))?;

        let stream = futures::stream::iter(flights.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    // TODO: PERF: Use DataFrame::execute_stream() not to load keep entire result in
    // memory
    #[tracing::instrument(level = "info", skip_all)]
    async fn df_to_stream(
        &self,
        df: DataFrame,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let schema: Schema = df.schema().clone().into();

        let mut batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("Error executing plan: {e}")))?;

        // TODO: FIXME: There seems to be some issue with JDBC connector where a
        // non-empty result that consists of some empty batches is considered
        // fully empty by the client. Thus below we filter out empty
        // batches manually. Empty batches often happen in GROUP BY queries - we should
        // dig in and file an issue.
        let first_batch = batches[0].clone();
        batches.retain(|b| b.num_rows() != 0);

        // Add an empty batch back if entire result is empty
        if batches.is_empty() {
            batches.push(first_batch);
        }

        let flights = batches_to_flight_data(&schema, batches)
            .map_err(|_| Status::internal("Error encoding batches".to_string()))?;

        let stream = futures::stream::iter(flights.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn do_action_close_session(&self, _request: Request<Action>) -> Result<(), Status> {
        self.session_manager.close_session().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// FlightSqlService
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(unused_variables)]
#[tonic::async_trait]
impl FlightSqlService for KamuFlightSqlService {
    type FlightService = KamuFlightSqlService;

    #[tracing::instrument(level = "debug", skip_all, fields(?request))]
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        use base64::Engine;
        use base64::engine::{GeneralPurpose, GeneralPurposeConfig};

        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let b64engine = GeneralPurpose::new(
            &base64::alphabet::STANDARD,
            GeneralPurposeConfig::new()
                .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
        );
        let bytes = b64engine
            .decode(base64)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let str = String::from_utf8(bytes)
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        let parts: Vec<_> = str.split(':').collect();
        if parts.len() != 2 {
            Err(Status::invalid_argument("Invalid authorization header"))?;
        }
        let username = parts[0];
        let password = parts[1];

        let session_token = self.session_auth.auth_basic(username, password).await?;

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: session_token.as_bytes().to_vec().into(),
        };
        let result = Ok(result);
        let output: futures::stream::Iter<std::vec::IntoIter<Result<HandshakeResponse, Status>>> =
            futures::stream::iter(vec![result]);
        let mut resp: Response<Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>> =
            Response::new(Box::pin(output));
        let md = MetadataValue::try_from(format!("Bearer {session_token}"))
            .map_err(|_| Status::invalid_argument("authorization not parsable"))?;
        resp.metadata_mut().insert("authorization", md);
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(type_url = %message.type_url))]
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(format!(
            "do_get: The defined request is invalid: {}",
            message.type_url
        )))
    }

    /// Get a FlightDataStream containing the data related to the supported XDBC
    /// types.
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_xdbc_type_info"))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let plan = Self::prepare_statement(&query.query, &ctx).await?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Status::internal(format!("Error executing plan: {e}")))?;

        let ticket = TicketStatementQuery {
            statement_handle: query.encode_to_vec().into(),
        };

        let resp = self.df_to_flight_info(&df, &ticket.as_any())?;
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let plan_id = PlanId(
            String::from_utf8(query.prepared_statement_handle.to_vec())
                .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?,
        );

        let plan = self.session_manager.get_plan(&plan_id).await?;

        let ctx = self.session_manager.get_context().await?;

        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Status::internal(format!("Error executing plan: {e}")))?;

        let resp = self.df_to_flight_info(&df, &query.as_any())?;
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_catalogs(&ctx, &query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_schemas(&ctx, &query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_tables(ctx, &query, true).await?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let _ctx = self.session_manager.get_context().await?;
        let data = self.get_table_types(true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let _ctx = self.session_manager.get_context().await?;
        let data = self.get_sql_info(&query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_primary_keys(&ctx, &query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_exported_keys(&ctx, &query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_imported_keys(&ctx, &query, true)?;
        self.record_batch_to_flight_info(&data, &query.as_any(), true)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_cross_reference",
        ))
    }

    /// Get a FlightInfo to extract information about the supported XDBC types.
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_xdbc_type_info",
        ))
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;

        let query = CommandStatementQuery::decode(ticket.statement_handle)
            .map_err(|e| Status::internal(format!("Invalid ticket: {e}")))?;

        tracing::debug!(?query, "Decoded query");

        let plan = Self::prepare_statement(&query.query, &ctx).await?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Status::internal(format!("Error executing plan: {e}")))?;

        self.df_to_stream(df).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let plan_id = PlanId(
            String::from_utf8(query.prepared_statement_handle.to_vec())
                .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?,
        );

        let plan = self.session_manager.get_plan(&plan_id).await?;

        let ctx = self.session_manager.get_context().await?;

        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| Status::internal(format!("Error executing plan: {e}")))?;

        self.df_to_stream(df).await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_catalogs(&ctx, &query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_schemas(&ctx, &query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_tables(ctx, &query, false).await?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _ctx = self.session_manager.get_context().await?;
        let data = self.get_table_types(false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let _ctx = self.session_manager.get_context().await?;
        let data = self.get_sql_info(&query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_primary_keys(&ctx, &query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_exported_keys(&ctx, &query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let ctx = self.session_manager.get_context().await?;
        let data = self.get_imported_keys(&ctx, &query, false)?;
        self.record_batch_to_stream(data)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Implement do_get_cross_reference"))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?ticket))]
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Implement do_put_statement_update"))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_query",
        ))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?handle))]
    async fn do_put_prepared_statement_update(
        &self,
        handle: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "Implement do_put_prepared_statement_update",
        ))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let ctx = self.session_manager.get_context().await?;

        let plan = Self::prepare_statement(&query.query, &ctx).await?;
        let schema_bytes = self.df_schema_to_arrow(plan.schema())?;
        let plan_token = self.session_manager.cache_plan(plan).await?;

        tracing::debug!(%plan_token, "Prepared statement");

        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: plan_token.as_bytes().to_vec().into(),
            dataset_schema: schema_bytes.into(),
            parameter_schema: Vec::new().into(), // TODO: parameters
        };
        Ok(res)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        let plan_id = PlanId(
            String::from_utf8(query.prepared_statement_handle.to_vec())
                .map_err(|e| Status::internal(format!("Error decoding handle: {e}")))?,
        );

        self.session_manager.remove_plan(&plan_id).await?;

        Ok(())
    }

    /// Get a FlightInfo for executing a substrait plan.
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn get_flight_info_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "Implement get_flight_info_substrait_plan",
        ))
    }

    /// Execute a substrait plan
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_put_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented("Implement do_put_substrait_plan"))
    }

    /// Create a prepared substrait plan.
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_create_prepared_substrait_plan(
        &self,
        query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    /// Begin a transaction
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_begin_transaction(
        &self,
        query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    /// End a transaction
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    /// Begin a savepoint
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_begin_savepoint(
        &self,
        query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    /// End a savepoint
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_end_savepoint(
        &self,
        query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    /// Cancel a query
    #[tracing::instrument(level = "debug", skip_all, fields(?query))]
    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    /// Register a new SqlInfo result, making it available when calling
    /// GetSqlInfo.
    #[tracing::instrument(level = "debug", skip_all, fields(%id, ?result))]
    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {}

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        // TODO: Base interface should handle CloseSession action
        // See: https://github.com/apache/arrow-rs/issues/6516
        if request.get_ref().r#type == CLOSE_SESSION {
            self.do_action_close_session(request).await?;
            Ok(Response::new(Box::pin(futures::stream::empty())))
        } else {
            Err(Status::invalid_argument(format!(
                "do_action: The defined request is invalid: {:?}",
                request.get_ref().r#type
            )))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Consider upstreaming into `dill`
// One downside to this type is that it panics on ingestion errors rather than
// returning them
struct LazyOnce<T> {
    f: dill::Lazy<T>,
    v: std::sync::OnceLock<T>,
}

impl<T> LazyOnce<T> {
    pub fn new(f: dill::Lazy<T>) -> Self {
        Self {
            f,
            v: std::sync::OnceLock::new(),
        }
    }
}

impl<T> std::ops::Deref for LazyOnce<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.v.get_or_init(|| self.f.get().unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
