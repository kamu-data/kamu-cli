// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
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
    SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::{
    Action,
    FlightDescriptor,
    FlightInfo,
    HandshakeRequest,
    HandshakeResponse,
    Ticket,
};
use internal_error::{InternalError, ResultIntoInternal};
use tonic::codegen::tokio_stream::Stream;
use tonic::{Request, Response, Status, Streaming};

use crate::KamuFlightSqlService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// This type is a singleton that is called by GRPC server. For it to play nicely
// with DB transactions we follow the same pattern as in Axum where middleware
// layers are responsible for attaching the Catalog to incoming requests. This
// wrapper will extract the catalog from the reuqest extensions and instantiate
// the inner service in the request context.
pub struct KamuFlightSqlServiceWrapper;

impl KamuFlightSqlServiceWrapper {
    async fn exec<Req, F, Fut, Ret>(&self, mut request: Request<Req>, f: F) -> Result<Ret, Status>
    where
        F: FnOnce(Request<Req>, Arc<KamuFlightSqlService>) -> Fut,
        Fut: std::future::Future<Output = Result<Ret, Status>>,
    {
        let Some(catalog) = request.extensions_mut().remove::<dill::Catalog>() else {
            return Err(Status::internal("Catalog extension is not configured"));
        };

        let transaction_runner = database_common::DatabaseTransactionRunner::new(catalog);

        transaction_runner
            .transactional(|tx_catalog: dill::Catalog| async move {
                let inner: Arc<KamuFlightSqlService> = tx_catalog.get_one().int_err()?;
                Ok(f(request, inner).await)
            })
            .await
            .map_err(internal_error::<InternalError>)?
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with a macro
#[expect(unused_variables)]
#[tonic::async_trait]
impl FlightSqlService for KamuFlightSqlServiceWrapper {
    type FlightService = KamuFlightSqlServiceWrapper;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        self.exec(request, |request, inner| async move {
            inner.do_handshake(request).await
        })
        .await
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_fallback(request, message).await
        })
        .await
    }

    async fn do_get_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_xdbc_type_info(query, request).await
        })
        .await
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_statement(query, request).await
        })
        .await
    }

    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner
                .get_flight_info_prepared_statement(query, request)
                .await
        })
        .await
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_catalogs(query, request).await
        })
        .await
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_schemas(query, request).await
        })
        .await
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_tables(query, request).await
        })
        .await
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_table_types(query, request).await
        })
        .await
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_sql_info(query, request).await
        })
        .await
    }

    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_primary_keys(query, request).await
        })
        .await
    }

    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_exported_keys(query, request).await
        })
        .await
    }

    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_imported_keys(query, request).await
        })
        .await
    }

    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_cross_reference(query, request).await
        })
        .await
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_xdbc_type_info(query, request).await
        })
        .await
    }

    async fn do_get_statement(
        &self,
        query: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_statement(query, request).await
        })
        .await
    }

    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_prepared_statement(query, request).await
        })
        .await
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_catalogs(query, request).await
        })
        .await
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_schemas(query, request).await
        })
        .await
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_tables(query, request).await
        })
        .await
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_table_types(query, request).await
        })
        .await
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_sql_info(query, request).await
        })
        .await
    }

    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_primary_keys(query, request).await
        })
        .await
    }

    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_exported_keys(query, request).await
        })
        .await
    }

    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_imported_keys(query, request).await
        })
        .await
    }

    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_get_cross_reference(query, request).await
        })
        .await
    }

    async fn do_put_statement_update(
        &self,
        query: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_put_statement_update(query, request).await
        })
        .await
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_put_prepared_statement_query(query, request).await
        })
        .await
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_put_prepared_statement_update(query, request).await
        })
        .await
    }

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.exec(request, |request, inner| async move {
            inner
                .do_action_create_prepared_statement(query, request)
                .await
        })
        .await
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.exec(request, |request, inner| async move {
            inner
                .do_action_close_prepared_statement(query, request)
                .await
        })
        .await
    }

    async fn get_flight_info_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.exec(request, |request, inner| async move {
            inner.get_flight_info_substrait_plan(query, request).await
        })
        .await
    }

    async fn do_put_substrait_plan(
        &self,
        query: CommandStatementSubstraitPlan,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_put_substrait_plan(query, request).await
        })
        .await
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        query: ActionCreatePreparedSubstraitPlanRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.exec(request, |request, inner| async move {
            inner
                .do_action_create_prepared_substrait_plan(query, request)
                .await
        })
        .await
    }

    async fn do_action_begin_transaction(
        &self,
        query: ActionBeginTransactionRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_begin_transaction(query, request).await
        })
        .await
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_end_transaction(query, request).await
        })
        .await
    }

    async fn do_action_begin_savepoint(
        &self,
        query: ActionBeginSavepointRequest,
        request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_begin_savepoint(query, request).await
        })
        .await
    }

    async fn do_action_end_savepoint(
        &self,
        query: ActionEndSavepointRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_end_savepoint(query, request).await
        })
        .await
    }

    async fn do_action_cancel_query(
        &self,
        query: ActionCancelQueryRequest,
        request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_cancel_query(query, request).await
        })
        .await
    }

    async fn register_sql_info(&self, id: i32, result: &SqlInfo) {}

    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        self.exec(request, |request, inner| async move {
            inner.do_action_fallback(request).await
        })
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn internal_error<E: std::error::Error>(error: E) -> Status {
    tracing::error!(
        error = ?error,
        error_msg = %error,
        "Internal error",
    );
    Status::internal("Internal error")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
