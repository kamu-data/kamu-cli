// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow_flight::sql::metadata::SqlInfoDataBuilder;
use arrow_flight::sql::{
    SqlInfo,
    SqlNullOrdering,
    SqlSupportedCaseSensitivity,
    SqlSupportedTransactions,
    SupportedSqlGrammar,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn default_sql_info() -> SqlInfoDataBuilder {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "Unknown");
    builder.append(SqlInfo::FlightSqlServerVersion, "0.0.0");
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransactions::SqlTransactionUnspecified as i32,
    );
    // don't yet support `CancelQuery` action
    builder.append(SqlInfo::FlightSqlServerCancel, false);
    builder.append(SqlInfo::FlightSqlServerStatementTimeout, 0i32);
    builder.append(SqlInfo::FlightSqlServerTransactionTimeout, 0i32);
    // SQL syntax information
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.append(
        SqlInfo::SqlIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityLowercase as i32,
    );
    builder.append(SqlInfo::SqlIdentifierQuoteChar, r#"""#);
    builder.append(
        SqlInfo::SqlQuotedIdentifierCase,
        SqlSupportedCaseSensitivity::SqlCaseSensitivityCaseInsensitive as i32,
    );
    builder.append(SqlInfo::SqlAllTablesAreSelectable, true);
    builder.append(
        SqlInfo::SqlNullOrdering,
        SqlNullOrdering::SqlNullsSortedHigh as i32,
    );
    // builder.append(SqlInfo::SqlKeywords, SQL_INFO_SQL_KEYWORDS);
    // builder.append(SqlInfo::SqlNumericFunctions, SQL_INFO_NUMERIC_FUNCTIONS);
    // builder.append(SqlInfo::SqlStringFunctions, SQL_INFO_STRING_FUNCTIONS);
    // builder.append(SqlInfo::SqlSystemFunctions, SQL_INFO_SYSTEM_FUNCTIONS);
    // builder.append(SqlInfo::SqlDatetimeFunctions, SQL_INFO_DATE_TIME_FUNCTIONS);
    builder.append(SqlInfo::SqlSearchStringEscape, "\\");
    builder.append(SqlInfo::SqlExtraNameCharacters, "");
    builder.append(SqlInfo::SqlSupportsColumnAliasing, true);
    builder.append(SqlInfo::SqlNullPlusNullIsNull, true);
    // Skip SqlSupportsConvert (which is the map of the conversions that are
    // supported); .with_sql_info(SqlInfo::SqlSupportsConvert, TBD);
    builder.append(SqlInfo::SqlSupportsTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsDifferentTableCorrelationNames, false);
    builder.append(SqlInfo::SqlSupportsExpressionsInOrderBy, true);
    builder.append(SqlInfo::SqlSupportsOrderByUnrelated, true);
    builder.append(SqlInfo::SqlSupportedGroupBy, 3i32);
    builder.append(SqlInfo::SqlSupportsLikeEscapeClause, true);
    builder.append(SqlInfo::SqlSupportsNonNullableColumns, true);
    builder.append(
        SqlInfo::SqlSupportedGrammar,
        SupportedSqlGrammar::SqlCoreGrammar as i32,
    );
    // report we support all ansi 92
    builder.append(SqlInfo::SqlAnsi92SupportedLevel, 0b111_i32);
    builder.append(SqlInfo::SqlSupportsIntegrityEnhancementFacility, false);
    builder.append(SqlInfo::SqlOuterJoinsSupportLevel, 2i32);
    builder.append(SqlInfo::SqlSchemaTerm, "schema");
    builder.append(SqlInfo::SqlProcedureTerm, "procedure");
    builder.append(SqlInfo::SqlCatalogAtStart, false);
    builder.append(SqlInfo::SqlSchemasSupportedActions, 0i32);
    builder.append(SqlInfo::SqlCatalogsSupportedActions, 0i32);
    builder.append(SqlInfo::SqlSupportedPositionedCommands, 0i32);
    builder.append(SqlInfo::SqlSelectForUpdateSupported, false);
    builder.append(SqlInfo::SqlStoredProceduresSupported, false);
    builder.append(SqlInfo::SqlSupportedSubqueries, 15i32);
    builder.append(SqlInfo::SqlCorrelatedSubqueriesSupported, true);
    builder.append(SqlInfo::SqlSupportedUnions, 3i32);
    // For max lengths, report max arrow string length
    builder.append(SqlInfo::SqlMaxBinaryLiteralLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCharLiteralLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInGroupBy, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInIndex, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInOrderBy, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInSelect, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxColumnsInTable, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxConnections, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCursorNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxIndexLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlDbSchemaNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxProcedureNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxCatalogNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxRowSize, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxRowSizeIncludesBlobs, true);
    builder.append(SqlInfo::SqlMaxStatementLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxStatements, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxTableNameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxTablesInSelect, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlMaxUsernameLength, i64::from(i32::MAX));
    builder.append(SqlInfo::SqlDefaultTransactionIsolation, 0i64);
    builder.append(SqlInfo::SqlTransactionsSupported, false);
    builder.append(SqlInfo::SqlSupportedTransactionsIsolationLevels, 0i32);
    builder.append(SqlInfo::SqlDataDefinitionCausesTransactionCommit, false);
    builder.append(SqlInfo::SqlDataDefinitionsInTransactionsIgnored, true);
    builder.append(SqlInfo::SqlSupportedResultSetTypes, 0i32);
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetUnspecified,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetForwardOnly,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollSensitive,
        0i32,
    );
    builder.append(
        SqlInfo::SqlSupportedConcurrenciesForResultSetScrollInsensitive,
        0i32,
    );
    builder.append(SqlInfo::SqlBatchUpdatesSupported, false);
    builder.append(SqlInfo::SqlSavepointsSupported, false);
    builder.append(SqlInfo::SqlNamedParametersSupported, false);
    builder.append(SqlInfo::SqlLocatorsUpdateCopy, false);
    builder.append(SqlInfo::SqlStoredFunctionsUsingCallSyntaxSupported, false);
    builder
}
