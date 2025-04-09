# QueryRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**data_format** | Option<[**models::DataFormat**](DataFormat.md)> | How data should be laid out in the response | [optional]
**datasets** | Option<[**Vec<models::DatasetState>**](DatasetState.md)> | Optional information used to affix an alias to the specific [`odf::DatasetID`] and reproduce the query at a specific state in time | [optional]
**include** | Option<[**Vec<models::Include>**](Include.md)> | What information to include | [optional]
**limit** | Option<**i64**> | Pagination: limits number of records in response to N | [optional]
**query** | **String** | Query string | 
**query_dialect** | Option<[**models::QueryDialect**](QueryDialect.md)> | Dialect of the query | [optional]
**schema_format** | Option<[**models::SchemaFormat**](SchemaFormat.md)> | What representation to use for the schema | [optional]
**skip** | Option<**i64**> | Pagination: skips first N records | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


