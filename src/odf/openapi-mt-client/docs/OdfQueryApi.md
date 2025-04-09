# \OdfQueryApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**dataset_metadata_handler**](OdfQueryApi.md#dataset_metadata_handler) | **GET** /{account_name}/{dataset_name}/metadata | Access dataset metadata chain
[**dataset_tail_handler**](OdfQueryApi.md#dataset_tail_handler) | **GET** /{account_name}/{dataset_name}/tail | Get a sample of latest events
[**query_handler**](OdfQueryApi.md#query_handler) | **GET** /query | Execute a batch query
[**query_handler_post**](OdfQueryApi.md#query_handler_post) | **POST** /query | Execute a batch query
[**verify_handler**](OdfQueryApi.md#verify_handler) | **POST** /verify | Verify query commitment



## dataset_metadata_handler

> models::DatasetMetadataResponse dataset_metadata_handler(account_name, dataset_name, include, schema_format)
Access dataset metadata chain

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |
**include** | Option<**String**> | What information to include in response |  |
**schema_format** | Option<[**SchemaFormat**](.md)> | Format to return the schema in |  |

### Return type

[**models::DatasetMetadataResponse**](DatasetMetadataResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_tail_handler

> models::DatasetTailResponse dataset_tail_handler(schema_format, account_name, dataset_name, skip, limit, data_format)
Get a sample of latest events

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**schema_format** | [**SchemaFormat**](.md) | How to encode the schema of the result | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |
**skip** | Option<**i64**> | Number of leading records to skip when returning result (used for pagination) |  |
**limit** | Option<**i64**> | Maximum number of records to return (used for pagination) |  |
**data_format** | Option<[**DataFormat**](.md)> | How the output data should be encoded |  |

### Return type

[**models::DatasetTailResponse**](DatasetTailResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## query_handler

> models::QueryResponse query_handler(query, schema_format, query_dialect, skip, limit, data_format, include)
Execute a batch query

Functions exactly like the [POST version](#tag/odf-query/POST/query) of the endpoint with all parameters passed in the query string instead of the body.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**query** | **String** | Query to execute (e.g. SQL) | [required] |
**schema_format** | [**SchemaFormat**](.md) | How to encode the schema of the result | [required] |
**query_dialect** | Option<**String**> | Dialect of the query |  |
**skip** | Option<**i64**> | Number of leading records to skip when returning result (used for pagination) |  |
**limit** | Option<**i64**> | Maximum number of records to return (used for pagination) |  |
**data_format** | Option<[**DataFormat**](.md)> | How the output data should be encoded |  |
**include** | Option<**String**> | What information to include in the response |  |

### Return type

[**models::QueryResponse**](QueryResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## query_handler_post

> models::QueryResponse query_handler_post(query_request)
Execute a batch query

### Regular Queries This endpoint lets you execute arbitrary SQL that can access multiple datasets at once.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\" } ```  Example response: ```json {     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     } } ```  ### Verifiable Queries [Cryptographic proofs](https://docs.kamu.dev/node/commitments) can be also requested to hold the node **forever accountable** for the provided result.  Example request body: ```json {     \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",     \"limit\": 3,     \"queryDialect\": \"SqlDataFusion\",     \"dataFormat\": \"JsonAoA\",     \"schemaFormat\": \"ArrowJson\",     \"include\": [\"proof\"] } ```  Currently, we support verifiability by ensuring that queries are deterministic and fully reproducible and signing the original response with Node's private key. In future more types of proofs will be supported.  Example response: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0119d20360650afd3d412c6b11529778b784c697559c0107d37ee5da61465726c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f1620708557a44c88d23c83f2b915abc10a41cc38d2a278e851e5dc6bb02b7e1f9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"output\": {         \"data\": [             [\"2024-09-02T21:50:00Z\", \"eth\", \"usd\", 2537.07],             [\"2024-09-02T21:51:00Z\", \"eth\", \"usd\", 2541.37],             [\"2024-09-02T21:52:00Z\", \"eth\", \"usd\", 2542.66]         ],         \"dataFormat\": \"JsonAoA\",         \"schema\": {\"fields\": [\"...\"]},         \"schemaFormat\": \"ArrowJson\"     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f1620e23f7d8cdde7504eadb86f3cdf34b3b1a7d71f10fe5b54b528dd803387422efc\",         \"outputHash\": \"f1620e91f4d3fa26bc4ca0c49d681c8b630550239b64d3cbcfd7c6c2d6ff45998b088\",         \"subQueriesHash\": \"f1620ca4510738395af1429224dd785675309c344b2b549632e20275c69b15ed1d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6MkkhJQPHpA41mTPLFgBeygnjeeADUSwuGDoF9pbGQsfwZp\",         \"proofValue\": \"uJfY3_g03WbmqlQG8TL-WUxKYU8ZoJaP14MzOzbnJedNiu7jpoKnCTNnDI3TYuaXv89vKlirlGs-5AN06mBseCg\"     } } ```  A client that gets a proof in response should perform [a few basic steps](https://docs.kamu.dev/node/commitments#response-validation) to validate the proof integrity. For example making sure that the DID in `proof.verificationMethod` actually corresponds to the node you're querying data from and that the signature in `proof.proofValue` is actually valid. Only after this you can use this proof to hold the node accountable for the result.  A proof can be stored long-term and then disputed at a later point using your own node or a 3rd party node you can trust via the [`/verify`](#tag/odf-query/POST/verify) endpoint.  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**query_request** | [**QueryRequest**](QueryRequest.md) |  | [required] |

### Return type

[**models::QueryResponse**](QueryResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## verify_handler

> models::VerifyResponse verify_handler(verify_request)
Verify query commitment

A query proof can be stored long-term and then disputed at a later point using this endpoint.  Example request: ```json {     \"input\": {         \"query\": \"select event_time, from, to, close from \\\"kamu/eth-to-usd\\\"\",         \"queryDialect\": \"SqlDataFusion\",         \"dataFormat\": \"JsonAoA\",         \"include\": [\"Input\", \"Proof\", \"Schema\"],         \"schemaFormat\": \"ArrowJson\",         \"datasets\": [{             \"id\": \"did:odf:fed0..26c4\",             \"alias\": \"kamu/eth-to-usd\",             \"blockHash\": \"f162..9a1a\"         }],         \"skip\": 0,         \"limit\": 3     },     \"subQueries\": [],     \"commitment\": {         \"inputHash\": \"f162..2efc\",         \"outputHash\": \"f162..b088\",         \"subQueriesHash\": \"f162..d210\"     },     \"proof\": {         \"type\": \"Ed25519Signature2020\",         \"verificationMethod\": \"did:key:z6Mk..fwZp\",         \"proofValue\": \"uJfY..seCg\"     } } ```  Example response: ```json {     \"ok\": false,     \"error\": {         \"kind\": \"VerificationFailed::OutputMismatch\",         \"actual_hash\": \"f162..c12a\",         \"expected_hash\": \"f162..2a2d\",         \"message\": \"Query was reproduced but resulted in output hash different from expected.                     This means that the output was either falsified, or the query                     reproducibility was not guaranteed by the system.\",     } } ```  See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**verify_request** | [**VerifyRequest**](VerifyRequest.md) |  | [required] |

### Return type

[**models::VerifyResponse**](VerifyResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

