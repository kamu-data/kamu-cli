# \OdfTransferApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**dataset_blocks_handler**](OdfTransferApi.md#dataset_blocks_handler) | **GET** /{account_name}/{dataset_name}/blocks/{block_hash} | Get block by hash
[**dataset_checkpoints_get_handler**](OdfTransferApi.md#dataset_checkpoints_get_handler) | **GET** /{account_name}/{dataset_name}/checkpoints/{physical_hash} | Get checkpoint by hash
[**dataset_checkpoints_put_handler**](OdfTransferApi.md#dataset_checkpoints_put_handler) | **PUT** /{account_name}/{dataset_name}/checkpoints/{physical_hash} | Upload checkpoint
[**dataset_data_get_handler**](OdfTransferApi.md#dataset_data_get_handler) | **GET** /{account_name}/{dataset_name}/data/{physical_hash} | Get data slice by hash
[**dataset_data_put_handler**](OdfTransferApi.md#dataset_data_put_handler) | **PUT** /{account_name}/{dataset_name}/data/{physical_hash} | Upload data slice
[**dataset_pull_ws_upgrade_handler**](OdfTransferApi.md#dataset_pull_ws_upgrade_handler) | **GET** /{account_name}/{dataset_name}/pull | Initiate pull via Smart Transfer Protocol
[**dataset_push_ws_upgrade_handler**](OdfTransferApi.md#dataset_push_ws_upgrade_handler) | **GET** /{account_name}/{dataset_name}/push | Initiate push via Smart Transfer Protocol
[**dataset_refs_handler**](OdfTransferApi.md#dataset_refs_handler) | **GET** /{account_name}/{dataset_name}/refs/{reference} | Get named block reference



## dataset_blocks_handler

> serde_json::Value dataset_blocks_handler(block_hash, account_name, dataset_name)
Get block by hash

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**block_hash** | **String** | Hash of the block | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_checkpoints_get_handler

> serde_json::Value dataset_checkpoints_get_handler(physical_hash, account_name, dataset_name)
Get checkpoint by hash

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**physical_hash** | **String** | Physical hash of the block | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_checkpoints_put_handler

> serde_json::Value dataset_checkpoints_put_handler(physical_hash, account_name, dataset_name, request_body)
Upload checkpoint

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**physical_hash** | **String** | Physical hash of the block | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |
**request_body** | [**Vec<i32>**](i32.md) |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/octet-stream
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_data_get_handler

> serde_json::Value dataset_data_get_handler(physical_hash, account_name, dataset_name)
Get data slice by hash

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**physical_hash** | **String** | Physical hash of the block | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_data_put_handler

> serde_json::Value dataset_data_put_handler(physical_hash, account_name, dataset_name, request_body)
Upload data slice

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**physical_hash** | **String** | Physical hash of the block | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |
**request_body** | [**Vec<i32>**](i32.md) |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/octet-stream
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_pull_ws_upgrade_handler

> serde_json::Value dataset_pull_ws_upgrade_handler(account_name, dataset_name)
Initiate pull via Smart Transfer Protocol

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_push_ws_upgrade_handler

> serde_json::Value dataset_push_ws_upgrade_handler(account_name, dataset_name)
Initiate push via Smart Transfer Protocol

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_refs_handler

> String dataset_refs_handler(reference, account_name, dataset_name)
Get named block reference

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**reference** | **String** | Name of the reference | [required] |
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |

### Return type

**String**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

