# \KamuOdataApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**odata_collection_handler_mt**](KamuOdataApi.md#odata_collection_handler_mt) | **GET** /odata/{account_name}/{dataset_name} | OData collection
[**odata_metadata_handler_mt**](KamuOdataApi.md#odata_metadata_handler_mt) | **GET** /odata/{account_name}/$metadata | OData service metadata
[**odata_service_handler_mt**](KamuOdataApi.md#odata_service_handler_mt) | **GET** /odata/{account_name} | OData root service description



## odata_collection_handler_mt

> String odata_collection_handler_mt(account_name, dataset_name)
OData collection

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Account name | [required] |
**dataset_name** | **String** | Dataset name | [required] |

### Return type

**String**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## odata_metadata_handler_mt

> String odata_metadata_handler_mt(account_name)
OData service metadata

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Account name | [required] |

### Return type

**String**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## odata_service_handler_mt

> String odata_service_handler_mt(account_name)
OData root service description

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Account name | [required] |

### Return type

**String**

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: text/plain

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

