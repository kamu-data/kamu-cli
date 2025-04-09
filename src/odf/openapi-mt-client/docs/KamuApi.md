# \KamuApi

All URIs are relative to *http://localhost*

Method | HTTP request | Description
------------- | ------------- | -------------
[**account_handler**](KamuApi.md#account_handler) | **GET** /accounts/me | Get account information
[**dataset_info_handler**](KamuApi.md#dataset_info_handler) | **GET** /datasets/{id} | Get dataset info by ID
[**dataset_ingest_handler**](KamuApi.md#dataset_ingest_handler) | **POST** /{account_name}/{dataset_name}/ingest | Push data ingestion
[**platform_file_upload_get_handler**](KamuApi.md#platform_file_upload_get_handler) | **GET** /platform/file/upload/{upload_token} | Get file from temporary storage
[**platform_file_upload_post_handler**](KamuApi.md#platform_file_upload_post_handler) | **POST** /platform/file/upload/{upload_token} | Upload file to temporary storage
[**platform_file_upload_prepare_post_handler**](KamuApi.md#platform_file_upload_prepare_post_handler) | **POST** /platform/file/upload/prepare | Prepare file upload
[**platform_login_handler**](KamuApi.md#platform_login_handler) | **POST** /platform/login | Authenticate with the node
[**platform_token_validate_handler**](KamuApi.md#platform_token_validate_handler) | **GET** /platform/token/validate | Validate auth token



## account_handler

> models::AccountResponse account_handler()
Get account information

### Parameters

This endpoint does not need any parameter.

### Return type

[**models::AccountResponse**](AccountResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_info_handler

> models::DatasetInfoResponse dataset_info_handler(id)
Get dataset info by ID

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**id** | **String** | Dataset ID | [required] |

### Return type

[**models::DatasetInfoResponse**](DatasetInfoResponse.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## dataset_ingest_handler

> serde_json::Value dataset_ingest_handler(account_name, dataset_name, request_body, source_name, upload_token)
Push data ingestion

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**account_name** | **String** | Name of the account | [required] |
**dataset_name** | **String** | Name of the dataset | [required] |
**request_body** | [**Vec<i32>**](i32.md) |  | [required] |
**source_name** | Option<**String**> |  |  |
**upload_token** | Option<**String**> |  |  |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/octet-stream
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## platform_file_upload_get_handler

> serde_json::Value platform_file_upload_get_handler(upload_token)
Get file from temporary storage

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**upload_token** | **String** |  | [required] |

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/octet-stream

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## platform_file_upload_post_handler

> models::UploadContext platform_file_upload_post_handler(upload_token, request_body)
Upload file to temporary storage

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**upload_token** | **String** |  | [required] |
**request_body** | [**Vec<i32>**](i32.md) |  | [required] |

### Return type

[**models::UploadContext**](UploadContext.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: application/octet-stream
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## platform_file_upload_prepare_post_handler

> models::UploadContext platform_file_upload_prepare_post_handler(file_name, content_length, content_type)
Prepare file upload

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**file_name** | **String** |  | [required] |
**content_length** | **i32** |  | [required] |
**content_type** | Option<**String**> |  |  |

### Return type

[**models::UploadContext**](UploadContext.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## platform_login_handler

> models::LoginResponseBody platform_login_handler(login_request_body)
Authenticate with the node

### Parameters


Name | Type | Description  | Required | Notes
------------- | ------------- | ------------- | ------------- | -------------
**login_request_body** | [**LoginRequestBody**](LoginRequestBody.md) |  | [required] |

### Return type

[**models::LoginResponseBody**](LoginResponseBody.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: application/json
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)


## platform_token_validate_handler

> serde_json::Value platform_token_validate_handler()
Validate auth token

### Parameters

This endpoint does not need any parameter.

### Return type

[**serde_json::Value**](serde_json::Value.md)

### Authorization

[api_key](../README.md#api_key)

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

