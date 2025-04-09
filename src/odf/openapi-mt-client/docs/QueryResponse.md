# QueryResponse

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**commitment** | Option<[**models::Commitment**](Commitment.md)> | Succinct commitment | [optional]
**input** | Option<[**models::QueryRequest**](QueryRequest.md)> | Inputs that can be used to fully reproduce the query | [optional]
**output** | [**models::Outputs**](Outputs.md) | Query results | 
**proof** | Option<[**models::Proof**](Proof.md)> | Signature block | [optional]
**sub_queries** | Option<[**Vec<models::SubQuery>**](SubQuery.md)> | Information about processing performed by other nodes as part of this operation | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


