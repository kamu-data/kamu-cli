## `CLIConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `auth` | [`AuthConfig`](#authconfig) |  | Auth configuration |
| `database` | [`DatabaseConfig`](#databaseconfig) |  | Database connection configuration |
| `datasetEnvVars` | [`DatasetEnvVarsConfig`](#datasetenvvarsconfig) |  | Dataset environment variables configuration |
| `didEncryption` | [`DidSecretEncryptionConfig`](#didsecretencryptionconfig) |  | Did secret key encryption configuration |
| `engine` | [`EngineConfig`](#engineconfig) |  | Engine configuration |
| `extra` | [`ExtraConfig`](#extraconfig) |  | Experimental and temporary configuration options |
| `flowSystem` | [`FlowSystemConfig`](#flowsystemconfig) |  | Configuration for flow system |
| `frontend` | [`FrontendConfig`](#frontendconfig) |  | Data access and visualization configuration |
| `identity` | [`IdentityConfig`](#identityconfig) |  | UNSTABLE: Identity configuration |
| `outbox` | [`OutboxConfig`](#outboxconfig) |  | Messaging outbox configuration |
| `protocol` | [`ProtocolConfig`](#protocolconfig) |  | Network protocols configuration |
| `search` | [`SearchConfig`](#searchconfig) |  | Search configuration |
| `source` | [`SourceConfig`](#sourceconfig) |  | Source configuration |
| `uploads` | [`UploadsConfig`](#uploadsconfig) |  | Uploads configuration |
| `webhooks` | [`WebhooksConfig`](#webhooksconfig) |  | Configuration for webhooks |


## `AccountConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `accountName` | [`AccountName`](#accountname) | `V` |  |
| `accountType` | [`AccountType`](#accounttype) |  |  |
| `avatarUrl` | `string` |  |  |
| `displayName` | `string` |  | Auto-derived from `account_name` if omitted |
| `email` | [`Email`](#email) | `V` |  |
| `id` | [`AccountID`](#accountid) |  | Auto-derived from `account_name` if omitted |
| `password` | [`Password`](#password) | `V` |  |
| `properties` | `array` |  |  |
| `provider` | `string` |  |  |
| `registeredAt` | `string` |  |  |
| `treatDatasetsAsPublic` | `boolean` |  |  |


## `AccountID`

Base type: `string`


## `AccountName`

Base type: `string`


## `AccountPropertyName`

| Variants |
|---|
| `CanProvisionAccounts` |
| `Admin` |


## `AccountType`

| Variants |
|---|
| `User` |
| `Organization` |


## `AuthConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `allowAnonymous` | `boolean` |  |  |
| `users` | [`PredefinedAccountsConfig`](#predefinedaccountsconfig) |  |  |


## `ContainerRuntimeType`

| Variants |
|---|
| `Docker` |
| `Podman` |


## `DatabaseConfig`

| Variants |
|---|
| [`Sqlite`](#databaseconfigsqlite) |
| [`Postgres`](#databaseconfigpostgres) |
| [`MySql`](#databaseconfigmysql) |
| [`MariaDB`](#databaseconfigmariadb) |


## `DatabaseConfig::Sqlite`

| Field | Type | Required | Description |
|---|---|---|---|
| `databasePath` | `string` | `V` |  |
| `provider` | `string` | `V` |  |


## `DatabaseConfig::Postgres`

| Field | Type | Required | Description |
|---|---|---|---|
| `acquireTimeoutSecs` | `integer` |  |  |
| `credentialsPolicy` | [`DatabaseCredentialsPolicyConfig`](#databasecredentialspolicyconfig) | `V` |  |
| `databaseName` | `string` | `V` |  |
| `host` | `string` | `V` |  |
| `maxConnections` | `integer` |  |  |
| `maxLifetimeSecs` | `integer` |  |  |
| `port` | `integer` |  |  |
| `provider` | `string` | `V` |  |


## `DatabaseConfig::MySql`

| Field | Type | Required | Description |
|---|---|---|---|
| `acquireTimeoutSecs` | `integer` |  |  |
| `credentialsPolicy` | [`DatabaseCredentialsPolicyConfig`](#databasecredentialspolicyconfig) | `V` |  |
| `databaseName` | `string` | `V` |  |
| `host` | `string` | `V` |  |
| `maxConnections` | `integer` |  |  |
| `maxLifetimeSecs` | `integer` |  |  |
| `port` | `integer` |  |  |
| `provider` | `string` | `V` |  |


## `DatabaseConfig::MariaDB`

| Field | Type | Required | Description |
|---|---|---|---|
| `acquireTimeoutSecs` | `integer` |  |  |
| `credentialsPolicy` | [`DatabaseCredentialsPolicyConfig`](#databasecredentialspolicyconfig) | `V` |  |
| `databaseName` | `string` | `V` |  |
| `host` | `string` | `V` |  |
| `maxConnections` | `integer` |  |  |
| `maxLifetimeSecs` | `integer` |  |  |
| `port` | `integer` |  |  |
| `provider` | `string` | `V` |  |


## `DatabaseCredentialSourceConfig`

| Variants |
|---|
| [`RawPassword`](#databasecredentialsourceconfigrawpassword) |
| [`AwsSecret`](#databasecredentialsourceconfigawssecret) |
| [`AwsIamToken`](#databasecredentialsourceconfigawsiamtoken) |


## `DatabaseCredentialSourceConfig::RawPassword`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |
| `rawPassword` | `string` | `V` |  |
| `userName` | `string` | `V` |  |


## `DatabaseCredentialSourceConfig::AwsSecret`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |
| `secretName` | `string` | `V` |  |


## `DatabaseCredentialSourceConfig::AwsIamToken`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |
| `userName` | `string` | `V` |  |


## `DatabaseCredentialsPolicyConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `rotationFrequencyInMinutes` | `integer` |  |  |
| `source` | [`DatabaseCredentialSourceConfig`](#databasecredentialsourceconfig) | `V` |  |


## `DatasetEnvVarsConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `enabled` | `boolean` |  |  |
| `encryptionKey` | `string` |  | Represents the encryption key for the dataset env vars. This field is<br>required if `enabled` is `true` or `None`.<br><br>The encryption key must be a 32-character alphanumeric string, which<br>includes both uppercase and lowercase Latin letters (A-Z, a-z) and<br>digits (0-9).<br><br>To generate use:<br><br>tr -dc 'A-Za-z0-9' < /dev/urandom \| head -c 32; echo<br> |


## `DidSecretEncryptionConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `enabled` | `boolean` |  |  |
| `encryptionKey` | `string` |  | The encryption key must be a 32-character alphanumeric string, which<br>includes both uppercase and lowercase Latin letters (A-Z, a-z) and<br>digits (0-9).<br><br>To generate use:<br><br>tr -dc 'A-Za-z0-9' < /dev/urandom \| head -c 32; echo<br> |


## `DurationString`

Base type: `string`


## `Email`

Base type: `string`


## `EmbeddingsChunkerConfig`

| Variants |
|---|
| [`Simple`](#embeddingschunkerconfigsimple) |


## `EmbeddingsChunkerConfig::Simple`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |
| `splitParagraphs` | `boolean` |  |  |
| `splitSections` | `boolean` |  |  |


## `EmbeddingsEncoderConfig`

| Variants |
|---|
| [`Dummy`](#embeddingsencoderconfigdummy) |
| [`OpenAi`](#embeddingsencoderconfigopenai) |


## `EmbeddingsEncoderConfig::Dummy`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |


## `EmbeddingsEncoderConfig::OpenAi`

| Field | Type | Required | Description |
|---|---|---|---|
| `apiKey` | `string` |  |  |
| `dimensions` | `integer` |  |  |
| `kind` | `string` | `V` |  |
| `modelName` | `string` |  |  |
| `url` | `string` |  |  |


## `EngineConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `datafusionEmbedded` | [`EngineConfigDatafusion`](#engineconfigdatafusion) |  | Embedded Datafusion engine configuration |
| `images` | [`EngineImagesConfig`](#engineimagesconfig) |  | UNSTABLE: Default engine images |
| `maxConcurrency` | `integer` |  | Maximum number of engine operations that can be performed concurrently |
| `networkNs` | [`NetworkNamespaceType`](#networknamespacetype) |  | Type of the networking namespace (relevant when running in container<br>environments) |
| `runtime` | [`ContainerRuntimeType`](#containerruntimetype) |  | Type of the runtime to use when running the data processing engines |
| `shutdownTimeout` | [`DurationString`](#durationstring) |  | Timeout for waiting the engine container to stop gracefully |
| `startTimeout` | [`DurationString`](#durationstring) |  | Timeout for starting an engine container |


## `EngineConfigDatafusion`

| Field | Type | Required | Description |
|---|---|---|---|
| `base` | `object` |  | Base configuration options<br>See: `<https://datafusion.apache.org/user-guide/configs.html>` |
| `batchQuery` | `object` |  | Batch query-specific overrides to the base config |
| `compaction` | `object` |  | Compaction-specific overrides to the base config |
| `ingest` | `object` |  | Ingest-specific overrides to the base config |
| `useLegacyArrowBufferEncoding` | `boolean` |  | Makes arrow batches use contiguous `Binary` and `Utf8` encodings instead<br>of more modern `BinaryView` and `Utf8View`. This is only needed for<br>compatibility with some older libraries that don't yet support them.<br><br>See: [kamu-node#277](https://github.com/kamu-data/kamu-node/issues/277) |


## `EngineImagesConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `datafusion` | `string` |  | UNSTABLE: `Datafusion` engine image |
| `flink` | `string` |  | UNSTABLE: `Flink` engine image |
| `risingwave` | `string` |  | UNSTABLE: `RisingWave` engine image |
| `spark` | `string` |  | UNSTABLE: `Spark` engine image |


## `EthRpcEndpoint`

| Field | Type | Required | Description |
|---|---|---|---|
| `chainId` | `integer` | `V` |  |
| `chainName` | `string` | `V` |  |
| `nodeUrl` | `string` | `V` |  |


## `EthereumSourceConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `commitAfterBlocksScanned` | `integer` |  | Forces iteration to stop after the specified number of blocks were<br>scanned even if we didn't reach the target record number. This is useful<br>to not lose a lot of scanning progress in case of an RPC error. |
| `getLogsBlockStride` | `integer` |  | Default number of blocks to scan within one query to `eth_getLogs` RPC<br>endpoint. |
| `rpcEndpoints` | `array` |  | Default RPC endpoints to use if source does not specify one explicitly. |
| `useBlockTimestampFallback` | `boolean` |  | Many providers don't yet return `blockTimestamp` from `eth_getLogs` RPC<br>endpoint and in such cases `block_timestamp` column will be `null`.<br>If you enable this fallback the library will perform additional call to<br>`eth_getBlock` to populate the timestam, but this may result in<br>significant performance penalty when fetching many log records.<br><br>See: [ethereum/execution-apis#295](https://github.com/ethereum/execution-apis/issues/295) |


## `ExtraConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `graphql` | [`GqlConfig`](#gqlconfig) |  |  |


## `FlightSqlConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `allowAnonymous` | `boolean` |  | Whether clients can authenticate as 'anonymous' user |
| `anonSessionExpirationTimeout` | [`DurationString`](#durationstring) |  | Time after which `FlightSQL` client session will be forgotten and client<br>will have to re-authroize (for anonymous clients) |
| `anonSessionInactivityTimeout` | [`DurationString`](#durationstring) |  | Time after which `FlightSQL` session context will be released to free<br>the resources (for anonymous clients) |
| `authedSessionExpirationTimeout` | [`DurationString`](#durationstring) |  | Time after which `FlightSQL` client session will be forgotten and client<br>will have to re-authroize (for authenticated clients) |
| `authedSessionInactivityTimeout` | [`DurationString`](#durationstring) |  | Time after which `FlightSQL` session context will be released to free<br>the resources (for authenticated clients) |


## `FlowAgentConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `awaitingStepSecs` | `integer` |  |  |
| `defaultRetryPolicies` | `object` |  |  |
| `mandatoryThrottlingPeriodSecs` | `integer` |  |  |


## `FlowSystemConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `flowAgent` | [`FlowAgentConfig`](#flowagentconfig) |  |  |
| `flowSystemEventAgent` | [`FlowSystemEventAgentConfig`](#flowsystemeventagentconfig) |  |  |
| `taskAgent` | [`TaskAgentConfig`](#taskagentconfig) |  |  |


## `FlowSystemEventAgentConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `batchSize` | `integer` |  |  |
| `maxListeningTimeoutMs` | `integer` |  |  |
| `minDebounceIntervalMs` | `integer` |  |  |


## `FrontendConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `jupyter` | [`JupyterConfig`](#jupyterconfig) |  | Integrated Jupyter notebook configuration |


## `GqlConfig`

| Field | Type | Required | Description |
|---|---|---|---|


## `HttpSourceConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `connectTimeout` | [`DurationString`](#durationstring) |  | Timeout for the connect phase of the HTTP client |
| `maxRedirects` | `integer` |  | Maximum number of redirects to follow |
| `userAgent` | `string` |  | Value to use for User-Agent header |


## `IdentityConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `privateKey` | [`PrivateKey`](#privatekey) |  | Private key used to sign API responses.<br>Currently only `ed25519` keys are supported.<br><br>To generate use:<br><br>    dd if=/dev/urandom bs=1 count=32 status=none \|<br>        base64 -w0 \|<br>        tr '+/' '-_' \|<br>        tr -d '=' \|<br>        (echo -n u && cat)<br><br>The command above:<br>- reads 32 random bytes<br>- base64-encodes them<br>- converts default base64 encoding to base64url and removes padding<br>- prepends a multibase prefix |


## `IpfsConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `httpGateway` | `string` |  | HTTP Gateway URL to use for downloads.<br>For safety, it defaults to `http://localhost:8080` - a local IPFS daemon.<br>If you don't have IPFS installed, you can set this URL to<br>one of the public gateways like `https://ipfs.io`.<br>List of public gateways can be found here: `https://ipfs.github.io/public-gateway-checker/` |
| `preResolveDnslink` | `boolean` |  | Whether kamu should pre-resolve IPNS `DNSLink` names using DNS or leave<br>it to the Gateway. |


## `JupyterConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `image` | `string` |  | Jupyter notebook server image |
| `livyImage` | `string` |  | UNSTABLE: Livy + Spark server image |


## `MqttSourceConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `brokerIdleTimeout` | [`DurationString`](#durationstring) |  | Time in milliseconds to wait for MQTT broker to send us some data after<br>which we will consider that we have "caught up" and end the polling<br>loop. |


## `NetworkNamespaceType`

Corresponds to podman's `containers.conf::netns`
We podman is used inside containers (e.g. podman-in-docker or podman-in-k8s)
it usually runs uses host network namespace.

| Variants |
|---|
| `Private` |
| `Host` |


## `OutboxConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `awaitingStepSecs` | `integer` |  |  |
| `batchSize` | `integer` |  |  |


## `Password`

Base type: `string`


## `PredefinedAccountsConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `predefined` | `array` |  |  |


## `PrivateKey`

Base type: `string`


## `ProtocolConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `flightSql` | [`FlightSqlConfig`](#flightsqlconfig) |  | `FlightSQL` configuration |
| `ipfs` | [`IpfsConfig`](#ipfsconfig) |  | IPFS configuration |


## `RetryPolicyConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `backoffType` | [`RetryPolicyConfigBackoffType`](#retrypolicyconfigbackofftype) |  |  |
| `maxAttempts` | `integer` |  |  |
| `minDelaySecs` | `integer` |  |  |


## `RetryPolicyConfigBackoffType`

| Variants |
|---|
| `Fixed` |
| `Linear` |
| `Exponential` |
| `ExponentialWithJitter` |


## `SearchConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `embeddingsChunker` | [`EmbeddingsChunkerConfig`](#embeddingschunkerconfig) |  | Embeddings chunker configuration |
| `embeddingsEncoder` | [`EmbeddingsEncoderConfig`](#embeddingsencoderconfig) |  | Embeddings encoder configuration |
| `indexer` | [`SearchIndexerConfig`](#searchindexerconfig) |  | Indexer configuration |
| `repo` | [`SearchRepositoryConfig`](#searchrepositoryconfig) |  | Search repository configuration |


## `SearchIndexerConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `clearOnStart` | `boolean` |  | Whether to clear and re-index on start or use existing vectors if any |
| `incrementalIndexing` | `boolean` |  | Whether incremental indexing is enabled |


## `SearchRepositoryConfig`

| Variants |
|---|
| [`Dummy`](#searchrepositoryconfigdummy) |
| [`Elasticsearch`](#searchrepositoryconfigelasticsearch) |
| [`ElasticsearchContainer`](#searchrepositoryconfigelasticsearchcontainer) |


## `SearchRepositoryConfig::Dummy`

| Field | Type | Required | Description |
|---|---|---|---|
| `kind` | `string` | `V` |  |


## `SearchRepositoryConfig::Elasticsearch`

| Field | Type | Required | Description |
|---|---|---|---|
| `caCertPemPath` | `string` |  |  |
| `embeddingDimensions` | `integer` |  |  |
| `enableCompression` | `boolean` |  |  |
| `indexPrefix` | `string` |  |  |
| `kind` | `string` | `V` |  |
| `password` | `string` |  |  |
| `timeoutSecs` | `integer` |  |  |
| `url` | `string` |  |  |


## `SearchRepositoryConfig::ElasticsearchContainer`

| Field | Type | Required | Description |
|---|---|---|---|
| `embeddingDimensions` | `integer` |  |  |
| `image` | `string` |  |  |
| `kind` | `string` | `V` |  |
| `startTimeout` | [`DurationString`](#durationstring) |  |  |


## `SourceConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `ethereum` | [`EthereumSourceConfig`](#ethereumsourceconfig) |  | Ethereum-specific configuration |
| `http` | [`HttpSourceConfig`](#httpsourceconfig) |  | HTTP-specific configuration |
| `mqtt` | [`MqttSourceConfig`](#mqttsourceconfig) |  | MQTT-specific configuration |
| `targetRecordsPerSlice` | `integer` |  | Target number of records after which we will stop consuming from the<br>resumable source and commit data, leaving the rest for the next<br>iteration. This ensures that one data slice doesn't become too big. |


## `TaskAgentConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `checkingIntervalSecs` | `integer` |  |  |


## `UploadsConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `maxFileSizeInMb` | `integer` |  |  |


## `WebhooksConfig`

| Field | Type | Required | Description |
|---|---|---|---|
| `deliveryTimeout` | `integer` |  |  |
| `maxConsecutiveFailures` | `integer` |  |  |
| `secretEncryptionEnabled` | `boolean` |  |  |
| `secretEncryptionKey` | `string` |  | Represents the encryption key for the webhooks secret. This field is<br>required if `secret_encryption_enabled` is `true` or `None`.<br><br>The encryption key must be a 32-character alphanumeric string, which<br>includes both uppercase and lowercase Latin letters (A-Z, a-z) and<br>digits (0-9).<br><br># Example<br><br>let config = WebhooksConfig {<br>    ...<br>    secret_encryption_enabled: Some(true),<br>    encryption_key:<br>Some(String::from("aBcDeFgHiJkLmNoPqRsTuVwXyZ012345")) };<br> |
