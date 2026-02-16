## `CLIConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>auth</code></td>
<td><a href="#authconfig"><code>AuthConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;allowAnonymous&quot;: true,
  &quot;users&quot;: {
    &quot;predefined&quot;: []
  }
}</code></pre></td>
<td>Auth configuration</td>
</tr>
<tr>
<td><code>database</code></td>
<td><a href="#databaseconfig"><code>DatabaseConfig</code></a></td>
<td><code class="language-json">null</code></td>
<td>Database connection configuration</td>
</tr>
<tr>
<td><code>datasetEnvVars</code></td>
<td><a href="#datasetenvvarsconfig"><code>DatasetEnvVarsConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;enabled&quot;: false
}</code></pre></td>
<td>Dataset environment variables configuration</td>
</tr>
<tr>
<td><code>didEncryption</code></td>
<td><a href="#didsecretencryptionconfig"><code>DidSecretEncryptionConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;enabled&quot;: false
}</code></pre></td>
<td>Did secret key encryption configuration</td>
</tr>
<tr>
<td><code>engine</code></td>
<td><a href="#engineconfig"><code>EngineConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;datafusionEmbedded&quot;: {
    &quot;base&quot;: {
      &quot;datafusion.catalog.default_catalog&quot;: &quot;kamu&quot;,
      &quot;datafusion.catalog.default_schema&quot;: &quot;kamu&quot;,
      &quot;datafusion.catalog.information_schema&quot;: &quot;true&quot;,
      &quot;datafusion.sql_parser.enable_ident_normalization&quot;: &quot;false&quot;
    },
    &quot;batchQuery&quot;: {},
    &quot;compaction&quot;: {
      &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
    },
    &quot;ingest&quot;: {
      &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
    },
    &quot;useLegacyArrowBufferEncoding&quot;: false
  },
  &quot;images&quot;: {
    &quot;datafusion&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-datafusion:0.9.0&quot;,
    &quot;flink&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-flink:0.18.2-flink_1.16.0-scala_2.12-java8&quot;,
    &quot;risingwave&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-risingwave:0.2.0-risingwave_1.7.0-alpha&quot;,
    &quot;spark&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;
  },
  &quot;networkNs&quot;: &quot;Private&quot;,
  &quot;runtime&quot;: &quot;Docker&quot;,
  &quot;shutdownTimeout&quot;: &quot;5s&quot;,
  &quot;startTimeout&quot;: &quot;30s&quot;
}</code></pre></td>
<td>Engine configuration</td>
</tr>
<tr>
<td><code>extra</code></td>
<td><a href="#extraconfig"><code>ExtraConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;graphql&quot;: {}
}</code></pre></td>
<td>Experimental and temporary configuration options</td>
</tr>
<tr>
<td><code>flowSystem</code></td>
<td><a href="#flowsystemconfig"><code>FlowSystemConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;flowAgent&quot;: {
    &quot;awaitingStepSecs&quot;: 1,
    &quot;defaultRetryPolicies&quot;: {},
    &quot;mandatoryThrottlingPeriodSecs&quot;: 60
  },
  &quot;flowSystemEventAgent&quot;: {
    &quot;batchSize&quot;: 20,
    &quot;maxListeningTimeoutMs&quot;: 2000,
    &quot;minDebounceIntervalMs&quot;: 100
  },
  &quot;taskAgent&quot;: {
    &quot;checkingIntervalSecs&quot;: 1
  }
}</code></pre></td>
<td>Configuration for flow system</td>
</tr>
<tr>
<td><code>frontend</code></td>
<td><a href="#frontendconfig"><code>FrontendConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;jupyter&quot;: {
    &quot;image&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;jupyter:0.7.1&quot;,
    &quot;livyImage&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;
  }
}</code></pre></td>
<td>Data access and visualization configuration</td>
</tr>
<tr>
<td><code>identity</code></td>
<td><a href="#identityconfig"><code>IdentityConfig</code></a></td>
<td><code class="language-json">{}</code></td>
<td>UNSTABLE: Identity configuration</td>
</tr>
<tr>
<td><code>outbox</code></td>
<td><a href="#outboxconfig"><code>OutboxConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;awaitingStepSecs&quot;: 1,
  &quot;batchSize&quot;: 20
}</code></pre></td>
<td>Messaging outbox configuration</td>
</tr>
<tr>
<td><code>protocol</code></td>
<td><a href="#protocolconfig"><code>ProtocolConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;flightSql&quot;: {
    &quot;allowAnonymous&quot;: true,
    &quot;anonSessionExpirationTimeout&quot;: &quot;30m&quot;,
    &quot;anonSessionInactivityTimeout&quot;: &quot;5s&quot;,
    &quot;authedSessionExpirationTimeout&quot;: &quot;30m&quot;,
    &quot;authedSessionInactivityTimeout&quot;: &quot;5s&quot;
  },
  &quot;ipfs&quot;: {
    &quot;httpGateway&quot;: &quot;http:&#x2F;&#x2F;localhost:8080&#x2F;&quot;,
    &quot;preResolveDnslink&quot;: true
  }
}</code></pre></td>
<td>Network protocols configuration</td>
</tr>
<tr>
<td><code>quotaDefaults</code></td>
<td><a href="#quotadefaults"><code>QuotaDefaults</code></a></td>
<td><pre><code class="language-json">{
  &quot;storage&quot;: 1000000000
}</code></pre></td>
<td>Default quotas configured by type</td>
</tr>
<tr>
<td><code>search</code></td>
<td><a href="#searchconfig"><code>SearchConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;embeddingsChunker&quot;: {
    &quot;kind&quot;: &quot;Simple&quot;,
    &quot;splitParagraphs&quot;: false,
    &quot;splitSections&quot;: false
  },
  &quot;embeddingsEncoder&quot;: {
    &quot;dimensions&quot;: 1536,
    &quot;kind&quot;: &quot;OpenAi&quot;,
    &quot;modelName&quot;: &quot;text-embedding-ada-002&quot;
  },
  &quot;indexer&quot;: {
    &quot;clearOnStart&quot;: false,
    &quot;incrementalIndexing&quot;: true
  },
  &quot;repo&quot;: {
    &quot;kind&quot;: &quot;Dummy&quot;
  }
}</code></pre></td>
<td>Search configuration</td>
</tr>
<tr>
<td><code>source</code></td>
<td><a href="#sourceconfig"><code>SourceConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;ethereum&quot;: {
    &quot;commitAfterBlocksScanned&quot;: 1000000,
    &quot;getLogsBlockStride&quot;: 100000,
    &quot;rpcEndpoints&quot;: [],
    &quot;useBlockTimestampFallback&quot;: false
  },
  &quot;http&quot;: {
    &quot;connectTimeout&quot;: &quot;30s&quot;,
    &quot;maxRedirects&quot;: 10,
    &quot;userAgent&quot;: &quot;kamu-cli&#x2F;0.259.1&quot;
  },
  &quot;mqtt&quot;: {
    &quot;brokerIdleTimeout&quot;: &quot;1s&quot;
  },
  &quot;targetRecordsPerSlice&quot;: 10000
}</code></pre></td>
<td>Source configuration</td>
</tr>
<tr>
<td><code>uploads</code></td>
<td><a href="#uploadsconfig"><code>UploadsConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;maxFileSizeInMb&quot;: 50
}</code></pre></td>
<td>Uploads configuration</td>
</tr>
<tr>
<td><code>webhooks</code></td>
<td><a href="#webhooksconfig"><code>WebhooksConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;deliveryTimeout&quot;: 10,
  &quot;maxConsecutiveFailures&quot;: 5,
  &quot;secretEncryptionEnabled&quot;: false
}</code></pre></td>
<td>Configuration for webhooks</td>
</tr>
</tbody>
</table>

## `AccountConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>accountName</code></td>
<td><a href="#accountname"><code>AccountName</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>accountType</code></td>
<td><a href="#accounttype"><code>AccountType</code></a></td>
<td><code class="language-json">&quot;User&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>avatarUrl</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>displayName</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td>

Auto-derived from `account_name` if omitted

</td>
</tr>
<tr>
<td><code>email</code></td>
<td><a href="#email"><code>Email</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>id</code></td>
<td><a href="#accountid"><code>AccountID</code></a></td>
<td><code class="language-json">null</code></td>
<td>

Auto-derived from `account_name` if omitted

</td>
</tr>
<tr>
<td><code>password</code></td>
<td><a href="#password"><code>Password</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>properties</code></td>
<td><code>array</code></td>
<td><code class="language-json">[]</code></td>
<td></td>
</tr>
<tr>
<td><code>provider</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;password&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>registeredAt</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>treatDatasetsAsPublic</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
</tbody>
</table>

## `AccountID`

Base type: `string`

## `AccountName`

Base type: `string`

## `AccountPropertyName`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><code>CanProvisionAccounts</code></td></tr>
<tr><td><code>Admin</code></td></tr>
</tbody>
</table>

## `AccountType`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><code>User</code></td></tr>
<tr><td><code>Organization</code></td></tr>
</tbody>
</table>

## `AuthConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>allowAnonymous</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">true</code></td>
<td></td>
</tr>
<tr>
<td><code>users</code></td>
<td><a href="#predefinedaccountsconfig"><code>PredefinedAccountsConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;predefined&quot;: []
}</code></pre></td>
<td></td>
</tr>
</tbody>
</table>

## `ContainerRuntimeType`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><code>Docker</code></td></tr>
<tr><td><code>Podman</code></td></tr>
</tbody>
</table>

## `DatabaseConfig`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><a href="#databaseconfigsqlite"><code>Sqlite</code></a></td></tr>
<tr><td><a href="#databaseconfigpostgres"><code>Postgres</code></a></td></tr>
<tr><td><a href="#databaseconfigmysql"><code>MySql</code></a></td></tr>
<tr><td><a href="#databaseconfigmariadb"><code>MariaDB</code></a></td></tr>
</tbody>
</table>


## `DatabaseConfig::Sqlite`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>databasePath</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>provider</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `DatabaseConfig::Postgres`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>acquireTimeoutSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>credentialsPolicy</code></td>
<td><a href="#databasecredentialspolicyconfig"><code>DatabaseCredentialsPolicyConfig</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>databaseName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>host</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>maxConnections</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>maxLifetimeSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>port</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>provider</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `DatabaseConfig::MySql`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>acquireTimeoutSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>credentialsPolicy</code></td>
<td><a href="#databasecredentialspolicyconfig"><code>DatabaseCredentialsPolicyConfig</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>databaseName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>host</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>maxConnections</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>maxLifetimeSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>port</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>provider</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `DatabaseConfig::MariaDB`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>acquireTimeoutSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>credentialsPolicy</code></td>
<td><a href="#databasecredentialspolicyconfig"><code>DatabaseCredentialsPolicyConfig</code></a></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>databaseName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>host</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>maxConnections</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>maxLifetimeSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>port</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>provider</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>

## `DatabaseCredentialSourceConfig`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><a href="#databasecredentialsourceconfigrawpassword"><code>RawPassword</code></a></td></tr>
<tr><td><a href="#databasecredentialsourceconfigawssecret"><code>AwsSecret</code></a></td></tr>
<tr><td><a href="#databasecredentialsourceconfigawsiamtoken"><code>AwsIamToken</code></a></td></tr>
</tbody>
</table>


## `DatabaseCredentialSourceConfig::RawPassword`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>rawPassword</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>userName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `DatabaseCredentialSourceConfig::AwsSecret`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>secretName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `DatabaseCredentialSourceConfig::AwsIamToken`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>userName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>

## `DatabaseCredentialsPolicyConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>rotationFrequencyInMinutes</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>source</code></td>
<td><a href="#databasecredentialsourceconfig"><code>DatabaseCredentialSourceConfig</code></a></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>

## `DatasetEnvVarsConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>enabled</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
<tr>
<td><code>encryptionKey</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td>

Represents the encryption key for the dataset env vars. This field is
required if `enabled` is `true` or `None`.

The encryption key must be a 32-character alphanumeric string, which
includes both uppercase and lowercase Latin letters (A-Z, a-z) and
digits (0-9).

To generate use:
```sh
tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32; echo
```

</td>
</tr>
</tbody>
</table>

## `DidSecretEncryptionConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>enabled</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
<tr>
<td><code>encryptionKey</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td>

The encryption key must be a 32-character alphanumeric string, which
includes both uppercase and lowercase Latin letters (A-Z, a-z) and
digits (0-9).

To generate use:
```sh
tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32; echo
```

</td>
</tr>
</tbody>
</table>

## `DurationString`

Base type: `string`

## `Email`

Base type: `string`

## `EmbeddingsChunkerConfig`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><a href="#embeddingschunkerconfigsimple"><code>Simple</code></a></td></tr>
</tbody>
</table>


## `EmbeddingsChunkerConfig::Simple`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>splitParagraphs</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
<tr>
<td><code>splitSections</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
</tbody>
</table>

## `EmbeddingsEncoderConfig`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><a href="#embeddingsencoderconfigdummy"><code>Dummy</code></a></td></tr>
<tr><td><a href="#embeddingsencoderconfigopenai"><code>OpenAi</code></a></td></tr>
</tbody>
</table>


## `EmbeddingsEncoderConfig::Dummy`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `EmbeddingsEncoderConfig::OpenAi`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>apiKey</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>dimensions</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1536</code></td>
<td></td>
</tr>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>modelName</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;text-embedding-ada-002&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>url</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
</tbody>
</table>

## `EngineConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>datafusionEmbedded</code></td>
<td><a href="#engineconfigdatafusion"><code>EngineConfigDatafusion</code></a></td>
<td><pre><code class="language-json">{
  &quot;base&quot;: {
    &quot;datafusion.catalog.default_catalog&quot;: &quot;kamu&quot;,
    &quot;datafusion.catalog.default_schema&quot;: &quot;kamu&quot;,
    &quot;datafusion.catalog.information_schema&quot;: &quot;true&quot;,
    &quot;datafusion.sql_parser.enable_ident_normalization&quot;: &quot;false&quot;
  },
  &quot;batchQuery&quot;: {},
  &quot;compaction&quot;: {
    &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
  },
  &quot;ingest&quot;: {
    &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
  },
  &quot;useLegacyArrowBufferEncoding&quot;: false
}</code></pre></td>
<td>Embedded Datafusion engine configuration</td>
</tr>
<tr>
<td><code>images</code></td>
<td><a href="#engineimagesconfig"><code>EngineImagesConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;datafusion&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-datafusion:0.9.0&quot;,
  &quot;flink&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-flink:0.18.2-flink_1.16.0-scala_2.12-java8&quot;,
  &quot;risingwave&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-risingwave:0.2.0-risingwave_1.7.0-alpha&quot;,
  &quot;spark&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;
}</code></pre></td>
<td>UNSTABLE: Default engine images</td>
</tr>
<tr>
<td><code>maxConcurrency</code></td>
<td><code>integer</code></td>
<td><code class="language-json">null</code></td>
<td>Maximum number of engine operations that can be performed concurrently</td>
</tr>
<tr>
<td><code>networkNs</code></td>
<td><a href="#networknamespacetype"><code>NetworkNamespaceType</code></a></td>
<td><code class="language-json">&quot;Private&quot;</code></td>
<td>

Type of the networking namespace (relevant when running in container
environments)

</td>
</tr>
<tr>
<td><code>runtime</code></td>
<td><a href="#containerruntimetype"><code>ContainerRuntimeType</code></a></td>
<td><code class="language-json">&quot;Docker&quot;</code></td>
<td>Type of the runtime to use when running the data processing engines</td>
</tr>
<tr>
<td><code>shutdownTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;5s&quot;</code></td>
<td>Timeout for waiting the engine container to stop gracefully</td>
</tr>
<tr>
<td><code>startTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;30s&quot;</code></td>
<td>Timeout for starting an engine container</td>
</tr>
</tbody>
</table>

## `EngineConfigDatafusion`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>base</code></td>
<td><code>object</code></td>
<td><pre><code class="language-json">{
  &quot;datafusion.catalog.default_catalog&quot;: &quot;kamu&quot;,
  &quot;datafusion.catalog.default_schema&quot;: &quot;kamu&quot;,
  &quot;datafusion.catalog.information_schema&quot;: &quot;true&quot;,
  &quot;datafusion.sql_parser.enable_ident_normalization&quot;: &quot;false&quot;
}</code></pre></td>
<td>

Base configuration options
See: `<https://datafusion.apache.org/user-guide/configs.html>`

</td>
</tr>
<tr>
<td><code>batchQuery</code></td>
<td><code>object</code></td>
<td><code class="language-json">{}</code></td>
<td>Batch query-specific overrides to the base config</td>
</tr>
<tr>
<td><code>compaction</code></td>
<td><code>object</code></td>
<td><pre><code class="language-json">{
  &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
}</code></pre></td>
<td>Compaction-specific overrides to the base config</td>
</tr>
<tr>
<td><code>ingest</code></td>
<td><code>object</code></td>
<td><pre><code class="language-json">{
  &quot;datafusion.execution.target_partitions&quot;: &quot;1&quot;
}</code></pre></td>
<td>Ingest-specific overrides to the base config</td>
</tr>
<tr>
<td><code>useLegacyArrowBufferEncoding</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td>

Makes arrow batches use contiguous `Binary` and `Utf8` encodings instead
of more modern `BinaryView` and `Utf8View`. This is only needed for
compatibility with some older libraries that don't yet support them.

See: [kamu-node#277](https://github.com/kamu-data/kamu-node/issues/277)

</td>
</tr>
</tbody>
</table>

## `EngineImagesConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>datafusion</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;engine-datafusion:0.9.0&quot;</code></td>
<td>

UNSTABLE: `Datafusion` engine image

</td>
</tr>
<tr>
<td><code>flink</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;engine-flink:0.18.2-flink_1.16.0-scala_2.12-java8&quot;</code></td>
<td>

UNSTABLE: `Flink` engine image

</td>
</tr>
<tr>
<td><code>risingwave</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;engine-risingwave:0.2.0-risingwave_1.7.0-alpha&quot;</code></td>
<td>

UNSTABLE: `RisingWave` engine image

</td>
</tr>
<tr>
<td><code>spark</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;</code></td>
<td>

UNSTABLE: `Spark` engine image

</td>
</tr>
</tbody>
</table>

## `EthRpcEndpoint`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>chainId</code></td>
<td><code>integer</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>chainName</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>nodeUrl</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>

## `EthereumSourceConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>commitAfterBlocksScanned</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1000000</code></td>
<td>

Forces iteration to stop after the specified number of blocks were
scanned even if we didn't reach the target record number. This is useful
to not lose a lot of scanning progress in case of an RPC error.

</td>
</tr>
<tr>
<td><code>getLogsBlockStride</code></td>
<td><code>integer</code></td>
<td><code class="language-json">100000</code></td>
<td>

Default number of blocks to scan within one query to `eth_getLogs` RPC
endpoint.

</td>
</tr>
<tr>
<td><code>rpcEndpoints</code></td>
<td><code>array</code></td>
<td><code class="language-json">[]</code></td>
<td>Default RPC endpoints to use if source does not specify one explicitly.</td>
</tr>
<tr>
<td><code>useBlockTimestampFallback</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td>

Many providers don't yet return `blockTimestamp` from `eth_getLogs` RPC
endpoint and in such cases `block_timestamp` column will be `null`.
If you enable this fallback the library will perform additional call to
`eth_getBlock` to populate the timestam, but this may result in
significant performance penalty when fetching many log records.

See: [ethereum/execution-apis#295](https://github.com/ethereum/execution-apis/issues/295)

</td>
</tr>
</tbody>
</table>

## `ExtraConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>graphql</code></td>
<td><a href="#gqlconfig"><code>GqlConfig</code></a></td>
<td><code class="language-json">{}</code></td>
<td></td>
</tr>
</tbody>
</table>

## `FlightSqlConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>allowAnonymous</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">true</code></td>
<td>Whether clients can authenticate as 'anonymous' user</td>
</tr>
<tr>
<td><code>anonSessionExpirationTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;30m&quot;</code></td>
<td>

Time after which `FlightSQL` client session will be forgotten and client
will have to re-authroize (for anonymous clients)

</td>
</tr>
<tr>
<td><code>anonSessionInactivityTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;5s&quot;</code></td>
<td>

Time after which `FlightSQL` session context will be released to free
the resources (for anonymous clients)

</td>
</tr>
<tr>
<td><code>authedSessionExpirationTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;30m&quot;</code></td>
<td>

Time after which `FlightSQL` client session will be forgotten and client
will have to re-authroize (for authenticated clients)

</td>
</tr>
<tr>
<td><code>authedSessionInactivityTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;5s&quot;</code></td>
<td>

Time after which `FlightSQL` session context will be released to free
the resources (for authenticated clients)

</td>
</tr>
</tbody>
</table>

## `FlowAgentConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>awaitingStepSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1</code></td>
<td></td>
</tr>
<tr>
<td><code>defaultRetryPolicies</code></td>
<td><code>object</code></td>
<td><code class="language-json">{}</code></td>
<td></td>
</tr>
<tr>
<td><code>mandatoryThrottlingPeriodSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">60</code></td>
<td></td>
</tr>
</tbody>
</table>

## `FlowSystemConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>flowAgent</code></td>
<td><a href="#flowagentconfig"><code>FlowAgentConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;awaitingStepSecs&quot;: 1,
  &quot;defaultRetryPolicies&quot;: {},
  &quot;mandatoryThrottlingPeriodSecs&quot;: 60
}</code></pre></td>
<td></td>
</tr>
<tr>
<td><code>flowSystemEventAgent</code></td>
<td><a href="#flowsystemeventagentconfig"><code>FlowSystemEventAgentConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;batchSize&quot;: 20,
  &quot;maxListeningTimeoutMs&quot;: 2000,
  &quot;minDebounceIntervalMs&quot;: 100
}</code></pre></td>
<td></td>
</tr>
<tr>
<td><code>taskAgent</code></td>
<td><a href="#taskagentconfig"><code>TaskAgentConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;checkingIntervalSecs&quot;: 1
}</code></pre></td>
<td></td>
</tr>
</tbody>
</table>

## `FlowSystemEventAgentConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>batchSize</code></td>
<td><code>integer</code></td>
<td><code class="language-json">20</code></td>
<td></td>
</tr>
<tr>
<td><code>maxListeningTimeoutMs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">2000</code></td>
<td></td>
</tr>
<tr>
<td><code>minDebounceIntervalMs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">100</code></td>
<td></td>
</tr>
</tbody>
</table>

## `FrontendConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>jupyter</code></td>
<td><a href="#jupyterconfig"><code>JupyterConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;image&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;jupyter:0.7.1&quot;,
  &quot;livyImage&quot;: &quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;
}</code></pre></td>
<td>Integrated Jupyter notebook configuration</td>
</tr>
</tbody>
</table>

## `GqlConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
</tbody>
</table>

## `HttpSourceConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>connectTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;30s&quot;</code></td>
<td>Timeout for the connect phase of the HTTP client</td>
</tr>
<tr>
<td><code>maxRedirects</code></td>
<td><code>integer</code></td>
<td><code class="language-json">10</code></td>
<td>Maximum number of redirects to follow</td>
</tr>
<tr>
<td><code>userAgent</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;kamu-cli&#x2F;0.259.1&quot;</code></td>
<td>Value to use for User-Agent header</td>
</tr>
</tbody>
</table>

## `IdentityConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>privateKey</code></td>
<td><a href="#privatekey"><code>PrivateKey</code></a></td>
<td><code class="language-json">null</code></td>
<td>

Private key used to sign API responses.
Currently only `ed25519` keys are supported.

To generate use:

    dd if=/dev/urandom bs=1 count=32 status=none |
        base64 -w0 |
        tr '+/' '-_' |
        tr -d '=' |
        (echo -n u && cat)

The command above:
- reads 32 random bytes
- base64-encodes them
- converts default base64 encoding to base64url and removes padding
- prepends a multibase prefix

</td>
</tr>
</tbody>
</table>

## `IpfsConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>httpGateway</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;http:&#x2F;&#x2F;localhost:8080&#x2F;&quot;</code></td>
<td>

HTTP Gateway URL to use for downloads.
For safety, it defaults to `http://localhost:8080` - a local IPFS daemon.
If you don't have IPFS installed, you can set this URL to
one of the public gateways like `https://ipfs.io`.
List of public gateways can be found here: `https://ipfs.github.io/public-gateway-checker/`

</td>
</tr>
<tr>
<td><code>preResolveDnslink</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">true</code></td>
<td>

Whether kamu should pre-resolve IPNS `DNSLink` names using DNS or leave
it to the Gateway.

</td>
</tr>
</tbody>
</table>

## `JupyterConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>image</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;jupyter:0.7.1&quot;</code></td>
<td>Jupyter notebook server image</td>
</tr>
<tr>
<td><code>livyImage</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;ghcr.io&#x2F;kamu-data&#x2F;engine-spark:0.23.1-spark_3.5.0&quot;</code></td>
<td>UNSTABLE: Livy + Spark server image</td>
</tr>
</tbody>
</table>

## `MqttSourceConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>brokerIdleTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;1s&quot;</code></td>
<td>

Time in milliseconds to wait for MQTT broker to send us some data after
which we will consider that we have "caught up" and end the polling
loop.

</td>
</tr>
</tbody>
</table>

## `NetworkNamespaceType`

Corresponds to podman's `containers.conf::netns`
We podman is used inside containers (e.g. podman-in-docker or podman-in-k8s)
it usually runs uses host network namespace.

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><code>Private</code></td></tr>
<tr><td><code>Host</code></td></tr>
</tbody>
</table>

## `OutboxConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>awaitingStepSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1</code></td>
<td></td>
</tr>
<tr>
<td><code>batchSize</code></td>
<td><code>integer</code></td>
<td><code class="language-json">20</code></td>
<td></td>
</tr>
</tbody>
</table>

## `Password`

Base type: `string`

## `PredefinedAccountsConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>predefined</code></td>
<td><code>array</code></td>
<td><code class="language-json">[]</code></td>
<td></td>
</tr>
</tbody>
</table>

## `PrivateKey`

Base type: `string`

## `ProtocolConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>flightSql</code></td>
<td><a href="#flightsqlconfig"><code>FlightSqlConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;allowAnonymous&quot;: true,
  &quot;anonSessionExpirationTimeout&quot;: &quot;30m&quot;,
  &quot;anonSessionInactivityTimeout&quot;: &quot;5s&quot;,
  &quot;authedSessionExpirationTimeout&quot;: &quot;30m&quot;,
  &quot;authedSessionInactivityTimeout&quot;: &quot;5s&quot;
}</code></pre></td>
<td>

`FlightSQL` configuration

</td>
</tr>
<tr>
<td><code>ipfs</code></td>
<td><a href="#ipfsconfig"><code>IpfsConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;httpGateway&quot;: &quot;http:&#x2F;&#x2F;localhost:8080&#x2F;&quot;,
  &quot;preResolveDnslink&quot;: true
}</code></pre></td>
<td>IPFS configuration</td>
</tr>
</tbody>
</table>

## `QuotaDefaults`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>storage</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1000000000</code></td>
<td></td>
</tr>
</tbody>
</table>

## `RetryPolicyConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>backoffType</code></td>
<td><a href="#retrypolicyconfigbackofftype"><code>RetryPolicyConfigBackoffType</code></a></td>
<td><code class="language-json">&quot;Fixed&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>maxAttempts</code></td>
<td><code>integer</code></td>
<td><code class="language-json">0</code></td>
<td></td>
</tr>
<tr>
<td><code>minDelaySecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">0</code></td>
<td></td>
</tr>
</tbody>
</table>

## `RetryPolicyConfigBackoffType`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><code>Fixed</code></td></tr>
<tr><td><code>Linear</code></td></tr>
<tr><td><code>Exponential</code></td></tr>
<tr><td><code>ExponentialWithJitter</code></td></tr>
</tbody>
</table>

## `SearchConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>embeddingsChunker</code></td>
<td><a href="#embeddingschunkerconfig"><code>EmbeddingsChunkerConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;kind&quot;: &quot;Simple&quot;,
  &quot;splitParagraphs&quot;: false,
  &quot;splitSections&quot;: false
}</code></pre></td>
<td>Embeddings chunker configuration</td>
</tr>
<tr>
<td><code>embeddingsEncoder</code></td>
<td><a href="#embeddingsencoderconfig"><code>EmbeddingsEncoderConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;dimensions&quot;: 1536,
  &quot;kind&quot;: &quot;OpenAi&quot;,
  &quot;modelName&quot;: &quot;text-embedding-ada-002&quot;
}</code></pre></td>
<td>Embeddings encoder configuration</td>
</tr>
<tr>
<td><code>indexer</code></td>
<td><a href="#searchindexerconfig"><code>SearchIndexerConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;clearOnStart&quot;: false,
  &quot;incrementalIndexing&quot;: true
}</code></pre></td>
<td>Indexer configuration</td>
</tr>
<tr>
<td><code>repo</code></td>
<td><a href="#searchrepositoryconfig"><code>SearchRepositoryConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;kind&quot;: &quot;Dummy&quot;
}</code></pre></td>
<td>Search repository configuration</td>
</tr>
</tbody>
</table>

## `SearchIndexerConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>clearOnStart</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td>Whether to clear and re-index on start or use existing vectors if any</td>
</tr>
<tr>
<td><code>incrementalIndexing</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">true</code></td>
<td>Whether incremental indexing is enabled</td>
</tr>
</tbody>
</table>

## `SearchRepositoryConfig`

<table>
<thead><tr><th>Variants</th></tr></thead>
<tbody>
<tr><td><a href="#searchrepositoryconfigdummy"><code>Dummy</code></a></td></tr>
<tr><td><a href="#searchrepositoryconfigelasticsearch"><code>Elasticsearch</code></a></td></tr>
<tr><td><a href="#searchrepositoryconfigelasticsearchcontainer"><code>ElasticsearchContainer</code></a></td></tr>
</tbody>
</table>


## `SearchRepositoryConfig::Dummy`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
</tbody>
</table>


## `SearchRepositoryConfig::Elasticsearch`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>caCertPemPath</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>embeddingDimensions</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1536</code></td>
<td></td>
</tr>
<tr>
<td><code>enableCompression</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
<tr>
<td><code>indexPrefix</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>password</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td></td>
</tr>
<tr>
<td><code>timeoutSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">30</code></td>
<td></td>
</tr>
<tr>
<td><code>url</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;http:&#x2F;&#x2F;localhost:9200&#x2F;&quot;</code></td>
<td></td>
</tr>
</tbody>
</table>


## `SearchRepositoryConfig::ElasticsearchContainer`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>embeddingDimensions</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1536</code></td>
<td></td>
</tr>
<tr>
<td><code>image</code></td>
<td><code>string</code></td>
<td><code class="language-json">&quot;docker.io&#x2F;elasticsearch:9.2.1&quot;</code></td>
<td></td>
</tr>
<tr>
<td><code>kind</code></td>
<td><code>string</code></td>
<td></td>
<td></td>
</tr>
<tr>
<td><code>startTimeout</code></td>
<td><a href="#durationstring"><code>DurationString</code></a></td>
<td><code class="language-json">&quot;30s&quot;</code></td>
<td></td>
</tr>
</tbody>
</table>

## `SourceConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>ethereum</code></td>
<td><a href="#ethereumsourceconfig"><code>EthereumSourceConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;commitAfterBlocksScanned&quot;: 1000000,
  &quot;getLogsBlockStride&quot;: 100000,
  &quot;rpcEndpoints&quot;: [],
  &quot;useBlockTimestampFallback&quot;: false
}</code></pre></td>
<td>Ethereum-specific configuration</td>
</tr>
<tr>
<td><code>http</code></td>
<td><a href="#httpsourceconfig"><code>HttpSourceConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;connectTimeout&quot;: &quot;30s&quot;,
  &quot;maxRedirects&quot;: 10,
  &quot;userAgent&quot;: &quot;kamu-cli&#x2F;0.259.1&quot;
}</code></pre></td>
<td>HTTP-specific configuration</td>
</tr>
<tr>
<td><code>mqtt</code></td>
<td><a href="#mqttsourceconfig"><code>MqttSourceConfig</code></a></td>
<td><pre><code class="language-json">{
  &quot;brokerIdleTimeout&quot;: &quot;1s&quot;
}</code></pre></td>
<td>MQTT-specific configuration</td>
</tr>
<tr>
<td><code>targetRecordsPerSlice</code></td>
<td><code>integer</code></td>
<td><code class="language-json">10000</code></td>
<td>

Target number of records after which we will stop consuming from the
resumable source and commit data, leaving the rest for the next
iteration. This ensures that one data slice doesn't become too big.

</td>
</tr>
</tbody>
</table>

## `TaskAgentConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>checkingIntervalSecs</code></td>
<td><code>integer</code></td>
<td><code class="language-json">1</code></td>
<td></td>
</tr>
</tbody>
</table>

## `UploadsConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>maxFileSizeInMb</code></td>
<td><code>integer</code></td>
<td><code class="language-json">50</code></td>
<td></td>
</tr>
</tbody>
</table>

## `WebhooksConfig`

<table>
<thead><tr><th>Field</th><th>Type</th><th>Default</th><th>Description</th></tr></thead>
<tbody>
<tr>
<td><code>deliveryTimeout</code></td>
<td><code>integer</code></td>
<td><code class="language-json">10</code></td>
<td></td>
</tr>
<tr>
<td><code>maxConsecutiveFailures</code></td>
<td><code>integer</code></td>
<td><code class="language-json">5</code></td>
<td></td>
</tr>
<tr>
<td><code>secretEncryptionEnabled</code></td>
<td><code>boolean</code></td>
<td><code class="language-json">false</code></td>
<td></td>
</tr>
<tr>
<td><code>secretEncryptionKey</code></td>
<td><code>string</code></td>
<td><code class="language-json">null</code></td>
<td>

Represents the encryption key for the webhooks secret. This field is
required if `secret_encryption_enabled` is `true` or `None`.

The encryption key must be a 32-character alphanumeric string, which
includes both uppercase and lowercase Latin letters (A-Z, a-z) and
digits (0-9).

# Example
```
let config = WebhooksConfig {
    ...
    secret_encryption_enabled: Some(true),
    encryption_key:
Some(String::from("aBcDeFgHiJkLmNoPqRsTuVwXyZ012345")) };
```

</td>
</tr>
</tbody>
</table>
