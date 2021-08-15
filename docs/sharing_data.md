# Sharing Data <!-- omit in toc -->

- [Repository Types](#repository-types)
- [Configuring Repositories](#configuring-repositories)
- [Pulling Data](#pulling-data)
- [Pushing Data](#pushing-data)
- [Validity of Data](#validity-of-data)
  - [Validity of Root Data](#validity-of-root-data)
  - [Validity of Derivative Data](#validity-of-derivative-data)
  - [Verifying Validity](#verifying-validity)

While `kamu` is a very powerful tool for managing and processing data on your own computer, the real power of it becomes apparent only when you start exchanging data with other people. Thanks to its core properties it makes sharing data reliable and safe both within your organization and between multiple completely independent parties.


## Repository Types
Data in `kamu` is shared via repositories. There are multiple types of repositories that differ by the kinds of services they provide. The most basic repository allows you to simply upload ("push") and download ("pull") data.

|   Type   | Description                                                                                             | Capabilities  | URL Examples                                                                                                      |
| :------: | ------------------------------------------------------------------------------------------------------- | :-----------: | ----------------------------------------------------------------------------------------------------------------- |
| Local FS | A basic repository that uses the local file system folder. Mainly used for examples and testing.        | `pull` `push` | `file:///home/me/example/repository` `file:///c:/Users/me/example/repository`                                     |
|    S3    | A basic repository that stores data in Amazon S3 bucket. Can be used with any S3-compatible storage API | `pull` `push` | `s3://bucket.my-company.example` `s3+http://my-minio-server:9000/bucket` `s3+https://my-minio-server:9000/bucket` |


## Configuring Repositories
Repositories are configured per workspace using the `kamu repo` command group.

To add new repo use:
```bash
kamu repo add acme s3://kamu.acme.com
```

This will create a repository with an alias `acme` pointing to the `kamu.acme.com` S3 bucket.

This repo will now be visible in `kamu repo list`.


## Pulling Data
If the repository you added already contains a dataset you're interested in you can download it using the `pull` command:

```bash
# Pulls `acme/com.acme.shipments` into local dataset `com.acme.shipments`
kamu pull acme/com.acme.shipments

# Or pull `acme/com.acme.shipments` into local dataset named `shipments`
kamu pull acme/com.acme.shipments --as shipments
```

These commands will associate the local dataset with the repository it came from, so next time you pull you can simply do:

```bash
# Will pull from associated `acme/com.acme.shipments`
kamu pull shipments
```

These associations are called "remote aliases" and can be viewed using:

```bash
kamu repo alias list
```

## Pushing Data
If you have created a brand new dataset you would like to share, or made some changes to a dataset you are sharing with your friends - you can upload the new data using the `push` command:

```bash
# Push local dataset `orders` to repository `acme/com.acme.orders`
kamu push orders --as acme/com.acme.orders

# This creates push alias, so next time you can push as simply as
kamu push orders
```

This command will analyze the state of the dataset at the repository and will only upload data and metadata that wasn't previously seen. It also detects any type of history collisions, so you will never overwrite someone else's changes.

## Validity of Data
With `kamu` sharing data becomes very easy, but with that problem out of the way you will soon start wondering "How can I trust the data I downloaded from someon else?". Let's first define what validity or trustworthiness of data means.

### Validity of Root Data
Let's say you're about to use a root dataset containing historical weather information in your city. How can you be sure it's trustworthy?

Because source data is non-reproducible its validity depends entirely on its publisher. Publisher is in full control of data they present, which also means that measuring and processing errors and even malicious data can easily make its way into the root dataset. Make your puplishers have good reputation and prefer data that comes from well-established organizations (government or NGOs) that directly collet or opertate the systems from which the data is gathered.

Aside from external audits, another way to improve confidence in data is to correlate it with data from other similar sources. In our example we could compare it with the data from a weather station in the neighbouring city and look for anomalies.

### Validity of Derivative Data
Derivative data in `kamu` is created purely through transformation that are recorded in metadata. These transformations can still be malicious, but since they are usually small (e.g. a few SQL queries) we can easily audit them to ensure they are sound and done in good faith. Repeating this process for the entire transformation chain, starting with root datasets, will give you confidence in trustworthiness of derivative data.

### Verifying Validity
Based on the above, here are the steps needed to ensure a dataset is trustworthy:
- Inspect lineage and identify all root datasets it's composed of
- Ensure publishers of root datasets are reputable and credible
- Use lineage to audit all derivative transformations to see if they are sound and non-malicious
- Use `kamu verify` command to ensure that data you downloaded actually matches the declared transformations

Example:

```bash
# Inspect the dependency graph of a dataset to find all root sources
kamu inspect lineage ca.vancouver.opendata.weather.aggregated-daily

# Inspect the transformations applied with every derivative dataset
kamu log ca.vancouver.opendata.weather.aggregated-daily

# Recursively verify the entire transformation chain starting from root datasets
kamu verify --recursive ca.vancouver.opendata.weather.aggregated-daily
```

For every derivative dataset the `kamu verify` command does two things:
- Compares hashes of data you downloaded to the ones stored in metadata (ensures data is not tampered or corrupted)
- Executes the declared derivative transformations locally to compares the hash of the result to one stored in metadata (ensures that metadata was not spoofed to match the fake result)

> Note: Remember that you are not alone in the fight for data validity. Other people will also be verifying these datasets, so the Open Data Fabric network can quickly detect and exclude the participants who distribute malicious data.
