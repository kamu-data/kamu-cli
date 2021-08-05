# Sharing Data <!-- omit in toc -->

- [Repository Types](#repository-types)
- [Configuring Repositories](#configuring-repositories)
- [Pulling Data](#pulling-data)
- [Pushing Data](#pushing-data)

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
