# Sharing Data <!-- omit in toc -->

- [Remote Types](#remote-types)
- [Configuring Remotes](#configuring-remotes)
- [Pulling Data](#pulling-data)
- [Pushing Data](#pushing-data)

While `kamu` is a very powerful tool for managing and processing data on your own computer the real power of it becomes apparent when you start exchanging data with other people. Thanks to its core properties it makes sharing data reliable and safe both within your organization and between multiple completely independent parties.


## Remote Types
Data in `kamu` is shared via remote repositories ("remotes" for short). There are multiple types of remotes in `kamu` that differ by the types of services they provide. The most basic remote allows you to simply upload ("push") and download ("pull") data.

|   Type   | Description                                                                                         | Capabilities  | URL Examples                                                                                                      |
| :------: | --------------------------------------------------------------------------------------------------- | :-----------: | ----------------------------------------------------------------------------------------------------------------- |
| Local FS | A basic remote that uses the local file system folder. Mainly used for examples and testing.        | `pull` `push` | `file:///home/me/example/remote` `file:///c:/Users/me/example/remote`                                             |
|    S3    | A basic remote that stores data in Amazon S3 bucket. Can be used with any S3-compatible storage API | `pull` `push` | `s3://bucket.my-company.example` `s3+http://my-minio-server:9000/bucket` `s3+https://my-minio-server:9000/bucket` |


## Configuring Remotes
Remotes are configured per workspace using the `kamu remote` command group.

To add new remote use:
```bash
kamu remote add acme s3://kamu.acme.com
```

This will create a remote with an alias `acme` pointing to the `kamu.acme.com` S3 bucket.

This remote will now be visible in `kamu remote list`.


## Pulling Data
If the remote you added already contains a dataset you're interested in you can download it using the `pull` command:

```bash
kamu pull com.acme.shipments --remote acme
```

This command will download all contents of the dataset to your computer and validate the integrity of metadata.

> Note: Currently the `pull` command with `--remote` flag does not create association between the downloaded dataset and the remote it came from, so executing `kamu pull com.acme.shipments` will make `kamu` attempt to perform ingest or derivative transformation (depending on the type of the dataset) instead of refreshing data from the remote. Bear with us while we improve the remotes API and continue to specify the `--remote` option for now.


## Pushing Data
If you have created a brand new dataset you would like to share or made some changes to a dataset you are sharing with your friends - you can upload the new data using the `push` command:

```bash
kamu push com.acme.orders --remote acme
```

This command will analyze the state of the dataset at the remote and will only upload data and metadata that wasn't previously seen.