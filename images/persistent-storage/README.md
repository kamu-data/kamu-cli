# Example of using `kamu-cli` with persistent storage

This guide demonstrates an example of running the `kamu-cli` HTTP API that uses PostgreSQL as a data store.

## Requirements
To run this example you'll only need:
* `docker`

## Running
First you will need to download the `compose.yml` file:

```shell
wget https://raw.githubusercontent.com/kamu-data/kamu-cli/master/images/persistent-storage/compose.yml
```

To run all components on your computer simply do:

```shell
docker compose up
```
> [!TIP]
> If you've run the environment before you might want to get latest versions of the images using `docker compose pull` command.

This will run:
* PostgreSQL database, for data storage
* Applying migrations to the database
* `kamu-cli` HTTP API

> [!Note]
> To save the database data between runs, `postgres-data/` directory will be created locally.

Enjoy, and please send us your feedback!

## Shutting down
To shutdown the environment do:

```shell
docker compose down
```
