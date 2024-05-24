# Self-serve guided demo

This demo guides you through the basics of using `kamu` and its key concepts. It lets you try out most features of the tool without having to install it.

## Requirements
To run this demo you'll only need:
* `docker`

## Running
First you will need to download the `docker-compose.yml` file:

```shell
wget https://raw.githubusercontent.com/kamu-data/kamu-cli/master/images/demo/docker-compose.yml
```

To run all demo components on your computer simply do:

```shell
docker compose up
```

> [!TIP]
> If you've run the environment before you might want to get latest versions of the images using `docker compose pull` command.

This will run:
* JupyterHub web notebooks with Kamu integration
* Minio - S3-like storage server used as a shared dataset repository

Once you start the environment you should see a log line like `jupyter_1  |   http://127.0.0.1:8765/?token=...`  - use this URL in your browser to open Jupyter. Once in Jupyter, navigate to the chapter of interest, open the first notebook, and follow instructions.

Enjoy, and please send us your feedback!

## Shutting down
To shutdown the environment do:

```shell
docker compose down
```
