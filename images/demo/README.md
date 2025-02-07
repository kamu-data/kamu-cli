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

## Updating images in the registry

In this section, we will look at how to build images locally and send them to the registry.

1. Update `DEMO_VERSION` in the [Makefile](./Makefile)
2. Prepare the build environment:
   ```shell
   # Optional: if image building was previously performed, skip this step
   make prepare-multi-arch-build

   make clean
   ```
3. `kamu-cli-demo-minio`  image:
    - Building & pushing:
      ```shell
      make minio-data
      make minio-multi-arch
      ```
    - Check in the registry ([kamu-cli-demo-minio](https://github.com/kamu-data/kamu-cli/pkgs/container/kamu-cli-demo-minio)) that the new version of the image is uploaded.
4. `kamu-cli-demo-jupyter` image:
    - Building & pushing:
      ```shell
      make jupyter-multi-arch
      ```
    - Check in the registry ([kamu-cli-demo-jupyter](https://github.com/kamu-data/kamu-cli/pkgs/container/kamu-cli-demo-jupyter)) that the new version of the image is uploaded.
    - ⚠️ Begin the procedure of deploying the new image
