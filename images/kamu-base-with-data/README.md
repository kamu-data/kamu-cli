# Local image debugging

## Context

We have two scripts to initialize the workspace to be used inside the image:
- [init-workspace.sh](init-workspace.sh), used on CI:
  - Pulls datasets from a simple repository (AWS S3), resulting in very long execution times
- [init-workspace-rustfs.sh](init-workspace-rustfs.sh), for debugging purposes:
  - Counts on preloaded data from AWS S3 buckets (downloading via AWS SDK is very fast)
  - Populates local RustFS with data from AWS S3
  - Pulls datasets from a simple repository (RustFS), locally, which is dozens of times faster

## Debugging with RustFS

First of all, we need the RustFS itself:

```shell
# Run in the first terminal tab 
podman run --rm \
  -p 9000:9000 \
  -p 9001:9001 \
  -v "./s3-data:/data:Z" \
  -e "RUSTFS_ROOT_USER=kamu" \
  -e "RUSTFS_ROOT_PASSWORD=password" \
  docker.io/rustfs/rustfs server /data --console-address ":9001"
```

Next, we need to upload data from AWS S3 and then use it to initialize RustFS:
```shell
# Run in the second terminal tab 
aws-sso exec
aws s3 sync s3://datasets.kamu.dev/odf/v2/example/ ./s3-aws/example
aws s3 sync s3://datasets.kamu.dev/odf/v2/contrib/ ./s3-aws/contrib
 
./init-workspace-rustfs.sh
```

Done! We can delete a workspace and restart [init-workspace-rustfs.sh](init-workspace-rustfs.sh) to get it again without tedious waiting.
