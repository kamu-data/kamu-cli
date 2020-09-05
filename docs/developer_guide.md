# Developer Guide

- [Developer Guide](#developer-guide)
  - [Building Locally](#building-locally)
  - [Release Procedure](#release-procedure)

## Building Locally
Prerequisites:
* Docker
  * Make sure the docker is usable without `sudo`
* Rust toolset
  * Use `rustup` to install the compiler and `cargo`
* AWS account and configured AWS CLI (optional, needed for S3 volumes)

Clone the repository:
```shell
git clone git@github.com:kamu-data/kamu-cli.git
```

Then do:
```shell
cd kamu-cli
cargo build
cargo test
```

To use your locally-built `kamu` executable link it as so:
```shell
sudo ln -s $PWD/target/debug/kamu /usr/bin/kamu
```

## Release Procedure
1. Create a CHANGELOG entry for the version you are releasing
2. Tag the latest commit with a version: `git tag vX.Y.Z`
3. Push the tag and the commit to the origin: `git push origin tag vX.Y.Z`
4. Github Actions will pick up the new tag and create a new GitHub release from it
