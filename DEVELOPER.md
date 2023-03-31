# Developer Guide <!-- omit in toc -->
- [Building Locally](#building-locally)
  - [Run Tests with Podman (Recommended)](#run-tests-with-podman-recommended)
  - [Run Tests with Docker (Alternative)](#run-tests-with-docker-alternative)
  - [Using Nextest test runner (Optional)](#using-nextest-test-runner-optional)
  - [Build Speed Tweaks (Optional)](#build-speed-tweaks-optional)
  - [Building with Web UI (Optional)](#building-with-web-ui-optional)
  - [Code Generation](#code-generation)
- [Release Procedure](#release-procedure)


## Building Locally
Prerequisites:
* Docker or Podman (note: unit tests run with Podman by default)
  * If using `docker` - make sure it's usable without `sudo` ([guidelines](https://docs.docker.com/engine/install/linux-postinstall))
  * If using `podman` - make sure it's setup to run root-less containers ([guidelines](https://github.com/containers/podman/blob/main/docs/tutorials/rootless_tutorial.md))
* Rust toolset
  * Install `rustup`
  * The correct toolchain version will be automatically installed based on the `rust-toolchain` file in the repository
* Tools used by tests
  * Install [`jq`](https://stedolan.github.io/jq) - used to query and format JSON files
  * Install [`kubo`](https://docs.ipfs.io/install/command-line/#official-distributions) (formerly known as `go-ipfs`) - for IPFS-related tests
* Code generation tools (optional - needed if you will be updating schemas)
  * Install [`flatc`](https://github.com/google/flatbuffers)
  * Install [`protoc`](https://github.com/protocolbuffers/protobuf) followed by:
    * `cargo install protoc-gen-prost` - to install [prost protobuf plugin](https://crates.io/crates/protoc-gen-prost)
    * `cargo install protoc-gen-tonic` - to install [tonic protobuf plugin](https://crates.io/crates/protoc-gen-tonic)
* Cargo toolbelt (optional - if you will be doing releases)
  * `cargo install cargo-update` - to easily keep your tools up-to-date
  * `cargo install cargo-binstall` - to install binaries without compiling
  * `cargo binstall cargo-binstall --force` - make future updates to use precompiled version
  * `cargo binstall cargo-edit` - for setting crate versions during release
  * `cargo binstall cargo-update` - for keeping up with major dependency updates
  * `cargo binstall cargo-deny` - for linting dependencies
  * `cargo binstall cargo-llvm-cov` - for coverage

Clone the repository:
```shell
git clone git@github.com:kamu-data/kamu-cli.git
```

Build it:
```shell
cd kamu-cli
cargo build
```

To use your locally-built `kamu` executable link it as so:
```shell
ln -s $PWD/target/debug/kamu-cli ~/.local/bin/kamu
```

When needing to test against a specific official release you can install it under a different alias:

```shell
curl -s "https://get.kamu.dev" | KAMU_ALIAS=kamu-release sh
```


### Run Tests with Podman (Recommended)

Prepare test containers once before running tests:
```shell
cargo run --bin kamu-cli -- config set --user engine.runtime podman
kamu init --pull-test-images
```

Then run tests:
```shell
cargo test
```


### Run Tests with Docker (Alternative)

Prepare test containers once before running tests:
```shell
kamu init --pull-test-images
```

Then run tests:
```shell
KAMU_CONTAINER_RUNTIME_TYPE=docker cargo test

```

### Using Nextest test runner (Optional)
[Nextest](https://nexte.st/) is a better test runner.

To use it run:

```sh
cargo binstall cargo-nextest
cargo nextest run
```


### Build Speed Tweaks (Optional)
Consider configuring Rust to use `lld` linker, which is much faster than the default `ld` (may improve link times by ~10-20x).

To do so install `lld`, then create `~/.cargo/config.toml` file with the following contents:

```toml
[build]
rustflags = ["-C", "link-arg=-fuse-ld=lld"]
```

One more alternative is to use `mold` linker, which is also much faster than the default `ld`.

To do so install `mold` or build it with `clang++` compiler from [mold sources](https://github.com/rui314/mold#how-to-build) then create `~/.cargo/config.toml` file with the following contents:

```toml
[build]
linker = "clang"
rustflags = ["-C", "link-arg=-fuse-ld=mold"]
```


### Building with Web UI (Optional)
To build the tool with embedded Web UI you will need to clone and build [kamu-web-ui](https://github.com/kamu-data/kamu-web-ui) repo or use pre-built release. Now build the tool while enabling the optional feature and passing the location of the web root directory:

```shell
KAMU_WEB_UI_DIR=`pwd`/../kamu-web-ui/dist/kamu-platform/ cargo build --features kamu-cli/web-ui
```

Note: `KAMU_WEB_UI_DIR` requires absolute path

Note: in debug mode the directory content is not actually being embedded into the executable but accessed from the specified directory.


### Code Generation
Many core types in `kamu` are generated from schemas and IDLs in the [open-data-fabric](https://github.com/open-data-fabric/open-data-fabric) repository. If your work involves making changes to those - you will need to re-run the code generation tasks using:

```sh
make codegen
```

Make sure you have all related dependencies installed (see above) and that ODF repo is checked out in the same directory as `kamu-cli` repo.


## Release Procedure
1. While on the feature branch, bump the crates versions using `release` tool, e.g. `cargo run --bin release -- --major / --minor / --patch`
2. We try to stay up-to-date with all dependencies, so:
   1. Run `cargo update` to pull in any minor releases
   2. Run `cargo upgrade --dry-run` and see which packages have major upgrades - either perform them or ticket them up
   3. Run `cargo deny check` to audit updated dependencies for licenses, security advisories etc.
3. Create a `CHANGELOG` entry for the new version
4. Create PR, wait for tests, then merge
5. Checkout and pull `master`
6. Tag the latest commit with a new version: `git tag vX.Y.Z`
7. Push the tag to repo: `git push origin tag vX.Y.Z`
8. Github Actions will pick up the new tag and create a new GitHub release from it