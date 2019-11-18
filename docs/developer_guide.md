# Developer Guide

- [Developer Guide](#developer-guide)
  - [Building Locally](#building-locally)
    - [Building](#building)
  - [Release Procedure](#release-procedure)
  - [Building Hive JDBC](#building-hive-jdbc)

## Building Locally

What you'll need:

* Docker
* Java
* Scala
* Sbt
* Maven
* Spark (optional)

Note: Use [SdkMan!](https://sdkman.io/) to install these dependencies:

```shell
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 8.0.222-zulu
sdk install maven 3.6.1
sdk install sbt 1.2.8
sdk install scala 2.11.12
sdk install spark 2.4.0
```

### Building

Clone the repository with submodules:
```shell
git clone --recurse-submodules https://github.com/kamu-data/kamu-cli
```

Then do:

```shell
cd kamu-cli
make bootstrap
sbt
>> assembly
```

After the assembly is created tod run tests do:

```shell
sbt
>> test
```

> Note: Make sure the docker is running and accessible without `sudo` - it's needed by the system tests.

To use your locally-built `kamu` executable link it as so:

```shell
sudo ln -s $PWD/target/scala-2.11/kamu /usr/bin/kamu
```

## Release Procedure

TBD

* Tag versions
* Push tags
* Build assembly
* Create release and upload assembly


## Building Hive JDBC

We use `hive-jdbc` to connect the SQL shell to Spark Thrift Server exposed by Livy. Unfortunately Spark uses a very old version of Hive which has some show-stopper bugs, so we had to fork `hive` project to fix them. In order to build `kamu-cli` you'll first need to build our version of `hive`.

When you run `make bootstrap` you already downloaded a pre-built "fat Jar" of `hive-jdbc`.

Follow these steps in case you need to re-build it:

Clone [kamu-data/hive](https://github.com/kamu-data/hive), build it and install into local maven repo using:

```shell
git clone https://github.com/kamu-data/hive
cd hive
git checkout release-1.2.1-spark2-kamu
mvn -Phadoop-2 -DskipTests -Psources install
```

Note: You'll need to have a GPG key configured to sign the artifacts.
