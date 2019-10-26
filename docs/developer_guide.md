# Developer Guide

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

### Preface: Hive JDBC

We use `hive-jdbc` to connect the SQL shell to Spark Thrift Server exposed by Livy. Unfortunately Spark uses a very old version of Hive which has some show-stopper bugs, so we had to fork `hive` project to fix them. In order to build `kamu-cli` you'll first need to build our version of `hive`.

This has to be done only once. After you build it and publish the package in your local maven repository you pretty much won't have to touch `hive` ever again.

> Note: You can skip this step entirely if you're in a hurry by running `sbt` with `-Dhive.jdbc.version=upstream`. You can use this option if your work is not related to SQL shell and JDBC connector.

Instructions:

Clone [kamu-data/hive](https://github.com/kamu-data/hive), build it and install into local maven repo using:

```shell
git clone https://github.com/kamu-data/hive
cd hive
git checkout release-1.2.1-spark2-kamu
mvn -Phadoop-2 -DskipTests -Psources install
```

Note: You'll need to have a GPG key configured to sign the artifacts.

### Building

Clone the repository with submodules:
```shell
git clone --recurse-submodules https://github.com/kamu-data/kamu-cli
```

Then do:

```shell
cd kamu-cli
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
