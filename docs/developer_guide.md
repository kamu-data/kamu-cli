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

```
java: 8.0.222-zulu
maven: 3.6.1
sbt: 1.2.8
scala: 2.11.12
spark: 2.4.0
```

### Preface: Hive JDBC

We use `hive-jdbc` to connect the SQL shell to Spark Thrift Server exposed by Livy. Unfortunately Spark uses a very old version of Hive which has some show-stopper bugs, so we had to fork `hive` project to fix them. In order to build `kamu-cli` you'll first need to build our version of `hive`.

This has to be done only once. After you build it and publish the package in your local maven repository you pretty much won't have to touch `hive` ever again.

> Note: You can skip this step entirely if you're in a hurry by changing `hive-jdbc` dependency version from `1.2.1.spark2.kamu.X` to `1.2.1.spark2`.

Instructions:

Clone [kamu-data/hive](https://github.com/kamu-data/hive)

Build it and install into local maven repo using:

```sh
git checkout release-1.2.1-spark2-kamu
mvn -Phadoop-2 -DskipTests -Psources install
```

Note: You'll need to have a GPG key configured to sign the artifacts.

### Building

Clone following projects into the same folder:

* [kamu-data/kamu-core-manifests](https://github.com/kamu-data/kamu-core-manifests)
* [kamu-data/kamu-core-ingest-polling](https://github.com/kamu-data/kamu-core-ingest-polling)
* [kamu-data/kamu-core-transform-streaming](https://github.com/kamu-data/kamu-core-transform-streaming)
* [kamu-data/kamu-cli](https://github.com/kamu-data/kamu-cli)

Then do:

```sh
cd kamu-cli
sbt
>> assembly
```

After the assembly is created run tests:

```sh
sbt
>> test
```

## Release Procedure

TBD

* Tag versions
* Push tags
* Build assembly
* Create release and upload assembly
