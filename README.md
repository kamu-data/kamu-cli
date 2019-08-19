# Kamu CLI

# Building Locally

Clone [kamu-data/hive](https://github.com/kamu-data/hive)

Build it and install into local maven repo using:

```sh
mvn -Phadoop-2 -DskipTests -Psources install
```

Note: You'll need to have a GPG key configured to sign the artifacts.
