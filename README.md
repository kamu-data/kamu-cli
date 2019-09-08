# Kamu CLI

[Installation](docs/install.md)

[How it Works](docs/architecture.md)

[Developer Guide](docs/developer_guide.md)

Step by step guide:
- Install the tools:
```shell
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install java 8.0.222-zulu
sdk install maven 3.6.1
sdk install sbt 1.2.8
sdk install scala 2.11.12
sdk install spark 2.4.0
```
- Build local kamu version of Hive(requires GnuPG to sign it):
```shell
git clone https://github.com/kamu-data/hive
cd hive
git checkout release-1.2.1-spark2-kamu
mvn -Phadoop-2 -DskipTests -Psources install
cd ..
```
- Build kamu
```shell
git clone https://github.com/kamu-data/kamu-cli
git clone https://github.com/kamu-data/kamu-core-ingest-polling
git clone https://github.com/kamu-data/kamu-core-manifests
git clone https://github.com/kamu-data/kamu-core-transform-streaming
cd kamu-cli
sbt assembly
```
- Link the executable, so it can be run from anywhere:
```shell
sudo ln -s $PWD/target/scala-2.11/kamu /usr/bin/kamu
cd ..
```
- Make sure the docker is accessible and running before using kamu:
```shell
docker ps
```
- Download the repo
- Pull the data
- Play with it
