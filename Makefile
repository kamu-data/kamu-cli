.PHONY: bootstrap
bootstrap:
	mkdir -p core.coordinator/lib/
	cd core.coordinator/lib/ \
	&& rm -f *.jar \
	&& wget --no-verbose "https://github.com/kamu-data/hive/releases/download/1.2.1.spark2.kamu.1/hive-jdbc-1.2.1.spark2.kamu.1-standalone.jar"
