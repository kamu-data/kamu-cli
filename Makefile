ODF_SPEC_DIR=../open-data-fabric
ODF_METADATA_CRATE_DIR=./src/odf/metadata
LICENSE_HEADER=docs/license_header.txt
TEST_LOG_PARAMS=RUST_LOG_SPAN_EVENTS=new,close RUST_LOG=debug

POSTGRES_CRATES := \
	./src/e2e/app/cli/postgres \
	./src/infra/accounts/postgres \
	./src/infra/auth-rebac/postgres \
	./src/infra/auth-web3/postgres \
	./src/infra/datasets/postgres \
	./src/infra/flow-system/postgres \
	./src/infra/messaging-outbox/postgres \
	./src/infra/task-system/postgres \
	./src/infra/webhooks/postgres

MYSQL_CRATES := \
	./src/e2e/app/cli/mysql \
	./src/infra/accounts/mysql \

SQLITE_CRATES := \
	./src/e2e/app/cli/sqlite \
	./src/infra/accounts/sqlite \
	./src/infra/auth-rebac/sqlite \
	./src/infra/auth-web3/sqlite \
	./src/infra/datasets/sqlite \
	./src/infra/flow-system/sqlite \
	./src/infra/messaging-outbox/sqlite \
	./src/infra/task-system/sqlite \
	./src/infra/webhooks/sqlite

ALL_DATABASE_CRATES := $(POSTGRES_CRATES) $(MYSQL_CRATES) $(SQLITE_CRATES)
MIGRATION_DIRS := ./migrations/mysql ./migrations/postgres ./migrations/sqlite

KAMU_CONTAINER_RUNTIME_TYPE ?= podman

###############################################################################
# Lint
###############################################################################

.PHONY: lint
lint: lint-rustfmt lint-repo lint-deps clippy lint-openapi lint-sqlx


.PHONY: lint-rustfmt
lint-rustfmt:
	cargo fmt --check


.PHONY: lint-repo
lint-repo:
	cargo test -p kamu-repo-tools


.PHONY: lint-deps
lint-deps:
	cargo deny check --hide-inclusion-graph


.PHONY: lint-sqlx
lint-sqlx:
	$(foreach crate,$(ALL_DATABASE_CRATES),(cd $(crate) && cargo sqlx prepare --check);)


.PHONY: clippy
clippy:
	cargo clippy --workspace --all-targets -- -D warnings


# See: https://github.com/IBM/openapi-validator
# ToDo replace openapi-validator version to latest once issue with linter is fixed
.PHONY: lint-openapi
lint-openapi:
	$(KAMU_CONTAINER_RUNTIME_TYPE) run --rm -t \
		-v "${PWD}:/data:ro,Z" \
  		ibmdevxsdk/openapi-validator:1.31.0 \
		--config src/adapter/http/resources/openapi/linter-config.yaml \
		--ruleset src/adapter/http/resources/openapi/linter-ruleset.yaml \
    	resources/openapi-mt.json


.PHONY: lint-udeps
lint-udeps:
	cargo udeps --all-targets

###############################################################################
# Lint (with fixes)
###############################################################################

.PHONY: lint-fix
lint-fix:
	cargo clippy --workspace --all-targets --fix --allow-dirty --allow-staged --broken-code
	cargo fmt --all


###############################################################################
# Sqlx Local Setup (create databases for local work)
###############################################################################

define Setup_EnvFile
echo "DATABASE_URL=$(1)://root:root@localhost:$(2)/kamu" > $(3)/.env;
echo "SQLX_OFFLINE=false" >> $(3)/.env;
endef

define Setup_EnvFile_Sqlite
echo "DATABASE_URL=sqlite://$(1)/kamu.sqlite.db" > $(2)/.env;
echo "SQLX_OFFLINE=false" >> $(2)/.env;
endef

.PHONY: sqlx-local-setup
sqlx-local-setup: sqlx-local-setup-postgres sqlx-local-setup-mariadb sqlx-local-setup-sqlite

.PHONY: sqlx-local-setup-postgres
sqlx-local-setup-postgres:
#	$(KAMU_CONTAINER_RUNTIME_TYPE) pull postgres:latest
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-postgres || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-postgres || true
	$(KAMU_CONTAINER_RUNTIME_TYPE) run --name kamu-postgres -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -d postgres:latest
	$(foreach crate,$(POSTGRES_CRATES),$(call Setup_EnvFile,postgres,5432,$(crate)))
	sleep 3  # Letting the container to start
	until PGPASSWORD=root psql -h localhost -U root -p 5432 -d root -c '\q'; do sleep 3; done
	sqlx database create --database-url postgres://root:root@localhost:5432/kamu
	sqlx migrate run --source ./migrations/postgres --database-url postgres://root:root@localhost:5432/kamu

.PHONY: sqlx-local-setup-mariadb
sqlx-local-setup-mariadb:
##	$(KAMU_CONTAINER_RUNTIME_TYPE) pull mariadb:latest
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-mariadb || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-mariadb || true
	$(KAMU_CONTAINER_RUNTIME_TYPE) run --name kamu-mariadb -p 3306:3306 -e MARIADB_ROOT_PASSWORD=root -d mariadb:latest
	$(foreach crate,$(MYSQL_CRATES),$(call Setup_EnvFile,mysql,3306,$(crate)))
	sleep 10  # Letting the container to start
	until mariadb -h localhost -P 3306 -u root --password=root sys --protocol=tcp -e "SELECT 'Hello'" -b; do sleep 3; done
	sqlx database create --database-url mysql://root:root@localhost:3306/kamu
	sqlx migrate run --source ./migrations/mysql --database-url mysql://root:root@localhost:3306/kamu

.PHONY: sqlx-local-setup-sqlite
sqlx-local-setup-sqlite:
	sqlx database drop -y --database-url sqlite://kamu.sqlite.db
	sqlx database create --database-url sqlite://kamu.sqlite.db
	sqlx migrate run --source ./migrations/sqlite --database-url sqlite://kamu.sqlite.db
	$(foreach crate,$(SQLITE_CRATES),$(call Setup_EnvFile_Sqlite,$(shell pwd),$(crate)))

.PHONY: sqlx-local-clean
sqlx-local-clean: sqlx-local-clean-postgres sqlx-local-clean-mariadb sqlx-local-clean-sqlite

.PHONY: sqlx-local-clean-postgres
sqlx-local-clean-postgres:
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-postgres || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-postgres || true
	$(foreach crate,$(POSTGRES_CRATES),rm $(crate)/.env -f ;)

.PHONY: sqlx-local-clean-mariadb
sqlx-local-clean-mariadb:
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-mariadb || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-mariadb || true
	$(foreach crate,$(MYSQL_CRATES),rm $(crate)/.env -f ;)

.PHONY: sqlx-local-clean-sqlite
sqlx-local-clean-sqlite:
	sqlx database drop -y --database-url sqlite://kamu.sqlite.db
	$(foreach crate,$(SQLITE_CRATES),rm $(crate)/.env -f ;)

###############################################################################
# Sqlx Prepare (update data for offline compilation)
###############################################################################

.PHONY: sqlx-prepare
sqlx-prepare:
	$(foreach crate,$(ALL_DATABASE_CRATES),(cd $(crate) && cargo sqlx prepare);)

.PHONY: sqlx-prepare-postgres
sqlx-prepare-postgres:
	$(foreach crate,$(POSTGRES_CRATES),(cd $(crate) && cargo sqlx prepare);)

.PHONY: sqlx-prepare-mariadb
sqlx-prepare-mariadb:
	$(foreach crate,$(MYSQL_CRATES),(cd $(crate) && cargo sqlx prepare);)

.PHONY: sqlx-prepare-sqlite
sqlx-prepare-sqlite:
	$(foreach crate,$(SQLITE_CRATES),(cd $(crate) && cargo sqlx prepare);)

###############################################################################
# Sqlx: add migration
###############################################################################

.PHONY: sqlx-add-migration
sqlx-add-migration:
	@@echo "Migration name: $${NAME:?Usage: make sqlx-add-migration NAME=new_table}"
	$(foreach dir,$(MIGRATION_DIRS),(sqlx migrate add -r $$NAME --source $(dir) );)

###############################################################################
# Elasticsearch
###############################################################################

ES_IMAGE ?= docker.elastic.co/elasticsearch/elasticsearch:9.2.1
KIBANA_IMAGE ?= docker.elastic.co/kibana/kibana:9.2.1

ES_TEST_PREFIX ?= kamu-test-
ES_TIMEOUT_SECS ?= 60
ES_POLL_INTERVAL_SECS ?= 1
ES_USERNAME ?= elastic
ES_PASSWORD ?= root

# Where certs will be stored on the host (generated once, reused)
ES_CERTS_DIR ?= .local/elasticsearch/certs
ES_CA_CERT ?= $(ES_CERTS_DIR)/ca/ca.crt
KIBANA_TOKEN_FILE ?= $(ES_CERTS_DIR)/kibana.service_token

# Password used to protect *.p12 files (dev/CI only)
ES_CERT_PASS ?= root

# Setup Elasticsearch + Kibana with HTTPS
.PHONY: elasticsearch-setup-https
elasticsearch-setup-https:
	$(MAKE) elasticsearch-certs
	$(MAKE) elasticsearch-net
#	$(MAKE) elasticsearch-pull
	$(MAKE) elasticsearch-stop-containers
	$(MAKE) elasticsearch-start-https
	$(MAKE) elasticsearch-wait \
		ES_URL=https://localhost:9200 \
		ES_CURL='curl -fsS --cacert "$(ES_CA_CERT)" -u "$(ES_USERNAME):$(ES_PASSWORD)"'
	$(MAKE) elasticsearch-kibana-token \
		ES_URL=https://localhost:9200
	$(MAKE) elasticsearch-test-gc \
		ES_URL=https://localhost:9200 \
		ES_CURL='curl -fsS --cacert "$(ES_CA_CERT)" -u "$(ES_USERNAME):$(ES_PASSWORD)"'
	$(MAKE) kibana-start-https

# Setup Elasticsearch + Kibana with HTTP (no TLS)
.PHONY: elasticsearch-setup-http
elasticsearch-setup-http:
	$(MAKE) elasticsearch-net
#	$(MAKE) elasticsearch-pull
	$(MAKE) elasticsearch-stop-containers
	$(MAKE) elasticsearch-start-http
	$(MAKE) elasticsearch-wait \
		ES_URL=http://localhost:9200 \
		ES_CURL='curl -fsS -u "$(ES_USERNAME):$(ES_PASSWORD)"'
	$(MAKE) elasticsearch-test-gc \
		ES_URL=http://localhost:9200 \
		ES_CURL='curl -fsS -u "$(ES_USERNAME):$(ES_PASSWORD)"'
	$(MAKE) elasticsearch-kibana-token \
		ES_URL=http://localhost:9200
	$(MAKE) kibana-start-http	

.PHONY: elasticsearch-start-https
elasticsearch-start-https:
	@echo "Starting Elasticsearch container (HTTPS)..."
	@$(KAMU_CONTAINER_RUNTIME_TYPE) run \
		--name kamu-elasticsearch \
		--network kamu-elastic-net \
		-p 9200:9200 \
		-p 9300:9300 \
		-e "ES_JAVA_OPTS=-Xms1024m -Xmx1024m" \
		-e "xpack.security.enabled=true" \
		-e "xpack.security.http.ssl.enabled=true" \
		-e "xpack.security.http.ssl.keystore.path=/usr/share/elasticsearch/config/certs/elasticsearch/http.p12" \
		-e "xpack.security.http.ssl.keystore.password=$(ES_CERT_PASS)" \
		-e "xpack.security.http.ssl.certificate_authorities=/usr/share/elasticsearch/config/certs/ca/ca.crt" \
		-e "xpack.ml.enabled=false" \
		-e "discovery.type=single-node" \
		-e "ELASTIC_PASSWORD=$(ES_PASSWORD)" \
		-e "cluster.routing.allocation.disk.threshold_enabled=false" \
		-v kamu-elastic-data:/usr/share/elasticsearch/data \
		-v "$(PWD)/$(ES_CERTS_DIR):/usr/share/elasticsearch/config/certs:ro" \
		-d \
		$(ES_IMAGE)

.PHONY: elasticsearch-start-http
elasticsearch-start-http:
	@echo "Starting Elasticsearch container (HTTP)..."
	@$(KAMU_CONTAINER_RUNTIME_TYPE) run \
		--name kamu-elasticsearch \
		--network kamu-elastic-net \
		-p 9200:9200 \
		-p 9300:9300 \
		-e "ES_JAVA_OPTS=-Xms1024m -Xmx1024m" \
		-e "xpack.security.enabled=true" \
		-e "xpack.security.http.ssl.enabled=false" \
		-e "xpack.ml.enabled=false" \
		-e "discovery.type=single-node" \
		-e "ELASTIC_PASSWORD=$(ES_PASSWORD)" \
		-e "cluster.routing.allocation.disk.threshold_enabled=false" \
		-v kamu-elastic-data:/usr/share/elasticsearch/data \
		-d \
		$(ES_IMAGE)

.PHONY: kibana-start-https
kibana-start-https:
	@sh -ec '\
		set -eu; \
		if [ ! -s "$(KIBANA_TOKEN_FILE)" ]; then \
			echo "ERROR: Kibana token file missing: $(KIBANA_TOKEN_FILE)"; \
			exit 1; \
		fi; \
		token=$$(cat "$(KIBANA_TOKEN_FILE)"); \
		$(KAMU_CONTAINER_RUNTIME_TYPE) run \
			--name kamu-kibana \
			--network kamu-elastic-net \
			-e "ELASTICSEARCH_HOSTS=https://kamu-elasticsearch:9200" \
			-e "ELASTICSEARCH_SERVICEACCOUNTTOKEN=$$token" \
			-e "ELASTICSEARCH_SSL_CERTIFICATEAUTHORITIES=/usr/share/kibana/config/certs/ca/ca.crt" \
			-e "NODE_OPTIONS=--max-old-space-size=512" \
			-p 5601:5601 \
			-v "$(PWD)/$(ES_CERTS_DIR):/usr/share/kibana/config/certs:ro,Z" \
			-d \
			$(KIBANA_IMAGE); \
	'

.PHONY: kibana-start-http
kibana-start-http:
	@sh -ec '\
		set -eu; \
		if [ ! -s "$(KIBANA_TOKEN_FILE)" ]; then \
			echo "ERROR: Kibana token file missing: $(KIBANA_TOKEN_FILE)"; \
			exit 1; \
		fi; \
		token=$$(cat "$(KIBANA_TOKEN_FILE)"); \
		$(KAMU_CONTAINER_RUNTIME_TYPE) run \
			--name kamu-kibana \
			--network kamu-elastic-net \
			-e "ELASTICSEARCH_HOSTS=http://kamu-elasticsearch:9200" \
			-e "ELASTICSEARCH_SERVICEACCOUNTTOKEN=$$token" \
			-e "NODE_OPTIONS=--max-old-space-size=512" \
			-p 5601:5601 \
			-d \
			$(KIBANA_IMAGE); \
	'

# Logic to generate CA + HTTPS certs for Elasticsearch
.PHONY: elasticsearch-certs
elasticsearch-certs:
	@sh -ec '\
		set -eu; \
		echo "Generating Elasticsearch HTTPS certs in $(ES_CERTS_DIR) ..."; \
		mkdir -p "$(ES_CERTS_DIR)/ca" "$(ES_CERTS_DIR)/elasticsearch"; \
		rm -f "$(ES_CERTS_DIR)/instances.yml" \
		      "$(ES_CERTS_DIR)/ca/ca.p12" "$(ES_CERTS_DIR)/ca/ca.crt" \
		      "$(ES_CERTS_DIR)/elasticsearch/http.p12" \
		      "$(ES_CERTS_DIR)/elasticsearch/http.zip"; \
		rm -rf "$(ES_CERTS_DIR)/elasticsearch/pem"; \
		\
		# Write instances.yml without heredocs (robust under make + /bin/sh) \
		printf "%s\n" \
			"instances:" \
			"  - name: kamu-elasticsearch" \
			"    dns: [ \"kamu-elasticsearch\", \"localhost\" ]" \
			"    ip:  [ \"127.0.0.1\" ]" \
			> "$(ES_CERTS_DIR)/instances.yml"; \
		\
		$(KAMU_CONTAINER_RUNTIME_TYPE) run --rm \
			--user 0:0 \
			-v "$(PWD)/$(ES_CERTS_DIR):/certs:Z" \
			"$(ES_IMAGE)" \
			sh -euc '"'"' \
				set -eu; \
				/usr/share/elasticsearch/bin/elasticsearch-certutil ca --silent \
					--out /certs/ca/ca.p12 \
					--pass "$(ES_CERT_PASS)"; \
				test -s /certs/ca/ca.p12; \
				/usr/share/elasticsearch/bin/elasticsearch-certutil cert --silent --pem \
					--in /certs/instances.yml \
					--ca /certs/ca/ca.p12 --ca-pass "$(ES_CERT_PASS)" \
					--out /certs/elasticsearch/http.zip; \
				test -s /certs/elasticsearch/http.zip; \
				chmod 0644 /certs/ca/ca.p12 /certs/elasticsearch/http.zip; \
			'"'"'; \
		\
		echo "Exporting CA cert to PEM (ca.crt) via openssl..."; \
		openssl pkcs12 -in "$(ES_CERTS_DIR)/ca/ca.p12" \
			-nokeys -clcerts \
			-passin pass:"$(ES_CERT_PASS)" \
			-out "$(ES_CERTS_DIR)/ca/ca.crt" >/dev/null; \
		test -s "$(ES_CERTS_DIR)/ca/ca.crt"; \
		chmod 0644 "$(ES_CERTS_DIR)/ca/ca.crt"; \
		\
		echo "Unzipping HTTP PEM bundle..."; \
		mkdir -p "$(ES_CERTS_DIR)/elasticsearch/pem"; \
		unzip -q "$(ES_CERTS_DIR)/elasticsearch/http.zip" -d "$(ES_CERTS_DIR)/elasticsearch/pem"; \
		\
		KEY_FILE=$$(find "$(ES_CERTS_DIR)/elasticsearch/pem" -type f -name "*.key" | head -n 1 || true); \
		CRT_FILE=$$(find "$(ES_CERTS_DIR)/elasticsearch/pem" -type f -name "*.crt" | head -n 1 || true); \
		if [ -z "$$KEY_FILE" ] || [ -z "$$CRT_FILE" ]; then \
			echo "ERROR: did not find .key/.crt in extracted zip"; \
			find "$(ES_CERTS_DIR)/elasticsearch/pem" -maxdepth 4 -type f -print; \
			exit 1; \
		fi; \
		\
		echo "Converting PEM -> PKCS#12 http.p12"; \
		openssl pkcs12 -export \
			-inkey "$$KEY_FILE" \
			-in "$$CRT_FILE" \
			-certfile "$(ES_CERTS_DIR)/ca/ca.crt" \
			-name "http" \
			-out "$(ES_CERTS_DIR)/elasticsearch/http.p12" \
			-passout pass:"$(ES_CERT_PASS)" >/dev/null; \
		chmod 0644 "$(ES_CERTS_DIR)/elasticsearch/http.p12"; \
		\
		echo "Validating http.p12"; \
		openssl pkcs12 -in "$(ES_CERTS_DIR)/elasticsearch/http.p12" \
			-passin pass:"$(ES_CERT_PASS)" -info -noout >/dev/null; \
		echo "OK: certs ready in $(ES_CERTS_DIR)"; \
	'

.PHONY: elasticsearch-kibana-token
elasticsearch-kibana-token:
	@sh -ec '\
		set -eu; \
		mkdir -p "$(ES_CERTS_DIR)"; \
		if [ -s "$(KIBANA_TOKEN_FILE)" ]; then \
			echo "Kibana service token already exists: $(KIBANA_TOKEN_FILE)"; \
			exit 0; \
		fi; \
		echo "Creating Kibana service account token..."; \
		resp=$$(curl -fsS $(if $(findstring https://,$(ES_URL)),--cacert "$(ES_CA_CERT)",) -u "elastic:$(ES_PASSWORD)" \
		-X POST "$(ES_URL)/_security/service/elastic/kibana/credential/token/kamu-kibana" ) \
		token=$$(printf "%s" "$$resp" | sed -n '\''s/.*"value":"\([^"]*\)".*/\1/p'\'' ); \
		if [ -z "$$token" ]; then \
			echo "ERROR: could not parse token from response:"; \
			echo "$$resp"; \
			exit 1; \
		fi; \
		printf "%s" "$$token" > "$(KIBANA_TOKEN_FILE)"; \
		chmod 0600 "$(KIBANA_TOKEN_FILE)"; \
		echo "OK: wrote token to $(KIBANA_TOKEN_FILE)"; \
	'


.PHONY: elasticsearch-net
elasticsearch-net:
	@echo "Creating Elasticsearch network..."
	$(KAMU_CONTAINER_RUNTIME_TYPE) network create kamu-elastic-net || true

.PHONY: elasticsearch-pull
elasticsearch-pull:
	@echo "Pulling Elasticsearch and Kibana images ..."
	$(KAMU_CONTAINER_RUNTIME_TYPE) pull $(ES_IMAGE)
	$(KAMU_CONTAINER_RUNTIME_TYPE) pull $(KIBANA_IMAGE)

.PHONY: elasticsearch-stop-containers
elasticsearch-stop-containers:
	@echo "Stopping and removing existing Elasticsearch and Kibana containers, if any ..."
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-elasticsearch || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-elasticsearch || true
	$(KAMU_CONTAINER_RUNTIME_TYPE) stop kamu-kibana || true && $(KAMU_CONTAINER_RUNTIME_TYPE) rm kamu-kibana || true


# Stops and removes the Elasticsearch and Kibana containers + network
.PHONY: elasticsearch-stop
elasticsearch-stop:
	$(MAKE) elasticsearch-stop-containers
	$(KAMU_CONTAINER_RUNTIME_TYPE) network rm kamu-elastic-net || true

# Stop actions + removes the Elasticsearch data volume
.PHONY: elasticsearch-clean
elasticsearch-clean:
	$(MAKE) elasticsearch-stop
	$(KAMU_CONTAINER_RUNTIME_TYPE) volume rm kamu-elastic-data || true
	@rm -rf "$(ES_CERTS_DIR)" || true

# Waits for Elasticsearch to become available
.PHONY: elasticsearch-wait
elasticsearch-wait:
	@echo "Waiting for Elasticsearch at $(ES_URL) (timeout: $(ES_TIMEOUT_SECS)s)..."
	@start_time=$$(date +%s); \
	attempt=1; \
	while true; do \
		echo "Attempt $$attempt: Checking Elasticsearch..."; \
		if $(ES_CURL) "$(ES_URL)" >/dev/null 2>&1; then \
			echo "Elasticsearch is up"; \
			break; \
		fi; \
		now=$$(date +%s); \
		if [ $$((now - start_time)) -ge $(ES_TIMEOUT_SECS) ]; then \
			echo "Timed out waiting for Elasticsearch"; \
			echo "Try: podman logs kamu-elasticsearch | tail -n 200"; \
			exit 1; \
		fi; \
		attempt=$$((attempt + 1)); \
		sleep $(ES_POLL_INTERVAL_SECS); \
	done

# Runs cleanup of all test indices in the local Elasticsearch instance
.PHONY: elasticsearch-test-gc
elasticsearch-test-gc:
	@echo "GC Elasticsearch test artifacts for prefix: $(ES_TEST_PREFIX)"

	@echo "Deleting aliases..."
	@$(ES_CURL) "$(ES_URL)/_cat/aliases?h=alias,index" 2>/dev/null | \
		awk '$$1 ~ /^$(ES_TEST_PREFIX)/ {print $$1}' | sort -u | \
		xargs -r -I{} sh -c 'echo "  Removing alias {}"; curl -fsS -X POST "$(ES_URL)/_aliases" -H "Content-Type: application/json" -d "{\"actions\":[{\"remove\":{\"index\":\"*\",\"alias\":\"{}\"}}]}" >/dev/null' || true

	@echo "Deleting indices..."
	@$(ES_CURL) "$(ES_URL)/_cat/indices/$(ES_TEST_PREFIX)*?h=index" 2>/dev/null | \
		grep -E "^$(ES_TEST_PREFIX)" | \
		xargs -r -I{} sh -c 'echo "  DELETE index {}"; curl -fsS -X DELETE "$(ES_URL)/{}?ignore_unavailable=true" >/dev/null' || true

###############################################################################
# Podman cleanups (run from time to time to preserve tests performance)
###############################################################################

.PHONY: podman-clean
podman-clean:
	podman ps -aq | xargs --no-run-if-empty podman rm -f
	podman images -f dangling=true -q | xargs --no-run-if-empty podman rmi
	podman volume ls -q | xargs --no-run-if-empty podman volume rm
	podman network prune -f

###############################################################################
# Test
###############################################################################

# Executes the setup actions for tests (e.g. pulling images)
.PHONY: test-setup
test-setup:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::setup::)' --no-capture

# Run all tests excluding databases & search using nextest and configured concurrency limits
.PHONY: test
test:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'not (test(::database::) | test(::elasticsearch::))'

.PHONY: test-full
test-full:
	$(TEST_LOG_PARAMS) cargo nextest run

# Run all tests excluding the heavy engines and databases
.PHONY: test-fast
test-fast:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'not (test(::spark::) | test(::flink::) | test(::database::) | test(::elasticsearch::))'

.PHONY: test-e2e
test-e2e:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::e2e::)'

.PHONY: test-database
test-database:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::database::)'

.PHONY: test-elasticsearch
test-elasticsearch:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::elasticsearch::)'

###############################################################################
# Benchmarking
###############################################################################

.PHONY: bench
bench:
	cargo bench


###############################################################################
# Release
###############################################################################

.PHONY: release-patch
release-patch:
	cargo run -p kamu-repo-tools --bin release -- --patch

.PHONY: release-minor
release-minor:
	cargo run -p kamu-repo-tools --bin release -- --minor

.PHONY: release-major
release-major:
	cargo run -p kamu-repo-tools --bin release -- --major


###############################################################################
# Generated resources
###############################################################################

.PHONY: resources
resources:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::resourcegen::)'


###############################################################################
# Codegen
###############################################################################

define odf_codegen
	cd $(ODF_SPEC_DIR) && cargo run -q -- codegen $(1) > $(realpath $(2))
endef

define add_license_header
	cat $(LICENSE_HEADER) > tmp
	cat $(1) >> tmp
	mv tmp $(1)
endef

define insert_text_into_beginning
	echo -e $(1) | cat - $(2) > tmp
	mv tmp $(2)
endef

.PHONY: codegen-odf-dtos
codegen-odf-dtos:
	$(call odf_codegen, rust-dtos, $(ODF_METADATA_CRATE_DIR)/src/dtos/dtos_generated.rs)
	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/dtos/dtos_generated.rs")
	rustfmt $(ODF_METADATA_CRATE_DIR)/src/dtos/dtos_generated.rs


.PHONY: codegen-odf-flatbuffers-schema
codegen-odf-flatbuffers-schema:
	$(call odf_codegen, flatbuffers-schema, $(ODF_METADATA_CRATE_DIR)/schemas/odf.fbs)


# Requires `flatbuffers` package
# TODO: Consider using devbox to pin specific versions
.PHONY: codegen-odf-serde-flatbuffers
codegen-odf-serde-flatbuffers:
	flatc -o $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers --rust --gen-onefile $(ODF_METADATA_CRATE_DIR)/schemas/odf.fbs
	mv $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/odf_generated.rs $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs
	$(call insert_text_into_beginning, "// Generated with flatc=$(shell flatc --version)", "$(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	$(call insert_text_into_beginning, "#![allow(clippy::all)]\n#![allow(clippy::pedantic)]", "$(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	rustfmt $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs

	# Applying patch for https://github.com/kamu-data/kamu-cli/issues/1084
	patch --no-backup-if-mismatch -p0 < src/odf/metadata/schemas/flatc-issue-1084.patch

	$(call odf_codegen, rust-serde-flatbuffers, $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs)
	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs")
	rustfmt $(ODF_METADATA_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs


.PHONY: codegen-odf-serde-yaml
codegen-odf-serde-yaml:
	$(call odf_codegen, rust-serde, $(ODF_METADATA_CRATE_DIR)/src/serde/yaml/derivations_generated.rs)
	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/serde/yaml/derivations_generated.rs")
	rustfmt $(ODF_METADATA_CRATE_DIR)/src/serde/yaml/derivations_generated.rs


# Requires `protobuf`, `protoc-gen-prost`, `protoc-gen-tonic` packages
# TODO: Consider using devbox to pin specific versions
.PHONY: codegen-engine-tonic
codegen-engine-tonic:
	protoc \
		-I $(ODF_METADATA_CRATE_DIR)/schemas \
		$(ODF_METADATA_CRATE_DIR)/schemas/engine.proto \
		--prost_out=$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated \
		--tonic_out=$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated \
		--tonic_opt=compile_well_known_types

	$(call insert_text_into_beginning, "// Generated with protoc=$(shell protoc --version)", "$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.rs")
	$(call insert_text_into_beginning, "// Generated with protoc-gen-prost=$(strip $(shell protoc-gen-prost --version))", "$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.rs")
	$(call insert_text_into_beginning, "// Generated with protoc-gen-tonic=$(strip $(shell protoc-gen-tonic --version))", "$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.rs")

	rustfmt $(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.rs
	rustfmt $(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.tonic.rs

	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.rs")
	$(call add_license_header, "$(ODF_METADATA_CRATE_DIR)/src/engine/grpc_generated/engine/engine.tonic.rs")


.PHONY: codegen-graphql
codegen-graphql:
	$(call odf_codegen, rust-graphql, src/adapter/graphql/src/scalars/odf_generated.rs)
	$(call add_license_header, "src/adapter/graphql/src/scalars/odf_generated.rs")
	rustfmt src/adapter/graphql/src/scalars/odf_generated.rs


.PHONY: codegen
codegen: codegen-odf-dtos \
	codegen-odf-flatbuffers-schema \
	codegen-odf-serde-flatbuffers \
	codegen-odf-serde-yaml \
	codegen-engine-tonic \
	codegen-graphql


# Executes codegen action in a nix flake environment that contains necessary tools like `flatc` and `protoc`
.PHONY: codegen-nix
codegen-nix:
	nix develop .config -c make codegen
