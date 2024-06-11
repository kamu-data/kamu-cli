ODF_SPEC_DIR=../open-data-fabric
ODF_CRATE_DIR=./src/domain/opendatafabric
LICENSE_HEADER=docs/license_header.txt
TEST_LOG_PARAMS=RUST_LOG_SPAN_EVENTS=new,close RUST_LOG=debug

POSTGRES_CRATES := ./src/infra/accounts/postgres ./src/infra/task-system/postgres ./src/infra/flow-system/postgres ./src/e2e/app/cli/postgres
MYSQL_CRATES := ./src/infra/accounts/mysql
SQLITE_CRATES := ./src/infra/accounts/sqlite ./src/infra/task-system/sqlite ./src/infra/flow-system/sqlite
ALL_DATABASE_CRATES := $(POSTGRES_CRATES) $(MYSQL_CRATES) $(SQLITE_CRATES)
MIGRATION_DIRS := ./migrations/mysql ./migrations/postgres ./migrations/sqlite

###############################################################################
# Lint
###############################################################################

.PHONY: lint
lint:
	cargo fmt --check
	cargo test -p kamu-repo-tools
	cargo deny check
	# cargo udeps --all-targets
	cargo clippy --workspace --all-targets -- -D warnings
	$(foreach crate,$(ALL_DATABASE_CRATES),(cd $(crate) && cargo sqlx prepare --check);)


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
	docker pull postgres:latest
	docker stop kamu-postgres || true && docker rm kamu-postgres || true
	docker run --name kamu-postgres -p 5432:5432 -e POSTGRES_USER=root -e POSTGRES_PASSWORD=root -d postgres:latest
	$(foreach crate,$(POSTGRES_CRATES),$(call Setup_EnvFile,postgres,5432,$(crate)))
	sleep 3  # Letting the container to start
	until PGPASSWORD=root psql -h localhost -U root -p 5432 -d root -c '\q'; do sleep 3; done
	sqlx database create --database-url postgres://root:root@localhost:5432/kamu
	sqlx migrate run --source ./migrations/postgres --database-url postgres://root:root@localhost:5432/kamu

.PHONY: sqlx-local-setup-mariadb
sqlx-local-setup-mariadb:
	docker pull mariadb:latest
	docker stop kamu-mariadb || true && docker rm kamu-mariadb || true
	docker run --name kamu-mariadb -p 3306:3306 -e MARIADB_ROOT_PASSWORD=root -d mariadb:latest
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
	docker stop kamu-postgres || true && docker rm kamu-postgres || true
	$(foreach crate,$(POSTGRES_CRATES),rm $(crate)/.env -f ;)

.PHONY: sqlx-local-clean-mariadb
sqlx-local-clean-mariadb:
	docker stop kamu-mariadb || true && docker rm kamu-mariadb || true
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

###############################################################################
# Sqlx: add migration
###############################################################################

.PHONY: sqlx-add-migration
sqlx-add-migration:
	@@echo "Migration name: $${NAME:?Usage: make sqlx-add-migration NAME=new_table}"
	$(foreach dir,$(MIGRATION_DIRS),(sqlx migrate add -r $$NAME --source $(dir) );)

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

# Run all tests excluding databases using nextest and configured concurrency limits
.PHONY: test
test:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'not (test(::database::))'

.PHONY: test-full
test-full:
	$(TEST_LOG_PARAMS) cargo nextest run

# Run all tests excluding the heavy engines and databases
.PHONY: test-fast
test-fast:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'not (test(::spark::) | test(::flink::) | test(::database::))'

.PHONY: test-e2e
test-e2e:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::e2e::)'

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
# Codegen
###############################################################################

define add_license_header
	cat $(LICENSE_HEADER) > tmp
	cat $(1) >> tmp
	mv tmp $(1)
endef

define insert_text_into_beginning
	echo -e $(1) | cat - $(2) > tmp
	mv tmp $(2)
endef


.PHONY: codegen-odf-serde
codegen-odf-serde:
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_flatbuffers.py $(ODF_SPEC_DIR)/schemas > $(ODF_CRATE_DIR)/schemas/odf.fbs
	flatc -o $(ODF_CRATE_DIR)/src/serde/flatbuffers --rust --gen-onefile $(ODF_CRATE_DIR)/schemas/odf.fbs
	mv $(ODF_CRATE_DIR)/src/serde/flatbuffers/odf_generated.rs $(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs
	$(call insert_text_into_beginning, "#![allow(clippy::all)]\n#![allow(clippy::pedantic)]", "$(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	rustfmt $(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs

	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/dtos/dtos_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/dtos/dtos_dyntraits_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_dto_enum_flags.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/dtos/dtos_enum_flags_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/serde/yaml/derivations_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs

	$(call add_license_header, "$(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/dtos/dtos_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/dtos/dtos_dyntraits_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/dtos/dtos_enum_flags_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/serde/yaml/derivations_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs")


.PHONY: codegen-engine-tonic
codegen-engine-tonic:
	protoc \
		-I $(ODF_CRATE_DIR)/schemas \
		$(ODF_CRATE_DIR)/schemas/engine.proto \
		--prost_out=$(ODF_CRATE_DIR)/src/engine/grpc_generated \
		--tonic_out=$(ODF_CRATE_DIR)/src/engine/grpc_generated \
		--tonic_opt=compile_well_known_types

	rustfmt $(ODF_CRATE_DIR)/src/engine/grpc_generated/engine.rs
	rustfmt $(ODF_CRATE_DIR)/src/engine/grpc_generated/engine.tonic.rs

	$(call add_license_header, "$(ODF_CRATE_DIR)/src/engine/grpc_generated/engine.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/engine/grpc_generated/engine.tonic.rs")


.PHONY: codegen-graphql
codegen-graphql:
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_gql.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > src/adapter/graphql/src/scalars/odf_generated.rs
	$(call add_license_header, "src/adapter/graphql/src/scalars/odf_generated.rs")


.PHONY: codegen
codegen: codegen-odf-serde codegen-engine-tonic codegen-graphql

.PHONY: codegen-graphql-schema
codegen-graphql-schema:
	cargo nextest run -p kamu-adapter-graphql update_graphql_schema

.PHONY: codegen-cli-reference
codegen-cli-reference:
	cargo nextest run -p kamu-cli generate_reference_markdown
