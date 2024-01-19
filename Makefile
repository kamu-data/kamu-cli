ODF_SPEC_DIR=../open-data-fabric
ODF_CRATE_DIR=./src/domain/opendatafabric
LICENSE_HEADER=docs/license_header.txt
TEST_LOG_PARAMS=RUST_LOG_SPAN_EVENTS=new,close RUST_LOG=debug


###############################################################################
# Lint
###############################################################################

.PHONY: lint
lint:
	cargo fmt --check
	cargo test -p kamu-repo-tools
	cargo deny check
	cargo clippy --all --all-targets


###############################################################################
# Lint (with fixes)
###############################################################################

.PHONY: lint-fix
lint-fix:
	cargo clippy --fix --all-targets --allow-dirty --allow-staged --broken-code
	cargo fmt --all


###############################################################################
# Test
###############################################################################

# Executes the setup actions for tests (e.g. pulling images)
.PHONY: test-fast
test-setup:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'test(::setup::)' --no-capture

# Run all tests using nextest and configured concurrency limits
.PHONY: test
test:
	$(TEST_LOG_PARAMS) cargo nextest run

# Run all tests excluding the heavy engines
.PHONY: test-fast
test-fast:
	$(TEST_LOG_PARAMS) cargo nextest run -E 'not (test(::spark::) | test(::flink::))'


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
	echo $(1) | cat - $(2) > tmp
	mv tmp $(2)
endef


.PHONY: codegen-odf-serde
codegen-odf-serde:
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_flatbuffers.py $(ODF_SPEC_DIR)/schemas > $(ODF_CRATE_DIR)/schemas/odf.fbs
	flatc -o $(ODF_CRATE_DIR)/src/serde/flatbuffers --rust --gen-onefile $(ODF_CRATE_DIR)/schemas/odf.fbs
	mv $(ODF_CRATE_DIR)/src/serde/flatbuffers/odf_generated.rs $(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs
	$(call insert_text_into_beginning, "#![allow(clippy::all)]", "$(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	rustfmt $(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs

	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/dtos/dtos_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/dtos/dtos_dyntraits_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/serde/yaml/derivations_generated.rs
	python $(ODF_SPEC_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_SPEC_DIR)/schemas \
		| rustfmt > $(ODF_CRATE_DIR)/src/serde/flatbuffers/convertors_generated.rs

	$(call add_license_header, "$(ODF_CRATE_DIR)/src/serde/flatbuffers/proxies_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/dtos/dtos_generated.rs")
	$(call add_license_header, "$(ODF_CRATE_DIR)/src/dtos/dtos_dyntraits_generated.rs")
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
