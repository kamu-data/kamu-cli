ODF_DIR=../open-data-fabric
RUSTFMT=rustfmt --edition 2018
LICENSE_HEADER=docs/license_header.txt

define add_license_header
	cat $(LICENSE_HEADER) > tmp
	cat $(1) >> tmp
	mv tmp $(1)
endef

.PHONY: codegen
codegen:
	python $(ODF_DIR)/tools/jsonschema_to_flatbuffers.py $(ODF_DIR)/schemas > opendatafabric/schemas/odf.fbs
	flatc -o opendatafabric/src/serde/flatbuffers --rust --gen-onefile opendatafabric/schemas/odf.fbs
	mv opendatafabric/src/serde/flatbuffers/odf_generated.rs opendatafabric/src/serde/flatbuffers/proxies_generated.rs
	$(RUSTFMT) opendatafabric/src/serde/flatbuffers/proxies_generated.rs

	python $(ODF_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_DIR)/schemas | $(RUSTFMT) > opendatafabric/src/dtos/dtos_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_DIR)/schemas | $(RUSTFMT) > opendatafabric/src/dtos/dtos_dyntraits_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_DIR)/schemas | $(RUSTFMT) > opendatafabric/src/serde/yaml/derivations_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_DIR)/schemas | $(RUSTFMT) > opendatafabric/src/serde/flatbuffers/convertors_generated.rs

	$(call add_license_header, "opendatafabric/src/serde/flatbuffers/proxies_generated.rs")
	$(call add_license_header, "opendatafabric/src/dtos/dtos_generated.rs")
	$(call add_license_header, "opendatafabric/src/dtos/dtos_dyntraits_generated.rs")
	$(call add_license_header, "opendatafabric/src/serde/yaml/derivations_generated.rs")
	$(call add_license_header, "opendatafabric/src/serde/flatbuffers/convertors_generated.rs")

	python $(ODF_DIR)/tools/jsonschema_to_rust_gql.py $(ODF_DIR)/schemas | $(RUSTFMT) > kamu-adapter-graphql/src/scalars/odf_generated.rs
	$(call add_license_header, "kamu-adapter-graphql/src/scalars/odf_generated.rs")
