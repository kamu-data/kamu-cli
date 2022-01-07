ODF_DIR=../../open-data-fabric
RUSTFMT=rustfmt --edition 2018
LICENSE_HEADER=../docs/license_header.txt

define add_license_header
	cat $(LICENSE_HEADER) > tmp
	cat $(1) >> tmp
	mv tmp $(1)
endef

.PHONY: codegen
codegen:
	python $(ODF_DIR)/tools/jsonschema_to_flatbuffers.py $(ODF_DIR)/schemas > schemas/odf.fbs
	flatc -o src/serde/flatbuffers --rust --gen-onefile schemas/odf.fbs
	mv src/serde/flatbuffers/odf_generated.rs src/serde/flatbuffers/proxies_generated.rs
	$(RUSTFMT) src/serde/flatbuffers/proxies_generated.rs

	python $(ODF_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dtos/dtos_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dtos/dtos_dyntraits_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/yaml/derivations_generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/flatbuffers/convertors_generated.rs

	$(call add_license_header, "src/serde/flatbuffers/proxies_generated.rs")
	$(call add_license_header, "src/dtos/dtos_generated.rs")
	$(call add_license_header, "src/dtos/dtos_dyntraits_generated.rs")
	$(call add_license_header, "src/serde/yaml/derivations_generated.rs")
	$(call add_license_header, "src/serde/flatbuffers/convertors_generated.rs")
