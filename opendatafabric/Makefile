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
	$(RUSTFMT) src/serde/flatbuffers/odf_generated.rs

	python $(ODF_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dtos.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dynamic.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/yaml/generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/flatbuffers/convertors.rs

	$(call add_license_header, "src/serde/flatbuffers/odf_generated.rs")
	$(call add_license_header, "src/dtos.rs")
	$(call add_license_header, "src/dynamic.rs")
	$(call add_license_header, "src/serde/yaml/generated.rs")
	$(call add_license_header, "src/serde/flatbuffers/convertors.rs")
