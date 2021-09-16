ODF_DIR=../../open-data-fabric
RUSTFMT=rustfmt --edition 2018

.PHONY: codegen
codegen:
	python $(ODF_DIR)/tools/jsonschema_to_flatbuffers.py $(ODF_DIR)/schemas > schemas/odf.fbs
	flatc -o src/serde/flatbuffers --rust --gen-onefile flatbuffers/odf.fbs
	$(RUSTFMT) src/serde/flatbuffers/odf_generated.rs

	python $(ODF_DIR)/tools/jsonschema_to_rust_dtos.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dtos.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_traits.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/dynamic.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_serde_yaml.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/yaml/generated.rs
	python $(ODF_DIR)/tools/jsonschema_to_rust_flatbuffers.py $(ODF_DIR)/schemas | $(RUSTFMT) > src/serde/flatbuffers/convertors.rs
