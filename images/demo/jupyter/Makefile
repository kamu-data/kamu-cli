PLATFORM=linux/amd64
BASE_IMAGE:=quay.io/jupyter/minimal-notebook:2024-02-13


# Requires QEMU
# Refer to DEVELOPER.md when building multi-platform images
.PHONY: requirements
requirements:
	make requirements-platform PLATFORM=linux/amd64
	make requirements-platform PLATFORM=linux/arm64


.PHONY: requirements-platform
requirements-platform:
	docker run \
		--platform $(PLATFORM) \
		--rm \
		-u root \
		-v `pwd`:/opt/mnt \
		--workdir /opt/mnt \
		$(BASE_IMAGE) \
		bash -c "mamba install -y make && make requirements-install-freeze PLATFORM=$(PLATFORM)"


# Executed from inside the base image
.PHONY: requirements-install-freeze
requirements-install-freeze:
	mamba install -y --file requirements/$(PLATFORM)/requirements.in
	mamba list --export > requirements/$(PLATFORM)/requirements.txt
