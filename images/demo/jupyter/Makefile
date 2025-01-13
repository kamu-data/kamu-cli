PLATFORM=linux/amd64
BASE_IMAGE:=quay.io/jupyter/minimal-notebook:2024-12-09


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
#
# The stupidity of Python package management ecosystems is unbelievabe. Jupyter images are
# based on conda, but some packages we have are only installable by pip. We want to make
# environment reproducible, but `conda env export` in `dependencies.pip` section includes only
# **top-level** packages, ignoring all direct and transitive dependencies.
# 
# To make environment fully reproducible we have to resort to:
# - Run `conda env export` to lock conda packages (and part of pip packages)
# - Strig partial `pip` packages from conda env
# - Run `pip freeze` to lock pip packages
# - Filter out conda packages from `pip freeze` output
# - Merge the rest into `dependencies.pip` section of `conda env export`
.PHONY: requirements-install-freeze
requirements-install-freeze:
	pip install -r requirements/$(PLATFORM)/requirements.in
	pip freeze > requirements/$(PLATFORM)/requirements.txt
	mamba env export --no-builds > requirements/$(PLATFORM)/env.yaml
	python ./merge_requirements.py requirements/$(PLATFORM)/env.yaml requirements/$(PLATFORM)/requirements.txt
	rm requirements/$(PLATFORM)/requirements.txt
