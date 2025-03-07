IMAGE_PLATFORMS = linux/amd64,linux/arm64
IMAGE_REPO = ghcr.io/kamu-data
KAMU_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')

# Keep in sync with versions of Jupyter and Minio in docker-compose.yml
DEMO_VERSION = 0.17.7

#########################################################################################

.PHONY: prepare-multi-arch
prepare-multi-arch-build:
	docker buildx \
		create \
		--platform $(IMAGE_PLATFORMS) \
		--use \
		--name kamu-multi-platform-builder

#########################################################################################

.PHONY: all-examples
all-examples:
	rsync -avu --delete --exclude=".*/" "../../examples/" "user-home/XX - Other Examples"

# Build image for local testing purposes (using the release kamu-cli)
.PHONY: jupyter-local
jupyter-local: all-examples
	docker build \
		-f Dockerfile.jupyter \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		.

# Build image for local testing purposes (using the local kamu-cli)
.PHONY: jupyter-local-debug
jupyter-local-debug: jupyter
	mkdir -p ./tmp/
	cargo build -p kamu-cli
	cp -f ../../target/debug/kamu-cli ./tmp/kamu-cli
	# We overwrite the local image: based on the name (--build-arg), which assign later (-t).
	docker build \
		-f Dockerfile.debug.jupyter \
		--build-arg BASE_DEMO_JUPYTER_IMAGE=$(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		-t $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		.

# Build and push multi-arch image
# -------------------------------
# Error: "libc-bin segfaults (script subprocess returned error exit status 139)".
#
# If you suddenly encounter this error (for example on Fedora), here's what will help you:
# $ docker pull tonistiigi/binfmt:latest
# $ docker run --privileged --rm tonistiigi/binfmt --uninstall qemu-*
# $ docker run --privileged --rm tonistiigi/binfmt --install all
#
# Based on: https://github.com/docker/buildx/issues/314
.PHONY: jupyter-multi-arch
jupyter-multi-arch: all-examples
	docker buildx build \
		-f Dockerfile.jupyter \
		--push \
		--platform $(IMAGE_PLATFORMS) \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		.

#########################################################################################

# Build image for local testing purposes
.PHONY: minio
minio:
	docker build \
		-f Dockerfile.minio \
		-t $(IMAGE_REPO)/kamu-cli-demo-minio:$(DEMO_VERSION) \
		.

# Build and push multi-arch image
.PHONY: minio-multi-arch
minio-multi-arch:
	docker buildx build \
		-f Dockerfile.minio \
		--push \
		--platform $(IMAGE_PLATFORMS) \
		-t $(IMAGE_REPO)/kamu-cli-demo-minio:$(DEMO_VERSION) \
		.

#########################################################################################

.PHONY: minio-data
minio-data:
	rm -rf minio-data && \
		mkdir -p minio-data/kamu-hub && \
		mkdir minio-data/tmp && \
		cd minio-data/tmp && \
		kamu init && \
		kamu repo add kamu-hub file:///$(shell pwd)/minio-data/kamu-hub && \
		kamu repo list && \
		kamu add -r "../../user-home/01 - Kamu Basics (COVID-19 example)/datasets/" && \
		kamu pull --all && \
		kamu push covid19.british-columbia.case-details --to kamu-hub/covid19.british-columbia.case-details && \
		kamu push covid19.canada.case-details --to kamu-hub/covid19.canada.case-details && \
		kamu push covid19.canada.daily-cases --to kamu-hub/covid19.canada.daily-cases && \
		kamu push covid19.ontario.case-details --to kamu-hub/covid19.ontario.case-details && \
		kamu search && \
		cd .. && \
		rm -rf tmp

#########################################################################################

.PHONY: strip-notebooks
strip-notebooks:
	find . -name '.ipynb_checkpoints' -type d -prune -exec rm -rf {} \;
	find . -name '*.ipynb' -type f -exec nbstripout {} \;

#########################################################################################

podman-images: podman-images/
podman-images/:
	docker run --rm --entrypoint bash $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		-c "kamu init --pull-images --list-only | grep -e spark -e flink" | \
		xargs -n 1 -- podman --root podman-images pull

	# For Some reason podman creates files owned by a different users, so we need sudo to chmod them :(
	sudo chmod -R a+rx podman-images

#########################################################################################

.PHONY: clean
clean:
	rm -rf minio-data podman-images tmp
