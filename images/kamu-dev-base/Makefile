KAMU_BASE_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')
IMAGE_REPO=ghcr.io/kamu-data
IMAGE_NAME=kamu-dev-base
TARGET=x86_64-unknown-linux-gnu
# Branch name
TAG=$(shell git rev-parse --abbrev-ref HEAD | sed 's/\//-/g')


.PHONY: build
build:
	# cargo binstall cross
	cd ../../ && RUSTFLAGS="" cross build -p kamu-cli --release --target=$(TARGET) --features ingest-ftp


.PHONY: image
image: build
	mkdir -p ./tmp
	cp ../../target/$(TARGET)/release/kamu-cli tmp/kamu-cli
	docker build \
		--build-arg KAMU_BASE_VERSION=$(KAMU_BASE_VERSION) \
		-t $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG) \
		.
	rm -rf ./tmp


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG)
