KAMU_BASE_VERSION=$(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')
IMAGE_REPO=ghcr.io/kamu-data
IMAGE_NAME=kamu-dev-base
TARGET=x86_64-unknown-linux-gnu

# Branch name (feature/private-dataset -> feature-private-dataset)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD | tr -cd '[:alnum:]_-')
# Short commit hash (603bf0885)
COMMIT=$(shell git rev-parse --short HEAD)
# Example: feature-private-dataset-603bf0885
TAG=$(BRANCH)-$(COMMIT)
# Example: feature-private-dataset-latest
TAG_LATEST=$(BRANCH)-latest

ESC=\033
GREEN=$(ESC)[32m
BOLD=$(ESC)[1m
RESET=$(ESC)[0m

################################################################################


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
	@echo
	@echo -e "$(GREEN)✨ Image successfully built:$(RESET)"
	@echo -e "· $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG)"
	@echo


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG)
	docker tag $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG) $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG_LATEST)
	docker push $(IMAGE_REPO)/$(IMAGE_NAME):$(TAG_LATEST)

	@echo
	@echo -e "$(GREEN)✨ Image successfully pushed:$(RESET)"
	@echo -e "· $(BOLD)https://$(IMAGE_REPO)/$(IMAGE_NAME):$(TAG) $(RESET)"
	@echo -e "· $(BOLD)https://$(IMAGE_REPO)/$(IMAGE_NAME):$(TAG_LATEST) $(RESET)"
	@echo


################################################################################
