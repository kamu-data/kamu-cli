################################################################################

KAMU_VERSION=$(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu-core") | .version')
BRANCH_NAME=$(shell git rev-parse --abbrev-ref HEAD | sed 's/\//-/g')
SHA_SHORT=$(shell git rev-parse --short HEAD)

IMAGE_REPO=ghcr.io/kamu-data
IMAGE_NAME=sqlx-cli-with-migrations-dev
IMAGE_TAG=$(KAMU_VERSION)-$(BRANCH_NAME)-$(SHA_SHORT)
IMAGE_SQLX_TAG = latest

################################################################################


.PHONY: migrations
migrations:
	rm -rf ./migrations/
	cp -r ../../migrations/ ./migrations/


.PHONY: image
image: migrations
	docker build \
		--build-arg IMAGE_SQLX_TAG=$(IMAGE_SQLX_TAG) \
		-t $(IMAGE_REPO)/$(IMAGE_NAME):$(IMAGE_TAG) \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/$(IMAGE_NAME):$(IMAGE_TAG)


################################################################################
