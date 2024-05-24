IMAGE_PLATFORMS = linux/amd64,linux/arm64
IMAGE_REPO = ghcr.io/kamu-data
IMAGE_JUPYTER_TAG = 0.6.1

KAMU_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')

SQLX_VERSION = 0.7.4
IMAGE_SQLX_TAG = $(SQLX_VERSION)-1

################################################################################


# Build image for local testing purposes
.PHONY: jupyter
jupyter:
	docker build \
		-t $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG) \
		jupyter/


# Build and push multi-arch image
.PHONY: jupyter-multi-arch
jupyter-multi-arch:
	docker buildx build \
		--push \
		--platform $(IMAGE_PLATFORMS) \
		-t $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG) \
		jupyter/


################################################################################


.PHONY: kamu-base
kamu-base:
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION) \
		kamu-base/


.PHONY: kamu-base-push
kamu-base-push:
	docker push $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION)
	docker tag $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION) $(IMAGE_REPO)/kamu-base:latest
	docker push $(IMAGE_REPO)/kamu-base:latest


################################################################################


.PHONY: kamu-base-with-data
kamu-base-with-data:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


.PHONY: kamu-base-with-data-push
kamu-base-with-data-push:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


################################################################################


.PHONY: kamu-base-with-data-mt
kamu-base-with-data-mt:
	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


.PHONY: kamu-base-with-data-mt-push
kamu-base-with-data-mt-push:
	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


################################################################################


.PHONY: sqlx-cli
sqlx-cli:
	docker build \
		--build-arg VERSION=$(SQLX_VERSION) \
		-t $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) \
		sqlx-cli/


.PHONY: sqlx-push
sqlx-cli-push:
	docker push $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG)

	docker tag $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) $(IMAGE_REPO)/sqlx-cli:$(SQLX_VERSION)
	docker push $(IMAGE_REPO)/sqlx-cli:$(SQLX_VERSION)

	docker tag $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) $(IMAGE_REPO)/sqlx-cli:latest
	docker push $(IMAGE_REPO)/sqlx-cli:latest


################################################################################


.PHONY: sqlx-cli-with-migrations
sqlx-cli-with-migrations:
	make -C sqlx-cli-with-migrations/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		IMAGE_SQLX_TAG=$(IMAGE_SQLX_TAG) \
		image


.PHONY: sqlx-push
sqlx-cli-with-migrations-push:
	make -C sqlx-cli-with-migrations/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		IMAGE_SQLX_TAG=$(IMAGE_SQLX_TAG) \
		image-push

################################################################################
