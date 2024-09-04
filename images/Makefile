IMAGE_PLATFORMS = linux/amd64,linux/arm64
IMAGE_REPO = ghcr.io/kamu-data
IMAGE_JUPYTER_TAG = 0.6.3

KAMU_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')

SQLX_VERSION = 0.8.0
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

kamu-base-feature-branch \
	kamu-base-feature-branch-push \
	kamu-base-with-data-feature-branch \
	kamu-base-with-data-feature-branch-push \
	kamu-base-with-data-mt-feature-branch \
	kamu-base-with-data-mt-feature-branch-push \
		: FEATURE_TAG = $(shell git describe --tags --exact-match | grep -E "^feature-.+-v[0-9]+$$")

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


.PHONY: kamu-base-feature-branch
kamu-base-feature-branch:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION)-$(FEATURE_TAG) \
		kamu-base/


.PHONY: kamu-base-feature-branch-push
kamu-base-feature-branch-push:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	docker push $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION)-$(FEATURE_TAG)


################################################################################


.PHONY: kamu-base-with-data
kamu-base-with-data:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


.PHONY: kamu-base-with-data-feature-branch
kamu-base-with-data-feature-branch:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		workspace image


.PHONY: kamu-base-with-data-push
kamu-base-with-data-push:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


.PHONY: kamu-base-with-data-feature-branch-push
kamu-base-with-data-feature-branch-push:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		image-push


################################################################################


.PHONY: kamu-base-with-data-mt
kamu-base-with-data-mt:
	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


.PHONY: kamu-base-with-data-mt-feature-branch
kamu-base-with-data-mt-feature-branch:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		workspace image


.PHONY: kamu-base-with-data-mt-push
kamu-base-with-data-mt-push:
	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


.PHONY: kamu-base-with-data-mt-feature-branch-push
kamu-base-with-data-mt-feature-branch-push:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-data-mt/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		image-push


################################################################################


.PHONY: kamu-base-with-env-var-storage
kamu-base-with-env-var-storage:
	make -C kamu-base-with-env-var-storage/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


.PHONY: kamu-base-with-env-var-storage-branch
kamu-base-with-env-var-storage-branch:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-env-var-storage/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		workspace image


.PHONY: kamu-base-with-env-var-storage-push
kamu-base-with-env-var-storage-push:
	make -C kamu-base-with-env-var-storage/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


.PHONY: kamu-base-with-env-var-storage-push-feature-branch-push
kamu-base-with-env-var-storage-push-feature-branch-push:
	@if [ -z $(FEATURE_TAG) ]; then \
		echo "Error getting FEATURE_TAG"; \
		exit 1; \
	fi

	make -C kamu-base-with-env-var-storage/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		TAG_POSTFIX=$(FEATURE_TAG) \
		image-push


################################################################################

sqlx-cli sqlx-cli-push: SQLX_CLI_IMAGE_EXISTS = $(shell \
		docker manifest inspect $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) > /dev/null 2>&1 \
			&& echo yes \
			|| echo no \
	)


.PHONY: sqlx-cli
sqlx-cli:
	@if [ "$(SQLX_CLI_IMAGE_EXISTS)" = "yes" ]; then \
		echo "sqlx-cli:$(IMAGE_SQLX_TAG) image exists, skip this step" ; \
	else \
		docker build \
			--build-arg VERSION=$(SQLX_VERSION) \
			-t $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) \
			sqlx-cli/ \
			; \
	fi


.PHONY: sqlx-push
sqlx-cli-push:
	@if [ "$(SQLX_CLI_IMAGE_EXISTS)" = "yes" ]; then \
		echo "sqlx-cli:$(IMAGE_SQLX_TAG) image exists, skip this step" ; \
	else \
		docker push $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) ; \
		docker tag $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) $(IMAGE_REPO)/sqlx-cli:$(SQLX_VERSION) ; \
		docker push $(IMAGE_REPO)/sqlx-cli:$(SQLX_VERSION) ; \
		docker tag $(IMAGE_REPO)/sqlx-cli:$(IMAGE_SQLX_TAG) $(IMAGE_REPO)/sqlx-cli:latest ; \
		docker push $(IMAGE_REPO)/sqlx-cli:latest ; \
	fi


################################################################################


.PHONY: sqlx-cli-with-migrations
sqlx-cli-with-migrations:
	make -C sqlx-cli-with-migrations/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		IMAGE_SQLX_TAG=$(IMAGE_SQLX_TAG) \
		image


.PHONY: sqlx-cli-with-migrations-push
sqlx-cli-with-migrations-push:
	make -C sqlx-cli-with-migrations/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		IMAGE_SQLX_TAG=$(IMAGE_SQLX_TAG) \
		image-push


################################################################################
