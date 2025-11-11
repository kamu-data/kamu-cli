ifndef IMAGE_REPO
$(error IMAGE_REPO is not set)
endif

ifndef KAMU_VERSION
$(error KAMU_VERSION is not set)
endif

IMAGE_TAG = $(KAMU_VERSION).1
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
		-t $(IMAGE_REPO)/sqlx-cli-with-migrations:$(IMAGE_TAG) \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/sqlx-cli-with-migrations:$(IMAGE_TAG)

	docker tag $(IMAGE_REPO)/sqlx-cli-with-migrations:$(IMAGE_TAG) $(IMAGE_REPO)/sqlx-cli-with-migrations:latest
	docker push $(IMAGE_REPO)/sqlx-cli-with-migrations:latest


################################################################################
