# Inputs:
# - IMAGE_REPO
# - KAMU_VERSION
KAMU_S3_URL=s3://datasets.kamu.dev/example/


.PHONY: image
image:
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:latest-with-data \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/kamu-base:latest-with-data


.PHONY: workspace
workspace:
	mkdir -p workspace && \
	cd workspace/ && \
	KAMU_S3_URL=$(KAMU_S3_URL) ../init-workspace.sh
