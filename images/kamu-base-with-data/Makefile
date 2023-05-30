# Inputs:
# - IMAGE_REPO
# - KAMU_VERSION


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
	../init-workspace.sh
