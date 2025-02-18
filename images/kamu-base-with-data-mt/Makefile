KAMU_VERSION=$(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')
IMAGE_REPO=ghcr.io/kamu-data
TAG=latest-with-data-mt


.PHONY: image
image:
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:$(TAG) \
		.


.PHONY: image-push
image-push:
	docker push $(IMAGE_REPO)/kamu-base:$(TAG)


.PHONY: workspace
workspace:
	KAMU_VERSION=$(KAMU_VERSION) \
	mkdir -p workspace && \
	cd workspace/ && \
	../init-workspace.py

.PHONY: clean
clean:
	rm -rf workspace
