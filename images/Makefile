IMAGE_REPO = ghcr.io/kamu-data
IMAGE_JUPYTER_TAG = 0.5.1

KAMU_VERSION = $(shell cargo metadata --format-version 1 | jq -r '.packages[] | select( .name == "kamu") | .version')


.PHONY: jupyter
jupyter:
	docker build \
		-t $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG) \
		jupyter/


.PHONY: jupyter-push
jupyter-push:
	docker push $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG)


#########################################################################################


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


#########################################################################################


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
