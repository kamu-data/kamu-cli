IMAGE_REPO = docker.io/kamudata
IMAGE_JUPYTER_TAG = 0.1.0

KAMU_VERSION = 0.78.2


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

#########################################################################################


.PHONY: kamu-base-with-data
kamu-base-with-data:
	# Make sure to run first: cargo run --bin create-test-workspace
	cp -r ../examples/covid kamu-base-with-data/workspace
	rm -rf kamu-base-with-data/workspace/.kamu.local/cache
	
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:latest-with-data \
		kamu-base-with-data/
	
	rm -rf kamu-base-with-data/workspace


.PHONY: kamu-base-with-data-push
kamu-base-with-data-push:
	docker push $(IMAGE_REPO)/kamu-base:latest-with-data
