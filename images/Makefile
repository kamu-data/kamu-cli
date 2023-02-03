IMAGE_REPO = docker.io/kamudata
IMAGE_JUPYTER_TAG = 0.5.0

KAMU_VERSION = 0.108.0


jupyter:
	docker build \
		-t $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG) \
		jupyter/


jupyter-push:
	docker push $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG)


#########################################################################################


kamu-base:
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION) \
		kamu-base/


kamu-base-push:
	docker push $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION)
	docker tag $(IMAGE_REPO)/kamu-base:$(KAMU_VERSION) $(IMAGE_REPO)/kamu-base:latest
	docker push $(IMAGE_REPO)/kamu-base:latest


#########################################################################################


kamu-base-with-data:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		workspace image


kamu-base-with-data-push:
	make -C kamu-base-with-data/ \
		IMAGE_REPO=$(IMAGE_REPO) \
		KAMU_VERSION=$(KAMU_VERSION) \
		image-push


#########################################################################################


kamu-dev:
	cd .. \
		&& cross build --target x86_64-unknown-linux-gnu --release \
		&& cp target/x86_64-unknown-linux-gnu/release/kamu-cli images/dev/kamu

	# Make sure to run first: cargo run --bin create-test-workspace
	cp -r ../examples/covid dev/workspace

	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:dev \
		dev/

	rm -rf dev/kamu dev/workspace


kamu-dev-push:
	docker push $(IMAGE_REPO)/kamu-base:dev