IMAGE_REPO = docker.io/kamudata
KAMU_VERSION = 0.106.0
DEMO_VERSION = 0.6.1

#########################################################################################

.PHONY: all-examples
all-examples:
	rsync -avu --delete --exclude=".*/" "../../examples/" "user-home/XX - Other Examples"

.PHONY: jupyter
jupyter: all-examples
	docker build \
		-f Dockerfile.jupyter \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		.

.PHONY: jupyter-push
jupyter-push:
	docker push $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION)

#########################################################################################

.PHONY: minio
minio:
	docker build \
		-f Dockerfile.minio \
		-t $(IMAGE_REPO)/kamu-cli-demo-minio:$(DEMO_VERSION) \
		.

.PHONY: minio-push
minio-push:
	docker push $(IMAGE_REPO)/kamu-cli-demo-minio:$(DEMO_VERSION)

#########################################################################################

.PHONY: minio-data
minio-data:
	rm -rf minio-data && \
		mkdir -p minio-data/kamu-hub && \
		mkdir minio-data/tmp && \
		cd minio-data/tmp && \
		kamu init && \
		kamu repo add kamu-hub file:///$(shell pwd)/minio-data/kamu-hub && \
		kamu repo list && \
		kamu add -r "../../user-home/01 - Kamu Basics (COVID-19 example)/datasets/" && \
		kamu pull --all && \
		kamu push british-columbia.case-details --to kamu-hub/covid19.british-columbia.case-details && \
		kamu push canada.case-details --to kamu-hub/covid19.canada.case-details && \
		kamu push canada.daily-cases --to kamu-hub/covid19.canada.daily-cases && \
		kamu push ontario.case-details --to kamu-hub/covid19.ontario.case-details && \
		kamu search && \
		cd .. && \
		rm -rf tmp

#########################################################################################

.PHONY: strip-notebooks
strip-notebooks:
	find . -name '.ipynb_checkpoints' -type d -prune -exec rm -rf {} \;
	find . -name '*.ipynb' -type f -exec nbstripout {} \;

#########################################################################################

podman-images: podman-images/
podman-images/:
	docker run --rm --entrypoint bash $(IMAGE_REPO)/kamu-cli-demo-jupyter:$(DEMO_VERSION) \
		-c "kamu init --pull-images --list-only | grep -e spark -e flink" | \
		xargs -n 1 -- podman --root podman-images pull

	# For Some reason podman creates files owned by a different users, so we need sudo to chmod them :(
	sudo chmod -R a+rx podman-images