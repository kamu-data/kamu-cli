IMAGE_REPO = docker.io/kamudata
KAMU_VERSION = 0.69.0

#########################################################################################

.PHONY: jupyter
jupyter:
	docker build \
		-f Dockerfile.jupyter \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-cli-demo-jupyter:latest \
		.

.PHONY: jupyter-push
jupyter-push:
	docker push $(IMAGE_REPO)/kamu-cli-demo-jupyter:latest

#########################################################################################

.PHONY: minio
minio:
	docker build \
		-f Dockerfile.minio \
		-t $(IMAGE_REPO)/kamu-cli-demo-minio:latest \
		.

.PHONY: minio-push
minio-push:
	docker push $(IMAGE_REPO)/kamu-cli-demo-minio:latest

#########################################################################################

.PHONY: minio-data
minio-data:
	rm -rf minio-data && \
		mkdir -p minio-data/kamu-hub && \
		mkdir tmp && \
		cd tmp && \
		kamu init && \
		kamu repo add kamu-hub file:///$(shell pwd)/minio-data/kamu-hub && \
		kamu repo list && \
		kamu add ../user-home/demo/datasets/ca.ontario.data.covid19.case-details.yaml ../user-home/demo/datasets/ca.ontario.data.covid19.daily-cases.yaml && \
		kamu pull --all && \
		kamu push ca.ontario.data.covid19.case-details --as kamu-hub/ca.ontario.data.covid19.case-details && \
		kamu push ca.ontario.data.covid19.daily-cases --as kamu-hub/ca.ontario.data.covid19.daily-cases && \
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
	docker run --rm --entrypoint bash $(IMAGE_REPO)/kamu-cli-demo-jupyter:latest \
		-c "kamu init --pull-images --list-only | grep -e spark -e flink" | \
		xargs -n 1 -- podman --root podman-images pull

	# For Some reason podman creates files owned by a different users, so we need sudo to chmod them :(
	sudo chmod -R a+rx podman-images