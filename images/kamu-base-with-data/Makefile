# Inputs:
# - IMAGE_REPO
# - KAMU_VERSION
EXAMPLES_DIR=../../examples


image:
	docker build \
		--build-arg KAMU_VERSION=$(KAMU_VERSION) \
		-t $(IMAGE_REPO)/kamu-base:latest-with-data \
		.


image-push:
	docker push $(IMAGE_REPO)/kamu-base:latest-with-data


workspace: sync-datasets
	docker run \
		--rm \
		--privileged \
		-v `pwd`/datasets:/tmp/datasets \
		$(IMAGE_REPO)/kamu-base:$(KAMU_VERSION) \
		bash -c "cd /tmp/datasets && ./init.sh"


sync-datasets:
	rsync -avu --delete --exclude=".*/" "$(EXAMPLES_DIR)/" datasets/examples
