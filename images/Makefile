IMAGE_REPO = kamu
IMAGE_SPARK_BASE_TAG = 2.4.0
IMAGE_SPARK_TAG = 2.4.0_0.0.1

.PHONY: image-spark-base
image-spark-base:
	# Build base images from local spark installation
	cd $(SPARK_HOME) && ./bin/docker-image-tool.sh \
		-r $(IMAGE_REPO) \
		-t $(IMAGE_SPARK_BASE_TAG) \
		build

.PHONY: image-spark
image-spark:
	# Build extended image
	docker build \
		-t $(IMAGE_REPO)/spark:$(IMAGE_SPARK_TAG) \
		-f Dockerfile.spark \
		.
