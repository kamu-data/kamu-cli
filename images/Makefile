IMAGE_REPO = kamu
IMAGE_SPARK_BASE_TAG = 2.4.0
IMAGE_SPARK_TAG = 2.4.0_0.0.1

.PHONY: base
base:
	cd $(SPARK_HOME) && ./bin/docker-image-tool.sh \
		-r $(IMAGE_REPO) \
		-t $(IMAGE_SPARK_BASE_TAG) \
		build

.PHONY: spark
spark:
	docker build \
		-t $(IMAGE_REPO)/spark:$(IMAGE_SPARK_TAG) \
		-f Dockerfile.spark \
		.

.PHONY: spark-py
spark-py:
	docker build \
		-t $(IMAGE_REPO)/spark-py:$(IMAGE_SPARK_TAG) \
		-f Dockerfile.spark-py \
		.
