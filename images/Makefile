IMAGE_REPO = docker.io/kamudata
IMAGE_JUPYTER_TAG = 0.1.0


.PHONY: jupyter
jupyter:
	docker build \
		-t $(IMAGE_REPO)/jupyter:$(IMAGE_JUPYTER_TAG) \
		jupyter/


.PHONY: jupyter-push
jupyter-push:
