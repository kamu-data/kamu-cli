BASE_IMAGE:=docker.io/jupyter/base-notebook:2022-11-15

requirements:
	docker run \
		--rm \
		-u root \
		-v `pwd`:/opt/mnt \
		--workdir /opt/mnt \
		$(BASE_IMAGE) \
		bash -c "conda install -y requests-kerberos make && make do-requirements"

do-requirements:
	pip --no-cache-dir install --upgrade pip setuptools
	pip --no-cache-dir install -r requirements.in
	pip --no-cache-dir list --format=freeze > requirements.txt
