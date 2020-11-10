start_dev:
	$(eval export FOLDERNAME=$(shell basename "${PWD}"))
	@-docker stop $(FOLDERNAME)_dev > /dev/null 2>&1 ||:
	@-docker container prune --force > /dev/null

	@-docker build -f Dockerfile . \
		 -t $(FOLDERNAME)_dev:latest
	@-docker run \
		-p 9996:8888 \
		--env-file .env \
		--rm \
		-v $(PWD)/src/:/app/ \
		--name $(FOLDERNAME)_dev \
		--cpus=1 \
		-d \
		$(FOLDERNAME)_dev:latest > /dev/null
	@echo "Container started"
	@echo "Jupyter is running at http://localhost:9996/?token=dst"
