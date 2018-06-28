.PHONY: build

IMAGE_NAME = lukaszimmermann/pht-station-simple:test

build:
	docker build --rm --pull --no-cache -t $(IMAGE_NAME) .

