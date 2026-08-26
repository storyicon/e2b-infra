ENV := $(shell cat ../../.last_used_env || echo "not-set")
-include ../../.env.${ENV}

AWS_BUCKET_PREFIX ?= $(PREFIX)$(AWS_ACCOUNT_ID)-
BUILD_ARCH ?= amd64

.PHONY: build
build:
	mkdir -p bin
	CGO_ENABLED=0 GOOS=linux GOARCH=$(BUILD_ARCH) go build -trimpath -o bin/capacity-controller .

.PHONY: upload
upload:
	chmod +x ./bin/capacity-controller
	aws s3 cp ./bin/capacity-controller "s3://${AWS_BUCKET_PREFIX}fc-env-pipeline/capacity-controller" --profile ${AWS_PROFILE} --cache-control "no-cache, max-age=0"

.PHONY: build-and-upload
build-and-upload: build upload

.PHONY: test
test:
	go test -race ./...
