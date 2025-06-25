#!/bin/bash

set -xe

source ~/.oxla/compile

docker build -t ${SQLANCER_IMAGE_TAG} -f oxla.Dockerfile --pull --progress=plain .

docker push ${SQLANCER_IMAGE_TAG}

echo -e "Image built and pushed to Harbor. Tag: ${SQLANCER_IMAGE_TAG}"
