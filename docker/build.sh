#!/bin/bash
#
# 建立 Tw_stock_DB_Operating Docker image

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

IMAGE_NAME="tw_stock_db_operating"
IMAGE_TAG="1.0.0"

echo "開始建立 Docker image: ${IMAGE_NAME}:${IMAGE_TAG}"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_DIR}"
echo "Docker image 建立完成: ${IMAGE_NAME}:${IMAGE_TAG}"
