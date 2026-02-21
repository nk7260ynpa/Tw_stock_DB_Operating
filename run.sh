#!/bin/bash
#
# 啟動 Docker container 並執行主程式

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="nk7260ynpa/tw_stock_db_operating"
IMAGE_TAG="2.1.0"
CONTAINER_NAME="tw_stock_db_operating"
LOG_DIR="${SCRIPT_DIR}/logs"

# 確保 logs 資料夾存在
mkdir -p "${LOG_DIR}"

# 檢查 image 是否存在
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" > /dev/null 2>&1; then
  echo "Docker image 不存在，請先執行 docker/build.sh 建立 image。"
  exit 1
fi

# 若容器已存在，先移除
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
  echo "移除既有容器: ${CONTAINER_NAME}"
  docker rm -f "${CONTAINER_NAME}" > /dev/null
fi

echo "啟動 Docker container: ${CONTAINER_NAME}"
docker run -d \
  --name "${CONTAINER_NAME}" \
  --network db_network \
  --restart always \
  -p 8080:8080 \
  -v "${LOG_DIR}:/workspace/logs" \
  "${IMAGE_NAME}:${IMAGE_TAG}"
