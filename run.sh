#!/bin/bash
#
# 啟動 Docker container 並執行主程式

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="tw_stock_db_operating"
IMAGE_TAG="1.0.0"
CONTAINER_NAME="tw_stock_db_operating"
LOG_DIR="${SCRIPT_DIR}/logs"

# 確保 logs 資料夾存在
mkdir -p "${LOG_DIR}"

# 檢查 image 是否存在
if ! docker image inspect "${IMAGE_NAME}:${IMAGE_TAG}" > /dev/null 2>&1; then
  echo "Docker image 不存在，請先執行 docker/build.sh 建立 image。"
  exit 1
fi

echo "啟動 Docker container: ${CONTAINER_NAME}"
docker run --rm \
  --name "${CONTAINER_NAME}" \
  --network db_network \
  -v "${LOG_DIR}:/workspace/logs" \
  "${IMAGE_NAME}:${IMAGE_TAG}" \
  "$@"
