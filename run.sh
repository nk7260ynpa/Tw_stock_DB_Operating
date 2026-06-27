#!/bin/bash
#
# 啟動 Docker container 並執行主程式

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="nk7260ynpa/tw_stock_db_operating"

# 版本號以 git tag（vX.Y.Z）為「單一版本真實來源」，與 CI deploy 同源
# （CI 用 ${CI_COMMIT_TAG#v}）。此處取最近的版本 tag、去掉開頭 v 作為 image 標籤，
# 確保手動 ./run.sh 與 CI 部署同一版本、不再各自硬編而分岔。
# 取不到（無 git 或尚無 tag）時退回 latest，與 CI build 一併打的 :latest 對齊。
IMAGE_TAG="$(git -C "${SCRIPT_DIR}" describe --tags --abbrev=0 2>/dev/null | sed 's/^v//')" || true
[[ -n "${IMAGE_TAG}" ]] || IMAGE_TAG="latest"

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

# NewsContents 目錄用於存放 CTEE 新聞全文
NEWS_CONTENTS_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)/Tw_stock_DB/NewsContents"
mkdir -p "${NEWS_CONTENTS_DIR}"

docker run -d \
  --name "${CONTAINER_NAME}" \
  --network db_network \
  --restart always \
  -v "${LOG_DIR}:/workspace/logs" \
  -v "${NEWS_CONTENTS_DIR}:/workspace/NewsContents" \
  "${IMAGE_NAME}:${IMAGE_TAG}"
