#!/bin/bash
#
# 建立 Tw_stock_DB_Operating Docker image

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "${SCRIPT_DIR}")"

IMAGE_NAME="nk7260ynpa/tw_stock_db_operating"

# 版本號以 git tag（vX.Y.Z）為「單一版本真實來源」，與 CI deploy 同源。
# 取最近的版本 tag、去掉開頭 v 作為 image 標籤；取不到時退回 latest。
IMAGE_TAG="$(git -C "${PROJECT_DIR}" describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null | sed 's/^v//')" || true
[[ -n "${IMAGE_TAG}" ]] || IMAGE_TAG="latest"

# 同時打版本化 tag 與 latest，與 .gitlab-ci.yml 的 deploy job build 行為一致，
# 讓 run.sh 的 latest fallback 與 CI 部署都能取到同一份 image。
echo "開始建立 Docker image: ${IMAGE_NAME}:${IMAGE_TAG}（同時打 :latest）"
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" -t "${IMAGE_NAME}:latest" \
  -f "${SCRIPT_DIR}/Dockerfile" "${PROJECT_DIR}"
echo "Docker image 建立完成: ${IMAGE_NAME}:${IMAGE_TAG} 與 ${IMAGE_NAME}:latest"
