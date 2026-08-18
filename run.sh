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
IMAGE_TAG="$(git -C "${SCRIPT_DIR}" describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null | sed 's/^v//')" || true
[[ -n "${IMAGE_TAG}" ]] || IMAGE_TAG="latest"

CONTAINER_NAME="tw_stock_db_operating"
LOG_DIR="${SCRIPT_DIR}/logs"
# 持久化設定目錄：與 logs/ 分離，並與 CI deploy 掛載「同一個」host 絕對路徑，
# 讓手動啟動與自動部署共用同一份設定（詳見 README「設定持久化」）。
CONFIG_DIR="${SCRIPT_DIR}/config"

# 確保 logs 與 config 資料夾存在
mkdir -p "${LOG_DIR}" "${CONFIG_DIR}"

# 相容遷移：舊版設定檔寄生在 logs/ 內，若新位置尚無設定就搬過去（原樣保留內容），
# 避免既有排程自訂在改用獨立設定目錄後被丟棄。容器內另有同語意的遷移邏輯，
# 此處先於 host 處理，讓 host 上的舊設定也能被具名 volume 部署沿用。
if [[ ! -f "${CONFIG_DIR}/config.json" && -f "${LOG_DIR}/config.json" ]]; then
  echo "偵測到舊設定檔 logs/config.json，搬遷至 config/config.json"
  mv "${LOG_DIR}/config.json" "${CONFIG_DIR}/config.json"
fi

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
  -v "${CONFIG_DIR}:/workspace/config" \
  -v "${NEWS_CONTENTS_DIR}:/workspace/NewsContents" \
  "${IMAGE_NAME}:${IMAGE_TAG}"
