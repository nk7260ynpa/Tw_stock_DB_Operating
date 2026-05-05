# Stage 1: 建構 React 前端
FROM node:20-alpine AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend/ .
RUN npm run build

# Stage 2: Python 執行環境
FROM python:3.12.7

ENV LANG=en_US.UTF-8
ENV TZ=Asia/Taipei
# Dashboard 反向代理路徑（供 FastAPI 產生正確的 OpenAPI/redirect 路徑）
ENV ROOT_PATH=/app/db-operating
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

WORKDIR /workspace

# 先安裝 Playwright 及瀏覽器（獨立快取層，不受依賴變動影響）
RUN pip install --no-cache-dir playwright \
    && playwright install chromium --with-deps

# 安裝 Python 套件（利用 Docker cache，先複製依賴定義檔）
COPY requirements.txt pyproject.toml /workspace/
RUN pip install --no-cache-dir -r requirements.txt

COPY . /workspace/

# 以套件模式安裝專案（註冊模組至 site-packages）
RUN pip install --no-cache-dir --no-deps .

# 複製前端建構產出
COPY --from=frontend-builder /app/dist /workspace/static

EXPOSE 8080

CMD ["python", "web_server.py"]
