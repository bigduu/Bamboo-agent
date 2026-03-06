# Bamboo Backend Docker 部署

这个目录提供 Bamboo 后端（`bamboo` 二进制）的 Docker 镜像与 `docker-compose` 示例。

## 快速开始

在 `bamboo/` 目录下：

```bash
cd docker
docker compose up -d --build
```

健康检查：

```bash
curl http://localhost:9562/api/v1/health
```

## 配置 (config.json)

服务会从数据目录读取配置文件：`$BAMBOO_DATA_DIR/config.json`（容器里默认是 `/data/config.json`）。

你可以把 `config.example.json` 复制成 `config.json`，然后在 `docker-compose.yml` 里把它挂载进去：

```yaml
volumes:
  - ./config.json:/data/config.json:ro
```

## 常用环境变量

- `BAMBOO_DATA_DIR`：数据目录（默认 `/data`）
- `BAMBOO_PORT`：监听端口（默认 `9562`）
- `BAMBOO_BIND`：监听地址（Docker 一般用 `0.0.0.0`）
- `BAMBOO_WORKERS`：Actix worker 数（默认 `10`）
- `BAMBOO_CORS_ALLOW_ORIGINS`：CORS allowlist（逗号分隔，支持 `https://...`、host、`*.example.com`）

