# 初始结构

`src/models/tables.py` 定义初始关系结构。API 启动时显式创建演示库的表；此版本只包含初始 schema。需要在已有部署升级表结构时，应单独引入版本化迁移。

# 证据归档列

本次为证据归档在 `business_evidence` 上新增 `archive_key`、`archived_at`，并把 `report` 改为可空。`src/models/migrate.py` 在 API 启动与演示数据脚本运行时幂等补齐这些列（已有数据不受影响）；后续其他结构变更仍需引入版本化迁移。
