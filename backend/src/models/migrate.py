"""轻量启动迁移：为已有部署补齐证据归档所需的列。

仅处理本次新增的列；更大范围的结构变更仍需版本化迁移（见 migrations/README.md）。
"""
from sqlalchemy import inspect, text

from src.config.settings import IS_MYSQL


def ensure_schema(engine):
    inspector = inspect(engine)
    if 'business_evidence' not in inspector.get_table_names():
        return
    columns = {column['name']: column for column in inspector.get_columns('business_evidence')}
    statements = []
    if 'archive_key' not in columns:
        statements.append('ALTER TABLE business_evidence ADD COLUMN archive_key VARCHAR(128)')
    if 'archived_at' not in columns:
        statements.append('ALTER TABLE business_evidence ADD COLUMN archived_at VARCHAR(32)')
    if columns.get('report') and not columns['report']['nullable']:
        if IS_MYSQL:
            statements.append('ALTER TABLE business_evidence MODIFY COLUMN report JSON NULL')
        elif engine.dialect.name == 'postgresql':
            statements.append('ALTER TABLE business_evidence ALTER COLUMN report DROP NOT NULL')
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
