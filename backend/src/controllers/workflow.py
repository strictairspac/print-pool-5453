from starlette.responses import Response

from src.config.settings import engine
from src.models.tables import evidence
from src.services.archive import load_report
from src.services.workflow import found
from src.utils.encoding import canonical

# 导出文档固定为这七个字段：归档迁移前后，旧链接下载到的字节完全一致，
# 签署信息与校验码保持不变。
EXPORT_FIELDS = ('id', 'run_id', 'report', 'digest', 'reviewer', 'review_note', 'reviewed_at')


def download(identity):
    with engine.connect() as conn:
        row = found(conn, evidence, identity)
    document = {field: row[field] for field in EXPORT_FIELDS if field != 'report'}
    document['report'] = load_report(row)
    return Response(canonical(document), media_type='application/json', headers={
        'Content-Disposition': f'attachment; filename="evidence-{identity}.json"'})
