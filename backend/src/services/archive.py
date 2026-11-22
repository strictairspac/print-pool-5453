"""已签署证据正文的归档存储。

归档对象按内容寻址（键即报告校验码），具备以下性质：

- 先核验后清理：归档副本落盘并回读校验通过后，才在一个事务里清掉库中正文；
  任何一步失败，库中正文保持不动，不会误删唯一副本。
- 重试幂等：同一校验码始终写入同一对象键，不重复占用空间；
  已核验的副本绝不覆盖，损坏的副本通过临时文件原子替换修复。
- 下载可核验：读取归档副本时重新计算 SHA-256 并与库中校验码比对，
  缺失或损坏时抛出带原因的异常，由下载接口转为明确的错误响应。
"""
import hashlib
import json
import logging
import os
import re
import tempfile
import time
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select

from src.config.settings import (ARCHIVE_BATCH, ARCHIVE_DIR, ARCHIVE_MIN_BYTES,
                                 engine)
from src.models.tables import audit, evidence
from src.utils.encoding import canonical
from src.utils.time import utc_time

KEY_PATTERN = re.compile(r'[0-9a-f]{2}/[0-9a-f]{64}\.json')


class ArchiveError(Exception):
    """归档存储读写异常。"""


class ArchiveMissing(ArchiveError):
    """归档副本缺失。"""


class ArchiveCorrupt(ArchiveError):
    """归档副本与校验码不符。"""


class ArchiveStore:
    def __init__(self, root=ARCHIVE_DIR):
        self.root = Path(root)

    def key_for(self, digest):
        return f'{digest[:2]}/{digest}.json'

    def get(self, key, digest):
        """读取归档副本并按校验码核验，核验通过才返回内容。"""
        if not KEY_PATTERN.fullmatch(key):
            raise ArchiveCorrupt(f'归档键非法：{key}')
        try:
            data = (self.root / key).read_bytes()
        except FileNotFoundError:
            raise ArchiveMissing(f'归档副本缺失：{key}') from None
        except OSError as exc:
            raise ArchiveError(f'归档存储读取失败：{exc}') from exc
        if hashlib.sha256(data).hexdigest() != digest:
            raise ArchiveCorrupt(f'归档副本校验失败：{key}')
        return data

    def put(self, digest, data):
        """写入归档副本并回读核验。已核验副本存在时不覆盖。返回 (键, 是否新写入)。"""
        key = self.key_for(digest)
        path = self.root / key
        if path.exists():
            try:
                self.get(key, digest)
                return key, False
            except ArchiveCorrupt:
                pass  # 损坏副本不属于已核验副本，原子替换修复
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=path.parent, prefix='.upload-')
        try:
            with os.fdopen(fd, 'wb') as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp, path)
        finally:
            Path(temp).unlink(missing_ok=True)
        self.get(key, digest)  # 回读核对归档副本与原报告一致
        return key, True

    def prune_stale_uploads(self, max_age_seconds=3600):
        """清理崩溃残留的临时文件，避免重试之外的额外空间占用。"""
        cutoff = time.time() - max_age_seconds
        for path in self.root.glob('**/.upload-*'):
            try:
                if path.stat().st_mtime < cutoff:
                    path.unlink()
            except OSError:
                pass


def load_report(row):
    """返回证据行的报告正文；正文已归档时从归档存储读取并核验。"""
    if row['report'] is not None:
        return row['report']
    if not row.get('archive_key'):
        raise HTTPException(500, '证据正文缺失：库中无正文且未登记归档副本')
    try:
        data = ArchiveStore().get(row['archive_key'], row['digest'])
    except ArchiveMissing as exc:
        raise HTTPException(502, f'归档副本缺失（{row["archive_key"]}），请从备份恢复归档存储后重试') from exc
    except ArchiveCorrupt as exc:
        raise HTTPException(502, f'归档副本校验失败（{row["archive_key"]}）：内容与证据校验码不符，副本已损坏') from exc
    except ArchiveError as exc:
        raise HTTPException(502, f'归档存储暂不可用：{exc}') from exc
    return json.loads(data)


def archive_signed(limit=ARCHIVE_BATCH, min_bytes=ARCHIVE_MIN_BYTES):
    """把已签署证据的大件正文迁入归档存储，核验通过后清掉库中正文。"""
    store = ArchiveStore()
    store.prune_stale_uploads()
    stats = {'candidates': 0, 'archived': 0, 'skipped_small': 0, 'failed': 0, 'freed_bytes': 0}
    with engine.connect() as conn:
        rows = [dict(row) for row in conn.execute(
            select(evidence.c.id, evidence.c.report, evidence.c.digest)
            .where(evidence.c.reviewed_at.is_not(None),
                   evidence.c.report.is_not(None),
                   evidence.c.archive_key.is_(None))
            .order_by(evidence.c.id)
            .limit(max(limit * 10, 50))).mappings()]
    stats['candidates'] = len(rows)
    for row in rows:
        if stats['archived'] >= limit:
            break
        try:
            body = canonical(row['report'])
            if len(body) < min_bytes:
                stats['skipped_small'] += 1
                continue
            if hashlib.sha256(body).hexdigest() != row['digest']:
                stats['failed'] += 1
                logging.error('证据 %s 正文与校验码不符，保留库中正文，跳过归档', row['id'])
                continue
            key, _ = store.put(row['digest'], body)
            with engine.begin() as conn:
                count = conn.execute(evidence.update()
                    .where(evidence.c.id == row['id'],
                           evidence.c.report.is_not(None),
                           evidence.c.archive_key.is_(None))
                    .values(report=None, archive_key=key, archived_at=utc_time())).rowcount
                if count != 1:
                    continue  # 并发下已被其他归档者处理
                conn.execute(audit.insert().values(actor='worker', action='archive',
                             reference=str(row['id']), created_at=utc_time()))
            stats['archived'] += 1
            stats['freed_bytes'] += len(body)
        except Exception:
            stats['failed'] += 1
            logging.exception('证据 %s 归档失败，库中正文保持不动', row['id'])
    return stats
