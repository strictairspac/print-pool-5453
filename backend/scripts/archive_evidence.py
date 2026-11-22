"""手动执行一轮已签署证据归档。

用法：docker compose exec -T backend python -m scripts.archive_evidence [--limit N] [--min-bytes N]
"""
import argparse

from src.config.settings import ARCHIVE_BATCH, ARCHIVE_MIN_BYTES
from src.services.archive import archive_signed

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='将已签署证据的大件正文迁入归档存储，核验通过后清掉库中正文')
    parser.add_argument('--limit', type=int, default=ARCHIVE_BATCH, help='本轮最多归档的证据条数')
    parser.add_argument('--min-bytes', type=int, default=ARCHIVE_MIN_BYTES, help='小于该字节的正文保留在库中')
    args = parser.parse_args()
    print(archive_signed(limit=args.limit, min_bytes=args.min_bytes))
