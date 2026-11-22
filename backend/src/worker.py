import logging
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

from src.config.settings import (ARCHIVE_BATCH, ARCHIVE_MIN_BYTES,
                                 ARCHIVE_SWEEP_SECONDS, ARTIFACTS, cache, engine)
from src.services.archive import archive_signed
from src.services.domain import solve
from src.services.workflow import claim, finish

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def work():
    HEARTBEAT.unlink(missing_ok=True)
    last_sweep = 0.0
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            if not job:
                now = monotonic()
                if now - last_sweep >= ARCHIVE_SWEEP_SECONDS:
                    last_sweep = now
                    stats = archive_signed(limit=ARCHIVE_BATCH, min_bytes=ARCHIVE_MIN_BYTES)
                    if stats['archived'] or stats['failed']:
                        logging.info('证据归档扫描：%s', stats)
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            thread = Thread(target=heartbeat, args=(stop,), daemon=True)
            thread.start()
            try:
                result = solve(payload, engine)
                finish(run_id, token, result=result)
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
            finally:
                stop.set()
                thread.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
