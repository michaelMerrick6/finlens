"""Bounded POSIX audit workers and atomic progress snapshots."""
import json
import multiprocessing
import os
from pathlib import Path
import signal
import tempfile


def _worker(check, output):
    os.setsid()
    try:
        result = {'status': 'completed', 'result': check()}
    except Exception as exc:
        result = {'status': 'error', 'error': f'{type(exc).__name__}: {exc}'}
    Path(output).write_text(json.dumps(result))


def run_document(check, timeout_seconds):
    if timeout_seconds <= 0:
        raise ValueError('Document timeout must be positive')
    if os.name != 'posix':
        raise RuntimeError('Audit process isolation requires macOS or Linux')
    # Fork is deliberate: CLI audit callbacks capture parser reference data.
    # No background threads are started by this audit runner.
    with tempfile.TemporaryDirectory(prefix='vail-audit-') as directory:
        output = str(Path(directory) / 'result.json')
        process = multiprocessing.get_context('fork').Process(target=_worker, args=(check, output))
        process.start()
        try:
            process.join(timeout_seconds)
            if process.is_alive():
                return {'status': 'timeout', 'timeout_seconds': timeout_seconds}
            if process.exitcode != 0 or not Path(output).exists():
                return {'status': 'error', 'error': f'Worker exited without a result (exit {process.exitcode})'}
            return json.loads(Path(output).read_text())
        finally:
            # Kill the worker's OCR descendants too, including after success.
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            if process.is_alive():
                process.kill()
            process.join()
            process.close()


def save_progress(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(report, stream, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
