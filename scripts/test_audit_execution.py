import json
import os
from pathlib import Path
import tempfile
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

from audit_execution import run_document, save_progress
from parser_write_policy import read_only_parser_scope, parser_writes_allowed


@unittest.skipUnless(os.name == 'posix', 'Audit workers require POSIX')
class AuditExecutionTests(unittest.TestCase):
    def test_timeout_allows_next_document_and_inherits_read_only_policy(self):
        with read_only_parser_scope():
            timed_out = run_document(lambda: time.sleep(10), .1)
            next_result = run_document(lambda: {'writes_allowed': parser_writes_allowed()}, 2)
        self.assertEqual(timed_out['status'], 'timeout')
        self.assertEqual(next_result['result'], {'writes_allowed': False})

    def test_worker_error_is_a_result_not_an_aborted_audit(self):
        def broken():
            raise ValueError('Invalid document')
        result = run_document(broken, 2)
        self.assertEqual(result['status'], 'error')
        self.assertIn('Invalid document', result['error'])
        self.assertEqual(run_document(lambda: 42, 2)['result'], 42)

    def test_large_result_does_not_block_on_pipe_buffers(self):
        self.assertEqual(len(run_document(lambda: 'x' * 1_000_000, 2)['result']), 1_000_000)

    def test_abrupt_worker_exit_is_reported(self):
        result = run_document(lambda: os._exit(3), 2)
        self.assertEqual(result['status'], 'error')
        self.assertIn('exit 3', result['error'])

    def test_checkpoint_is_valid_and_failed_replace_keeps_previous_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'progress.json'
            save_progress(path, {'documents': [1]})
            with patch('audit_execution.os.replace', side_effect=OSError('Disk failure')):
                with self.assertRaises(OSError):
                    save_progress(path, {'documents': [1, 2]})
            self.assertEqual(json.loads(path.read_text()), {'documents': [1]})
            self.assertEqual(list(Path(directory).iterdir()), [path])

    def test_timeout_kills_ocr_style_descendant(self):
        with tempfile.TemporaryDirectory() as directory:
            marker = Path(directory) / 'descendant-survived'
            def check():
                child = subprocess.Popen([sys.executable, '-c',
                                  'import time, pathlib, sys; time.sleep(.4); pathlib.Path(sys.argv[1]).touch()', str(marker)])
                child.wait()
            self.assertEqual(run_document(check, .1)['status'], 'timeout')
            time.sleep(.5)
            self.assertFalse(marker.exists())

    def test_invalid_timeout_is_rejected(self):
        with self.assertRaises(ValueError):
            run_document(lambda: None, 0)


if __name__ == '__main__':
    unittest.main()
