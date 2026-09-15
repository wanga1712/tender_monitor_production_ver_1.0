import json
import tempfile
import unittest
from pathlib import Path

from runtime_safety import atomic_write_text, cleanup_files


class RuntimeSafetyTests(unittest.TestCase):
    def test_failed_validation_preserves_old_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "state.json"
            target.write_text('{"old": true}', encoding="utf-8")
            with self.assertRaises(ValueError):
                atomic_write_text(target, "broken", validator=json.loads)
            self.assertEqual(target.read_text(encoding="utf-8"), '{"old": true}')
            self.assertEqual(list(Path(tmp).glob("*.tmp")), [])

    def test_cleanup_removes_created_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.xml"
            path.write_text("<root />", encoding="utf-8")
            cleanup_files([path])
            self.assertFalse(path.exists())

    def test_cleanup_runs_after_exception(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "failed.xml"
            path.write_text("<root />", encoding="utf-8")
            try:
                raise RuntimeError("simulated processing failure")
            except RuntimeError:
                cleanup_files([path])
            self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
