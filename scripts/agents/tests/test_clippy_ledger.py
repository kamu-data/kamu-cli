import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts.agents import clippy_ledger


class StopReminderTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        patcher = mock.patch.object(clippy_ledger, "STATE", Path(self.dir.name) / "clippy.json")
        patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(self.dir.cleanup)
        self.fp = "fp-1"
        fp_patch = mock.patch.object(clippy_ledger, "fingerprint", lambda: self.fp)
        fp_patch.start()
        self.addCleanup(fp_patch.stop)

    def test_sessions_that_did_not_edit_rust_are_left_alone(self):
        self.assertIsNone(clippy_ledger.stop_reminder("s"))

    def test_reminds_once_per_rust_state(self):
        clippy_ledger.note_rust_edit("s")
        self.assertIsNotNone(clippy_ledger.stop_reminder("s"))
        self.assertIsNone(clippy_ledger.stop_reminder("s"))
        self.fp = "fp-2"
        self.assertIsNotNone(clippy_ledger.stop_reminder("s"))

    def test_green_clippy_on_the_current_state_silences_it(self):
        clippy_ledger.note_rust_edit("s")
        clippy_ledger.record_green()
        self.assertIsNone(clippy_ledger.stop_reminder("s"))

    def test_clean_tree_needs_no_reminder(self):
        clippy_ledger.note_rust_edit("s")
        self.fp = None
        self.assertIsNone(clippy_ledger.stop_reminder("s"))

    def test_only_a_bare_make_clippy_counts_as_green(self):
        self.assertTrue(clippy_ledger.is_whole_clippy_run("make clippy"))
        self.assertTrue(clippy_ledger.is_whole_clippy_run("make lint"))
        self.assertFalse(clippy_ledger.is_whole_clippy_run("make clippy | grep warning"))
        self.assertFalse(clippy_ledger.is_whole_clippy_run("make clippy || true"))
        self.assertFalse(clippy_ledger.is_whole_clippy_run("cargo clippy"))
        for command in ["make -n clippy", "make --dry-run clippy", "make -q clippy",
                        "make -t clippy", "make -f /tmp/other clippy", "make -C /tmp clippy",
                        "make clippy ||", "make clippy &"]:
            with self.subTest(command=command):
                self.assertFalse(clippy_ledger.is_whole_clippy_run(command))


if __name__ == "__main__":
    unittest.main()
