import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest import mock

from scripts.agents import repeated_lines


def diff_adding(path: str, *lines: str) -> str:
    return f"--- a/{path}\n+++ b/{path}\n@@ -1 +1 @@\n" + "".join(f"+    {line}\n" for line in lines)


def no_base(texts):
    return Counter()


class RepeatsTest(unittest.TestCase):
    def test_a_field_added_to_every_copy_is_reported_with_its_files(self):
        diff = diff_adding("src/a.rs", "owner: None,", "owner: None,") + diff_adding("src/b.rs", "owner: None,")
        [found] = repeated_lines.repeats(diff, no_base)
        self.assertEqual(found.text, "owner: None,")
        self.assertEqual(found.paths, ("src/a.rs", "src/a.rs", "src/b.rs"))

    def test_fewer_copies_than_the_threshold_are_not_reported(self):
        diff = diff_adding("src/a.rs", "owner: None,", "owner: None,")
        self.assertEqual(repeated_lines.repeats(diff, no_base), [])

    def test_lines_that_repeat_in_any_change_are_skipped(self):
        for line in ["}).await.unwrap();", ".await?;", "use std::sync::Arc;", "#[test_log::test(tokio::test)]",
                     "// A comment repeated", "assert_eq!(", "principal: &PrincipalHandle,",
                     "owner_id: Option<ResourceID>,", "storage = postgres,", "return Ok(());",
                     "let harness = OwnerReferencesHarness::new();", "Ok(())", "ResourceRef {"]:
            with self.subTest(line=line):
                diff = diff_adding("src/a.rs", line, line, line)
                self.assertEqual(repeated_lines.repeats(diff, no_base), [])

    def test_idioms_of_the_base_tree_are_skipped(self):
        diff = diff_adding("src/a.rs", *["let tr = self.transaction.lock().await;"] * 3)
        common = lambda texts: Counter({t: 50 for t in texts})
        self.assertEqual(repeated_lines.repeats(diff, common), [])
        rare = lambda texts: Counter({t: 2 for t in texts})
        self.assertEqual(len(repeated_lines.repeats(diff, rare)), 1)

    def test_only_rust_files_count(self):
        diff = diff_adding("docs/a.md", *["owner: None, in prose"] * 3)
        self.assertEqual(repeated_lines.repeats(diff, no_base), [])

    def test_most_copies_come_first(self):
        diff = diff_adding("src/a.rs", *["a_field: Value::One,"] * 3, *["b_field: Value::Two,"] * 4)
        self.assertEqual([r.text for r in repeated_lines.repeats(diff, no_base)],
                         ["b_field: Value::Two,", "a_field: Value::One,"])


class StopReminderTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        root = Path(self.dir.name)
        for target, value in [("STATE", root / "repeated.json"), ("CLIPPY_STATE", root / "clippy.json")]:
            patcher = mock.patch.object(repeated_lines, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.diff = diff_adding("src/a.rs", *["owner: None,"] * 3)
        for target, value in [("working_tree_diff", lambda base: self.diff),
                              ("git_base_counts", lambda rev: no_base)]:
            patcher = mock.patch.object(repeated_lines, target, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def edit_rust(self, session_id):
        with repeated_lines.locked_state(repeated_lines.CLIPPY_STATE) as state:
            state.setdefault("edited_sessions", {})[session_id] = 1.0

    def test_sessions_that_did_not_edit_rust_are_left_alone(self):
        self.assertIsNone(repeated_lines.stop_reminder("s"))

    def test_reminds_once_per_newly_repeated_line(self):
        self.edit_rust("s")
        self.assertIn("owner: None,", repeated_lines.stop_reminder("s"))
        self.assertIsNone(repeated_lines.stop_reminder("s"))
        self.diff += diff_adding("src/b.rs", *["visibility: ResourceVisibility::Active,"] * 3)
        reminder = repeated_lines.stop_reminder("s")
        self.assertIn("visibility: ResourceVisibility::Active,", reminder)
        self.assertNotIn("owner: None,", reminder)


if __name__ == "__main__":
    unittest.main()
