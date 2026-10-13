import json
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

from scripts.agents import bash_edits
from scripts.agents.bash_edits import changed_paths, moves_working_tree, save_pending, snapshot, take_pending
from scripts.agents.common import ROOT

CLIENTS = {
    "claude": (ROOT / ".claude" / "settings.json", "claude_pre_bash", "claude_post_bash"),
    "codex": (ROOT / ".codex" / "hooks.json", "codex_pre_bash", "codex_post_bash"),
}


class BashEditsTest(unittest.TestCase):
    def test_changed_paths_are_written_or_created_files(self):
        before = {"a.rs": [1, 10], "b.rs": [1, 10], "gone.rs": [1, 10]}
        after = {"a.rs": [1, 10], "b.rs": [2, 12], "new.rs": [3, 5]}
        self.assertEqual(changed_paths(before, after), ["b.rs", "new.rs"])

    def test_git_commands_that_rewrite_the_tree_are_recognised(self):
        for command in ["git switch master", "git -C sub pull", "cd x && git stash pop",
                        "FOO=1 git checkout main", "git rebase origin/master", "git reset --soft HEAD~1"]:
            with self.subTest(command=command):
                self.assertTrue(moves_working_tree(command))
        for command in ["git status", "git diff HEAD", "cargo fmt", "sed -i 's/a/b/' x.rs",
                        "echo 'git switch' > notes.txt"]:
            with self.subTest(command=command):
                self.assertFalse(moves_working_tree(command))

    def test_a_reading_is_collected_once_and_stale_ones_expire(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp)
            save_pending(state, "call-1", {"a.rs": [1, 2]})
            self.assertEqual(take_pending(state, "call-1"), {"a.rs": [1, 2]})
            self.assertIsNone(take_pending(state, "call-1"))

            save_pending(state, "abandoned", {})
            later = time.time() + bash_edits.PENDING_LIFETIME_SECONDS + 1
            with mock.patch.object(bash_edits, "now", return_value=later):
                save_pending(state, "call-2", {})
            self.assertIsNone(take_pending(state, "abandoned"))
            self.assertEqual(take_pending(state, "call-2"), {})

    def test_regenerated_files_are_not_judged(self):
        with mock.patch.object(bash_edits, "check_edit", return_value=([], [])) as check:
            bash_edits.check_changed(["resources/schema.gql", "src/app/cli/src/app.rs"], "s/main",
                                     Path(tempfile.mkdtemp()) / "skills.json", "{skill}")
        self.assertEqual([call.args[0] for call in check.call_args_list], ["src/app/cli/src/app.rs"])

    def test_snapshot_covers_tracked_and_untracked_files_but_not_ignored_ones(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
            (root / ".gitignore").write_text("target/\n")
            (root / "tracked.rs").write_text("a")
            subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True)
            (root / "untracked.rs").write_text("b")
            (root / "target").mkdir()
            (root / "target" / "built.rs").write_text("c")
            self.assertEqual(sorted(snapshot(root)), [".gitignore", "tracked.rs", "untracked.rs"])


class BashEditHookTest(unittest.TestCase):
    """Runs each client's real hook configuration against a scratch repository."""

    def setUp(self):
        self.harness = BashHookHarness()
        self.addCleanup(self.harness.close)

    @unittest.skipUnless(shutil.which("rustfmt"), "rustfmt not installed")
    def test_rust_written_through_the_shell_is_formatted_and_checked(self):
        for client in CLIENTS:
            with self.subTest(client=client):
                result = self.harness.shell_write(client, "printf ... > example.rs",
                                                  {"example.rs": self.harness.rust("fn main(){dbg!(1);}")})
                self.assertEqual(result.returncode, 2)
                self.assertIn("dbg!", result.stderr)
                self.assertIn("fn main() {", (self.harness.root / "example.rs").read_text())

    def test_a_command_that_changes_nothing_is_silent(self):
        for client in CLIENTS:
            with self.subTest(client=client):
                result = self.harness.shell_write(client, "ls", {})
                self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "", ""))

    def test_files_brought_in_by_git_are_not_judged(self):
        for client in CLIENTS:
            with self.subTest(client=client):
                result = self.harness.shell_write(client, "git switch other",
                                                  {"example.rs": self.harness.rust("fn main() { dbg!(1); }")})
                self.assertEqual((result.returncode, result.stderr), (0, ""))

    def test_a_guarded_path_written_without_its_skill_gets_a_reminder(self):
        for client in CLIENTS:
            with self.subTest(client=client):
                result = self.harness.shell_write(client, "cat > AGENTS.md", {"AGENTS.md": f"# {client}\n"})
                self.assertEqual(result.returncode, 0)
                context = json.loads(result.stdout)["hookSpecificOutput"]["additionalContext"]
                self.assertIn("kamu-prose-and-comments", context)

    def test_a_denied_command_takes_no_reading(self):
        for client in CLIENTS:
            with self.subTest(client=client):
                pre = self.harness.invoke(client, "PreToolUse", "git reset --hard")
                self.assertIn('"deny"', pre.stdout)
                (self.harness.root / "example.rs").write_text(self.harness.rust("fn main() { dbg!(1); }"))
                post = self.harness.invoke(client, "PostToolUse", "git reset --hard")
                self.assertEqual((post.returncode, post.stderr), (0, ""))


class BashHookHarness:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        shutil.copytree(ROOT / "scripts" / "agents", self.root / "scripts" / "agents",
                        ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(ROOT / ".claude" / "hooks", self.root / ".claude" / "hooks")
        (self.root / "docs").mkdir()
        shutil.copy(ROOT / "docs" / "license_header.txt", self.root / "docs" / "license_header.txt")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, capture_output=True)
        self.calls = 0

    def close(self):
        self.temp.cleanup()

    def rust(self, body):
        return f"{(self.root / 'docs' / 'license_header.txt').read_text()}\n{body}\n"

    def invoke(self, client, event, command):
        config, pre, post = CLIENTS[client]
        module = pre if event == "PreToolUse" else post
        hook = next(h for group in json.loads(config.read_text())["hooks"][event]
                    for h in group["hooks"] if module in h["command"])
        payload = {"session_id": "test", "tool_use_id": f"call-{self.calls}", "hook_event_name": event,
                   "tool_name": "Bash", "tool_input": {"command": command}, "tool_response": None}
        return subprocess.run(hook["command"], shell=True, cwd=self.root, input=json.dumps(payload),
                              text=True, capture_output=True, timeout=60)

    def shell_write(self, client, command, files):
        """Bracket a simulated command that writes `files` with the client's two Bash hooks."""
        self.calls += 1
        pre = self.invoke(client, "PreToolUse", command)
        if pre.returncode:
            raise AssertionError(f"pre hook failed: {pre.stderr}")
        for rel, content in files.items():
            (self.root / rel).write_text(content)
        return self.invoke(client, "PostToolUse", command)


if __name__ == "__main__":
    unittest.main()
