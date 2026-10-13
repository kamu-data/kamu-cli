import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.agents.codex_common import codex_decision, patch_files
from scripts.agents.command_policy import decide
from scripts.agents.common import ROOT

PATCH = """*** Begin Patch
*** Update File: src/adapter/graphql/src/root.rs
@@ fn x
 context
-removed
+added one
*** Add File: docs/internal/new.md
+# New
*** Delete File: old.rs
*** End Patch"""


class CodexTest(unittest.TestCase):
    def test_patch_paths_and_added_lines(self):
        self.assertEqual(patch_files(PATCH), {
            "src/adapter/graphql/src/root.rs": ["added one"],
            "docs/internal/new.md": ["# New"],
            "old.rs": [],
        })

    def test_codex_only_denies(self):
        self.assertEqual(codex_decision(decide("git reset --hard"))[0], "deny")
        self.assertEqual(codex_decision(decide("cargo build -p kamu"))[0], "deny")
        self.assertIsNone(codex_decision(decide("git commit -m x")))
        self.assertIsNone(codex_decision(decide("git status")))

    def test_move_checks_both_paths_and_attributes_added_lines_to_destination(self):
        self.assertEqual(patch_files("*** Begin Patch\n*** Update File: old.rs\n"
                                     "*** Move to: new.rs\n@@\n+added\n*** End Patch"),
                         {"old.rs": [], "new.rs": ["added"]})


class CodexHookTest(unittest.TestCase):
    def setUp(self):
        self.harness = CodexHookHarness()
        self.addCleanup(self.harness.close)

    def test_shell_guard_denies_destructive_commands_and_passes_reads(self):
        self.assertEqual(self.harness.decision("Bash", "git reset --hard"), "deny")
        self.assertEqual(self.harness.decision("Bash", "cargo build -p kamu"), "deny")
        self.assertIsNone(self.harness.decision("Bash", "git status"))

    def test_generated_files_cannot_be_added_deleted_or_moved(self):
        for patch in ["*** Add File: resources/schema.gql\n+text",
                      "*** Delete File: resources/schema.gql",
                      "*** Update File: ordinary.txt\n*** Move to: resources/schema.gql"]:
            with self.subTest(patch=patch):
                self.assertEqual(self.harness.patch_decision(patch), "deny")

    def test_guarded_patch_requires_a_successful_skill_read_in_the_same_session(self):
        patch = "*** Update File: AGENTS.md\n@@\n+text"
        self.assertEqual(self.harness.patch_decision(patch), "deny")
        self.harness.read_skill(exit_code=1)
        self.assertEqual(self.harness.patch_decision(patch), "deny")
        self.harness.read_skill()
        self.assertIsNone(self.harness.patch_decision(patch))
        self.assertEqual(self.harness.patch_decision(patch, session="other"), "deny")

    def test_compaction_requires_skill_reload_and_uses_codex_instructions(self):
        self.harness.read_skill()
        output = self.harness.run("SessionStart", source="compact")
        context = output["hookSpecificOutput"]["additionalContext"]
        self.assertIn("cat .agents/skills/", context)
        self.assertNotIn("the Skill tool", context)
        self.assertEqual(self.harness.patch_decision("*** Update File: AGENTS.md"), "deny")

    @unittest.skipUnless(shutil.which("rustfmt"), "rustfmt not installed")
    def test_post_patch_formats_rust_and_reports_added_line_violations(self):
        self.harness.write_rust("fn main(){dbg!(1);}")
        result = self.harness.invoke("PostToolUse", "apply_patch", command=self.harness.patch(
            "*** Add File: example.rs\n+fn main(){dbg!(1);}"))
        self.assertEqual(result.returncode, 2)
        self.assertIn("dbg!", result.stderr)
        self.assertIn("fn main() {", (self.harness.root / "example.rs").read_text())

    def test_post_patch_provides_regeneration_context(self):
        output = self.harness.run("PostToolUse", "apply_patch", command=self.harness.patch(
            "*** Update File: src/adapter/graphql/src/root.rs\n@@\n+text"))
        self.assertIn("make resources-graphql-schema", output["hookSpecificOutput"]["additionalContext"])


class CodexHookHarness:
    def __init__(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.cwd = self.root / "nested"
        self.cwd.mkdir()
        shutil.copytree(ROOT / "scripts" / "agents", self.root / "scripts" / "agents",
                        ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copytree(ROOT / ".claude" / "hooks", self.root / ".claude" / "hooks")
        (self.root / "docs").mkdir()
        shutil.copy(ROOT / "docs" / "license_header.txt", self.root / "docs" / "license_header.txt")
        subprocess.run(["git", "init", "-q", str(self.root)], check=True, capture_output=True)
        self.config = json.loads((ROOT / ".codex" / "hooks.json").read_text())["hooks"]

    def close(self):
        self.temp.cleanup()

    def invoke(self, event, tool=None, session="test", source=None, response=None, **tool_input):
        groups = self.config[event]
        group = next(g for g in groups if tool is None or g["matcher"] == tool)
        payload = {"session_id": session, "cwd": str(self.cwd), "hook_event_name": event,
                   "tool_name": tool, "tool_input": tool_input, "tool_response": response,
                   "source": source}
        return subprocess.run(group["hooks"][0]["command"], shell=True, cwd=self.cwd,
                              input=json.dumps(payload), text=True, capture_output=True, timeout=10)

    def run(self, event, tool=None, **kwargs):
        result = self.invoke(event, tool, **kwargs)
        if result.returncode or result.stderr:
            raise AssertionError(f"hook failed: {result.returncode}: {result.stderr}")
        return json.loads(result.stdout) if result.stdout else {}

    def decision(self, tool, command, **kwargs):
        output = self.run("PreToolUse", tool, command=command, **kwargs)
        return output.get("hookSpecificOutput", {}).get("permissionDecision")

    def patch(self, body):
        return f"*** Begin Patch\n{body}\n*** End Patch"

    def patch_decision(self, body, **kwargs):
        return self.decision("apply_patch", self.patch(body), **kwargs)

    def read_skill(self, exit_code=0):
        self.run("PostToolUse", "Bash", command="cat .agents/skills/kamu-prose-and-comments/SKILL.md",
                 response=f"Chunk ID: example\nProcess exited with code {exit_code}\nFinal output:\n")

    def write_rust(self, content):
        header = (self.root / "docs" / "license_header.txt").read_text()
        (self.root / "example.rs").write_text(f"{header}\n{content}\n")


if __name__ == "__main__":
    unittest.main()
