import unittest

from scripts.agents.command_policy import decide


def verdict(command):
    v = decide(command)
    return (v.decision[0], v.kind) if v else None


class DiscardTest(unittest.TestCase):
    def test_destructive_git_forms_are_denied(self):
        for command in [
            "git reset --hard",
            "git reset --hard HEAD~1",
            "git clean -fd",
            "git clean -xdf",
            "git checkout -- src/lib.rs",
            "git checkout .",
            "git checkout -f main",
            "git switch --discard-changes main",
            "git restore src/lib.rs",
            "git restore --staged --worktree src/lib.rs",
            "git stash",
            "git stash push -m wip",
            "git stash drop",
            "git read-tree --reset -u HEAD",
            "git checkout-index -f -a",
            "git -C /repo checkout -- x",
        ]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("deny", "discard"))

    def test_safe_git_forms_pass(self):
        for command in [
            "git status",
            "git diff HEAD -- src",
            "git checkout main",
            "git checkout -b feature/x",
            "git restore --staged src/lib.rs",
            "git stash list",
            "git stash pop",
            "git clean -n",
            "git log --oneline | head -5",
            "git reset HEAD src/lib.rs",
            "git show HEAD:AGENTS.md > /tmp/orig",
            "git tag",
        ]:
            with self.subTest(command=command):
                self.assertIsNone(verdict(command))

    def test_wrappers_and_compound_lines_are_seen_through(self):
        for command in [
            "ls && git reset --hard",
            "timeout 60 git clean -fd",
            "env FOO=1 git stash",
            "nohup git checkout -- x &",
            "bash -c 'git reset --hard'",
            "eval git stash",
            "{ git reset --hard; }",
            "echo $(git stash)",
            "if true; then git restore x; fi",
            "cd src \\\n && git checkout .",
            "echo done\ngit stash",
        ]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("deny", "discard"))

    def test_quoted_and_heredoc_text_is_data(self):
        self.assertEqual(verdict('git commit -m "never git reset --hard"'), ("ask", "approval"))
        self.assertIsNone(verdict("echo 'git reset --hard'"))
        self.assertIsNone(verdict("python3 - <<'EOF'\ngit reset --hard\nEOF\necho ok"))

    def test_deny_wins_over_ask_on_one_line(self):
        self.assertEqual(verdict("git commit -m x && git reset --hard"), ("deny", "discard"))

    def test_unlexable_input_falls_back_to_refusal(self):
        self.assertEqual(verdict("git reset --hard 'unterminated"), ("deny", "discard"))


class ApprovalTest(unittest.TestCase):
    def test_history_and_publishing_commands_ask(self):
        for command in ["git commit -m x", "git push origin master", "git merge x", "git rebase master",
                        "git tag v1.0", "git cherry-pick abc", "git reset --soft HEAD~1", "git branch -D x"]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("ask", "approval"))


class CargoTest(unittest.TestCase):
    def test_package_scoped_builds_ask(self):
        for command in ["cargo build -p kamu", "cargo check --package kamu", "cargo clippy --package=kamu",
                        "cargo nextest run -p kamu-cli", "cargo test -pkamu", "cargo +nightly build -p x"]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("ask", "build-scope"))

    def test_package_specs_and_filters_pass(self):
        for command in ["cargo update -p arrow", "cargo -Z unstable-options update --breaking -p arrow --dry-run",
                        "cargo tree -p kamu", "cargo nextest run -E 'package(kamu) and test(x)'",
                        "cargo run --bin kamu -- -p foo", "cargo build"]:
            with self.subTest(command=command):
                self.assertIsNone(verdict(command))

    def test_sqlx_offline_override_is_denied(self):
        for command in ["SQLX_OFFLINE=true cargo build", "env SQLX_OFFLINE=false cargo check",
                        "export SQLX_OFFLINE=true", "A=1 SQLX_OFFLINE=true make clippy"]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("deny", "sqlx"))
        self.assertIsNone(verdict("grep SQLX_OFFLINE .env"))

    def test_truncated_build_output_is_denied(self):
        for command in ["cargo nextest run 2>&1 | tail -50", "make clippy | head", "make test-fast |& tail"]:
            with self.subTest(command=command):
                self.assertEqual(verdict(command), ("deny", "truncate"))
        self.assertIsNone(verdict("cargo nextest run 2>&1 | grep FAIL"))


if __name__ == "__main__":
    unittest.main()
