import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from scripts.agents.common import ROOT
from scripts.agents.skill_ledger import EXPIRY_SECONDS, forget_session, governing_skills, loaded, missing_skills, record, skills_read_by


class GoverningSkillTest(unittest.TestCase):
    def test_first_matching_rule_wins_and_baseline_rules_apply_in_addition(self):
        rust = "kamu-rust-style"
        cases = {
            "src/e2e/app/cli/repo-tests/src/x.rs": ["kamu-cli-e2e-tests", rust],
            "src/infra/accounts/repo-tests/src/x.rs": ["kamu-repository-tests", rust],
            "src/infra/accounts/postgres/src/x.rs": ["kamu-sqlx-database-work", rust],
            "src/infra/search/cache-sqlite/src/x.rs": ["kamu-sqlx-database-work", rust],
            "src/adapter/graphql/tests/tests/test_x.rs": ["kamu-graphql-api", rust],
            "src/domain/flow-system/services/tests/tests/test_x.rs": ["kamu-test-harness", rust],
            "src/domain/accounts/domain/src/lib.rs": [rust],
            "migrations/postgres/x.sql": ["kamu-sqlx-database-work"],
            "docs/internal/outbox.md": ["kamu-prose-and-comments"],
            "docs/license_header.txt": [],
        }
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(governing_skills(path), expected)


class LedgerTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.state = Path(self.dir.name) / "skills.json"
        self.path = str(ROOT / "migrations/postgres/x.sql")

    def tearDown(self):
        self.dir.cleanup()

    def test_edit_is_refused_until_the_skill_is_loaded(self):
        self.assertEqual(missing_skills([self.path], "s/main", self.state),
                         {"migrations/postgres/x.sql": ["kamu-sqlx-database-work"]})
        record({"kamu-sqlx-database-work"}, "s/main", self.state)
        self.assertEqual(missing_skills([self.path], "s/main", self.state), {})

    def test_a_subagent_loads_skills_for_itself_only(self):
        record({"kamu-sqlx-database-work"}, "s/agent-1", self.state)
        self.assertTrue(missing_skills([self.path], "s/main", self.state))
        record({"kamu-sqlx-database-work"}, "s/main", self.state)
        self.assertTrue(missing_skills([self.path], "s/agent-2", self.state))

    def test_a_compacted_session_loads_again(self):
        record({"kamu-sqlx-database-work"}, "s/main", self.state)
        record({"kamu-sqlx-database-work"}, "other/main", self.state)
        forget_session("s", self.state)
        self.assertTrue(missing_skills([self.path], "s/main", self.state))
        self.assertFalse(missing_skills([self.path], "other/main", self.state))

    def test_expired_records_do_not_authorize_edits(self):
        with mock.patch("scripts.agents.skill_ledger.now", return_value=1):
            record({"kamu-sqlx-database-work"}, "s/main", self.state)
        with mock.patch("scripts.agents.skill_ledger.now", return_value=EXPIRY_SECONDS + 2):
            self.assertTrue(missing_skills([self.path], "s/main", self.state))

    def test_parallel_skill_reads_preserve_every_record(self):
        skills = {f"skill-{i}" for i in range(32)}
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda skill: record({skill}, "s/main", self.state), skills))
        self.assertEqual(loaded("s/main", self.state), skills)

    def test_a_rust_file_under_a_skill_path_needs_both_skills(self):
        path = str(ROOT / "src/infra/accounts/postgres/src/x.rs")
        record({"kamu-sqlx-database-work"}, "s/main", self.state)
        self.assertEqual(missing_skills([path], "s/main", self.state),
                         {"src/infra/accounts/postgres/src/x.rs": ["kamu-rust-style"]})
        record({"kamu-rust-style"}, "s/main", self.state)
        self.assertEqual(missing_skills([path], "s/main", self.state), {})

    def test_shell_reads_of_skill_files_count_as_loads(self):
        self.assertEqual(skills_read_by("cat .agents/skills/kamu-graphql-api/SKILL.md"), {"kamu-graphql-api"})
        self.assertEqual(skills_read_by("sed -n 1,80p .claude/skills/kamu-dill-di/SKILL.md"), {"kamu-dill-di"})
        self.assertEqual(skills_read_by("sed -i s/a/b/ .claude/skills/kamu-dill-di/SKILL.md"), set())
        self.assertEqual(skills_read_by("ls .claude/skills/kamu-dill-di/SKILL.md"), set())
        self.assertEqual(skills_read_by("cat .claude/skills/kamu-dill-di/SKILL.md > /tmp/copy"), {"kamu-dill-di"})
        self.assertEqual(skills_read_by("cat < .claude/skills/kamu-dill-di/SKILL.md"), {"kamu-dill-di"})

    def test_writing_a_skill_through_a_redirect_is_not_a_load(self):
        self.assertEqual(skills_read_by("cat > .claude/skills/kamu-dill-di/SKILL.md <<'EOF'\n# x\nEOF"), set())
        self.assertEqual(skills_read_by("cat header.md >> .agents/skills/kamu-dill-di/SKILL.md"), set())


if __name__ == "__main__":
    unittest.main()
