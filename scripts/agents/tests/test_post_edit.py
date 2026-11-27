import shutil
import subprocess
import unittest

from scripts.agents.common import ROOT
from scripts.agents.post_edit import LICENSE_HEADER, added_lines, check_edit, check_rust_lines, inline_paths, nudges


def rustfmt_runs():
    # A rustup proxy is on PATH even when the toolchain lacks the rustfmt component
    if not shutil.which("rustfmt"):
        return False
    return subprocess.run(["rustfmt", "--version"], cwd=ROOT, capture_output=True).returncode == 0


def flagged(line):
    return bool(check_rust_lines("x.rs", [line]))


class AddedTextTest(unittest.TestCase):
    def test_only_added_lines_are_judged(self):
        old = "a\n#[allow(clippy::foo)]\nb"
        new = "a\n#[allow(clippy::foo)]\nb\nc"
        self.assertEqual(added_lines(old, new), ["c"])

    def test_banned_constructs_are_flagged(self):
        for line in ["    assert!(matches!(x, Some(_)));", "#[allow(clippy::too_many_lines)]",
                     "#![allow(clippy::all)]", "#[expect(dead_code)]", "    dbg!(x);", "//////////"]:
            with self.subTest(line=line):
                self.assertTrue(flagged(line))

    def test_comments_must_not_cite_plans_or_narrate_history(self):
        for line in ["// see plan 7 item 3", "// fixes JIRA-123", "// per issue #1588",
                     "/// This used to be a Vec", "// renamed from FooBar", "// this change adds retries"]:
            with self.subTest(line=line):
                self.assertTrue(flagged(line))

    def test_legitimate_text_passes(self):
        for line in ["// RF-012: apply is idempotent", "// UTF-8 bytes, SHA-256 digest", "// Step 1: load the plan",
                     'let url = "http://host/plan 7";', "    assert_matches!(x, Some(_));", "/" * 120,
                     "let plan = Plan::new(7);"]:
            with self.subTest(line=line):
                self.assertFalse(flagged(line))

    def test_inline_crate_paths_are_nudged(self):
        for line in ["    owner: Option<kamu_resources::ResourceRef>,", "let s = std::sync::LazyLock::new(f);",
                     "b.add::<kamu_resources_inmem::InMemoryResourceRepository>();", "x.map(crate::make_test_snapshot)"]:
            with self.subTest(line=line):
                self.assertTrue(inline_paths("src/domain/x/src/a.rs", [line]))
        self.assertIn("kamu-rust-style", nudges("src/domain/x/src/a.rs", ["let r: kamu_resources::ResourceRef;"])[0])

    def test_imports_and_legitimate_paths_are_not_nudged(self):
        for line in ["use kamu_resources::ResourceRef;", "pub use crate::make_test_snapshot;", "let id = odf::DatasetID::new();",
                     "tracing::info!(id, \"x\");", "$crate::ResourceID::new()", "fmt::Display::fmt(&self.0, f)",
                     '#[graphql(remote = "kamu_resources::ResourceVisibility")]', 'let s = "kamu_resources::X";',
                     "let a = 1; // kamu_resources::ResourceRef", "Self::Foo::bar()", "super::test_fn",
                     "    fixture = kamu_cli_e2e_repo_tests::commands::test_delete,"]:
            with self.subTest(line=line):
                self.assertEqual(inline_paths("src/domain/x/src/a.rs", [line]), [])
        self.assertEqual(inline_paths("src/app/cli/src/app.rs", ["b.add::<kamu_resources::X>();"]), [])

    def test_nudges_name_the_follow_up_command(self):
        self.assertIn("sqlx-prepare", nudges("migrations/postgres/x.sql", ["CREATE TABLE x ();"])[0])
        self.assertIn("(cd src/infra/accounts/postgres && cargo sqlx prepare)",
                      nudges("src/infra/accounts/postgres/src/r.rs", ["sqlx::query!(\"x\")"])[0])
        self.assertEqual(nudges("src/infra/accounts/postgres/src/r.rs", ["let a = 1;"]), [])
        self.assertIn("schema.gql", nudges("src/adapter/graphql/src/root.rs", ["x"])[0])
        self.assertIn("src/e2e/app/cli/sqlite/", nudges("src/e2e/app/cli/postgres/tests/x.rs", ["x"])[0])


class FileTest(unittest.TestCase):
    """Uses a gitignored scratch directory inside the repository; requires rustfmt, cargo-sort and taplo."""

    def setUp(self):
        self.dir = ROOT / ".claude" / "state" / "test-post-edit"
        self.dir.mkdir(parents=True, exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.dir, ignore_errors=True)

    @unittest.skipUnless(rustfmt_runs(), "rustfmt not installed for the pinned toolchain")
    def test_new_file_is_formatted_and_needs_the_license_header(self):
        path = self.dir / "x.rs"
        path.write_text("fn main(){let a=1;}\n")
        problems, _ = check_edit(str(path), None, path.read_text())
        self.assertTrue(any("license header" in p for p in problems))
        self.assertIn("fn main() {", path.read_text())

        path.write_text(LICENSE_HEADER + "\n\nfn main() {}\n")
        problems, _ = check_edit(str(path), None, path.read_text())
        self.assertEqual(problems, [])

    @unittest.skipUnless(shutil.which("cargo-sort") and shutil.which("taplo"), "cargo-sort or taplo not installed")
    def test_manifest_is_sorted_and_formatted(self):
        path = self.dir / "Cargo.toml"
        path.write_text('[package]\nname = "x"\n\n[dependencies]\nzeta = "1"\nalpha   =   "1"\n')
        problems, notes = check_edit(str(path), None, path.read_text())
        self.assertEqual((problems, notes), ([], []))
        self.assertLess(path.read_text().index("alpha"), path.read_text().index("zeta"))
        self.assertIn('alpha = "1"', path.read_text())

    def test_paths_outside_the_repository_are_ignored(self):
        self.assertEqual(check_edit("/tmp/elsewhere.rs", None, "dbg!(x);"), ([], []))


if __name__ == "__main__":
    unittest.main()
