import json
import re
import unittest

from scripts.agents.common import ROOT, policy
from scripts.agents.session_contract import contract

MODULE = re.compile(r"python3 -m scripts\.agents\.(\w+)")


class ConfigTest(unittest.TestCase):
    def test_every_hook_module_exists(self):
        for config in [ROOT / ".claude" / "settings.json", ROOT / ".codex" / "hooks.json"]:
            for module in MODULE.findall(config.read_text()):
                with self.subTest(config=config.name, module=module):
                    self.assertTrue((ROOT / "scripts" / "agents" / f"{module}.py").is_file())

    def test_every_governed_skill_exists(self):
        for rule in policy()["skills"]:
            with self.subTest(skill=rule["skill"]):
                self.assertTrue((ROOT / ".claude" / "skills" / rule["skill"] / "SKILL.md").is_file())

    def test_the_session_contract_names_every_governed_skill(self):
        text = contract()
        for rule in policy()["skills"]:
            self.assertIn(rule["skill"], text)

    def test_configs_are_valid_json(self):
        for config in [ROOT / ".claude" / "settings.json", ROOT / ".codex" / "hooks.json"]:
            json.loads(config.read_text())


if __name__ == "__main__":
    unittest.main()
