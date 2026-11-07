import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from deploy_tags import get_helm_values_tag, deploy, plan, replace_tag

VALUES = """apps:
  onlineconversation:
    image: "backend/onlineconversation:latest"
    port: 8080
  offlineconversation:
    image: "backend/offlineconversation:1111111"
flink:
  jobs:
    conversation-cdc-job:
      image: flink/conversationcdcjob:2222222
"""


class TagTest(unittest.TestCase):
    def test_current_tag_reads_quoted_and_unquoted_images(self):
        self.assertEqual(get_helm_values_tag(VALUES, "backend/onlineconversation"), "latest")
        self.assertEqual(get_helm_values_tag(VALUES, "flink/conversationcdcjob"), "2222222")

    def test_current_tag_rejects_a_missing_image(self):
        with self.assertRaises(ValueError):
            get_helm_values_tag(VALUES, "backend/turn")

    def test_a_name_that_is_a_prefix_of_another_image_is_not_matched(self):
        with self.assertRaises(ValueError):
            get_helm_values_tag(VALUES, "backend/online")

    def test_replace_tag_changes_only_that_line(self):
        changed = replace_tag(VALUES, "backend/offlineconversation", "abc1234")
        self.assertEqual(changed, VALUES.replace("backend/offlineconversation:1111111", "backend/offlineconversation:abc1234"))


class PlanTest(unittest.TestCase):
    def plan(self, is_older=lambda sha, tag: False):
        return plan(VALUES, ["backend/onlineconversation", "backend/offlineconversation"], "abc1234", is_older)

    def test_writes_every_image_with_another_tag(self):
        values, written = self.plan()
        self.assertEqual(written, ["backend/onlineconversation", "backend/offlineconversation"])
        self.assertIn('image: "backend/onlineconversation:abc1234"', values)
        self.assertIn('image: "backend/offlineconversation:abc1234"', values)

    def test_an_image_already_at_the_tag_is_not_written(self):
        values, written = plan(VALUES, ["backend/offlineconversation"], "1111111", lambda sha, tag: False)
        self.assertEqual((values, written), (VALUES, []))

    def test_an_older_build_does_not_replace_a_newer_tag(self):
        values, written = self.plan(is_older=lambda sha, tag: tag == "1111111")
        self.assertEqual(written, ["backend/onlineconversation"])
        self.assertIn('image: "backend/offlineconversation:1111111"', values)


def run(cwd, *args):
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


class DeployTest(unittest.TestCase):
    """Runs a real deploy against a throwaway origin, including the push."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.origin, self.work = root / "origin.git", root / "work"
        run(root, "git", "init", "-q", "--bare", "-b", "main", str(self.origin))
        run(root, "git", "clone", "-q", str(self.origin), str(self.work))
        for key, value in [("user.email", "t@t"), ("user.name", "t"), ("init.defaultBranch", "main")]:
            run(self.work, "git", "config", key, value)
        run(self.work, "git", "checkout", "-q", "-b", "main")
        self.commits = []
        for message in ["c1", "c2", "c3"]:
            run(self.work, "git", "commit", "-q", "--allow-empty", "-m", message)
            self.commits.append(run(self.work, "git", "rev-parse", "HEAD"))
        (self.work / "values.yaml").write_text(
            f'apps:\n  onlineconversation:\n    image: "backend/onlineconversation:latest"\n'
            f'  offlineconversation:\n    image: "backend/offlineconversation:{self.commits[2][:7]}"\n'
        )
        run(self.work, "git", "add", "-A")
        run(self.work, "git", "commit", "-qm", "values")
        run(self.work, "git", "push", "-q", "origin", "HEAD:main")
        self.cwd = os.getcwd()
        os.chdir(self.work)

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def origin_values(self):
        return run(self.work, "git", "--git-dir", str(self.origin), "show", "main:values.yaml")

    def test_one_commit_for_every_written_image_and_older_builds_skipped(self):
        sha = self.commits[1]
        deploy("values.yaml", ["backend/onlineconversation", "backend/offlineconversation"], sha, "main")

        message = run(self.work, "git", "--git-dir", str(self.origin), "log", "-1", "--format=%s|%b", "main")
        self.assertEqual(message.strip(), f"deploy: {sha[:7]}|backend/onlineconversation")
        self.assertIn(f'image: "backend/onlineconversation:{sha[:7]}"', self.origin_values())
        self.assertIn(f'image: "backend/offlineconversation:{self.commits[2][:7]}"', self.origin_values())

    def test_the_same_build_again_makes_no_commit(self):
        sha = self.commits[1]
        deploy("values.yaml", ["backend/onlineconversation"], sha, "main")
        before = run(self.work, "git", "--git-dir", str(self.origin), "rev-parse", "main")
        deploy("values.yaml", ["backend/onlineconversation"], sha, "main")
        self.assertEqual(run(self.work, "git", "--git-dir", str(self.origin), "rev-parse", "main"), before)


if __name__ == "__main__":
    unittest.main()
