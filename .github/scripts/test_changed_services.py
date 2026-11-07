import json
import unittest
from pathlib import Path

from changed_services import select_services

SERVICES = json.loads((Path(__file__).parent.parent / "services.json").read_text())


def names(files):
    return [s["name"] for s in select_services(SERVICES, files)]


class SelectServicesTest(unittest.TestCase):
    def test_the_backend_folder_selects_the_server(self):
        self.assertEqual(names(["backend/src/profile.rs"]), ["server"])
        self.assertEqual(names(["backend/Cargo.lock"]), ["server"])

    def test_files_outside_the_backend_select_nothing(self):
        self.assertEqual(names(["README.md", "k8s/helm/values.yaml", "k8s/config-map.yaml"]), [])

    def test_a_folder_sharing_a_prefix_is_not_the_backend(self):
        self.assertEqual(names(["backendx/main.rs"]), [])

    def test_the_pipeline_itself_selects_every_service(self):
        for f in [".github/services.json", ".github/workflows/services.yml", ".github/scripts/deploy_tags.py"]:
            self.assertEqual(len(names([f])), len(SERVICES), f)

    def test_an_unknown_previous_commit_selects_every_service(self):
        self.assertEqual(len(names(None)), len(SERVICES))


if __name__ == "__main__":
    unittest.main()
