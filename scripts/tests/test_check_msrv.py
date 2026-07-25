"""Behavior tests for scripts/check-msrv.py."""

import importlib.util
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).parents[1] / "check-msrv.py"
SPEC = importlib.util.spec_from_file_location("check_msrv", SCRIPT_PATH)
CHECK_MSRV = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(CHECK_MSRV)


def metadata_fixture(*, dependency_msrv="1.95", member_msrv="1.95.0"):
    root_id = "path+file:///workspace#bamboo-agent@0.0.0"
    member_id = "path+file:///workspace/member#member@0.0.0"
    dependency_id = "registry+https://example.invalid#index@1.0.0"
    return {
        "packages": [
            {
                "id": root_id,
                "name": "bamboo-agent",
                "version": "0.0.0",
                "rust_version": "1.95",
            },
            {
                "id": member_id,
                "name": "member",
                "version": "0.0.0",
                "rust_version": member_msrv,
            },
            {
                "id": dependency_id,
                "name": "target-only-dependency",
                "version": "1.0.0",
                "rust_version": dependency_msrv,
            },
        ],
        "workspace_members": [root_id, member_id],
        "resolve": {
            "root": root_id,
            "nodes": [
                {"id": root_id},
                {"id": member_id},
                {"id": dependency_id},
            ],
        },
    }


class AuditMetadataTests(unittest.TestCase):
    def test_accepts_equivalent_workspace_versions_and_full_locked_graph(self):
        errors, declared, workspace_count, resolved_count = CHECK_MSRV.audit_metadata(
            metadata_fixture()
        )

        self.assertEqual(errors, [])
        self.assertEqual(declared, "1.95")
        self.assertEqual(workspace_count, 2)
        self.assertEqual(resolved_count, 3)

    def test_rejects_target_only_dependency_above_declared_msrv(self):
        errors, *_ = CHECK_MSRV.audit_metadata(
            metadata_fixture(dependency_msrv="1.96")
        )

        self.assertEqual(
            errors,
            [
                "target-only-dependency 1.0.0 requires Rust 1.96, "
                "above the declared Rust 1.95"
            ],
        )

    def test_rejects_incoherent_workspace_declaration(self):
        errors, *_ = CHECK_MSRV.audit_metadata(metadata_fixture(member_msrv="1.94"))

        self.assertEqual(
            errors,
            [
                "workspace package member declares Rust 1.94, "
                "but the root package declares Rust 1.95"
            ],
        )


if __name__ == "__main__":
    unittest.main()
