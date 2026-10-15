#!/usr/bin/env python3
"""Owned, offline old-reader reproduction; never edits an existing data root."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

OLD_COMMIT = "48e1283fe6ce94078a2cb9554e75f0552c3fc52c"
ID = "interrupted-migration"
PRIMARY = "project.json"
BACKUP = "project.json.bak"
FLOOR = "state/manifest-revision"
FORENSIC = "project.json.migrated-v2"


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, capture_output=True, **kwargs)


def digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n"


def files_at(data):
    home = data / "projects" / ID
    return {name: (home / name).read_text() for name in [PRIMARY, BACKUP, FLOOR, FORENSIC]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository", type=Path, required=True)
    parser.add_argument("--legacy-source", type=Path, required=True)
    parser.add_argument("--artifacts", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    args = parser.parse_args()
    root = args.artifacts.resolve()
    root.mkdir(parents=True, exist_ok=False)
    assert not list(root.iterdir())
    repository = args.repository.resolve()
    legacy = args.legacy_source.resolve()
    source_proof = {}
    paths = ["Cargo.toml"] + run([
        "git", "-C", str(repository), "ls-tree", "-r", "--name-only", OLD_COMMIT,
        "crates/core/bamboo-domain", "crates/infra/bamboo-projects",
    ]).stdout.splitlines()
    for relative in paths:
        expected = subprocess.check_output([
            "git", "-C", str(repository), "show", f"{OLD_COMMIT}:{relative}"
        ])
        actual = (legacy / relative).read_bytes()
        assert actual == expected, f"archived source differs: {relative}"
        source_proof[relative] = hashlib.sha256(actual).hexdigest()
    (root / "legacy-source-proof.json").write_text(encode(source_proof))

    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(args.target.resolve())
    env["CARGO_BUILD_JOBS"] = "1"
    for variable, name in [("CARGO_BUILD_BUILD_DIR", "intermediate"), ("TMPDIR", "tmp")]:
        directory = args.target.resolve().parent() / name
        directory.mkdir(parents=True, exist_ok=True)
        env[variable] = str(directory)
    drivers = {}
    for name, source in [("legacy", legacy), ("current", repository)]:
        project = root / (name + "-driver")
        (project / "src").mkdir(parents=True)
        manifest = (
            "[package]\n"
            f'name = "project-migration-{name}-driver"\n'
            'version = "0.0.0"\nedition = "2021"\n[workspace]\n[dependencies]\n'
            f'bamboo-domain = {{ path = {json.dumps(str(source / "crates/core/bamboo-domain"))} }}\n'
            f'bamboo-projects = {{ path = {json.dumps(str(source / "crates/infra/bamboo-projects"))} }}\n'
            'serde_json = "1"\n'
        )
        (project / "Cargo.toml").write_text(manifest)
        shutil.copyfile(Path(__file__).with_name("driver.rs"), project / "src/main.rs")
        drivers[name] = args.target.resolve() / "debug" / ("project-migration-" + name + "-driver")
        lock = run(["cargo", "generate-lockfile", "--offline", "--manifest-path", str(project / "Cargo.toml")], env=env)
        (root / (name + "-lock.log")).write_text(lock.stdout + lock.stderr)
        build = subprocess.run(["cargo", "build", "--locked", "--offline", "--manifest-path", str(project / "Cargo.toml")], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (root / (name + "-build.log")).write_text(build.stdout)
        build.check_returncode()
        assert drivers[name].is_file(), drivers[name]

    legacy_manifest = {
        "schema_version": 2, "id": ID, "name": "original legacy metadata",
        "revision": 1, "resource_revision": 1,
        "created_at": "2026-10-15T00:00:00Z", "updated_at": "2026-10-15T00:00:00Z",
        "project_path": None, "project_path_status": "needs_configuration",
        "sidebar": {"sections": [{"id": "section:2d567c47-a081-4a8d-9c9e-246bce470c9f", "name": "Preserve me"}], "session_sections": {"root": None}, "hidden_legacy": False},
        "experimental_unknown_field": {"must_retain_raw_bytes": [1, "value"]},
    }
    original = encode(legacy_manifest)
    candidate = dict(legacy_manifest, schema_version=3, revision=2)
    # The current DTO knows sidebar but not arbitrary experimental fields;
    # those raw bytes belong in the forensic snapshot, not its schema-3 DTO.
    candidate.pop("experimental_unknown_field")
    completed = dict(candidate)
    case_results = []
    for stage in ["after_backup", "after_floor", "published"]:
        for operation in ["get", "update"]:
            case = root / f"{stage}-{operation}"
            case.mkdir()
            files = {
                PRIMARY: encode(completed) if stage == "published" else original,
                BACKUP: encode(candidate), FLOOR: "1\n" if stage == "after_backup" else "2\n",
                FORENSIC: original,
            }
            seed = case / "seed.json"
            seed.write_text(encode(files))
            before = {k: digest(v) for k, v in files.items()}
            result = json.loads(run([str(drivers["legacy"]), str(case / "old-data"), str(seed), operation]).stdout)
            assert result["reader_schema"] == 2
            after = files_at(case / "old-data")
            primary = json.loads(after[PRIMARY])
            expected_loss = stage != "published" and (operation == "update" or stage == "after_floor")
            if stage == "published":
                assert not result["ok"] and not result["mutated"]
                assert after == files
            else:
                assert result["ok"]
                assert ("sidebar" not in primary) == expected_loss
                assert after[FORENSIC] == original
            restart_seed = case / "restart-seed.json"
            restart_seed.write_text(encode(after))
            restarted = json.loads(run([str(drivers["current"]), str(case / "new-data"), str(restart_seed), "get"]).stdout)
            assert restarted["reader_schema"] == 3
            final = files_at(case / "new-data")
            protected = final[FORENSIC] == original
            if expected_loss:
                protected = protected and not restarted["ok"] and final == after
            else:
                protected = protected and restarted["ok"]
                assert json.loads(final[PRIMARY])["sidebar"] == legacy_manifest["sidebar"]
            record = {"stage": stage, "operation": operation, "old": result, "current": restarted, "old_lost_sidebar": expected_loss, "original_forensic_preserved_after_old": after[FORENSIC] == original, "original_forensic_preserved_after_restart": final[FORENSIC] == original, "retry_contract_passed": protected, "hashes_before": before, "hashes_after_old": {k: digest(v) for k, v in after.items()}, "hashes_after_restart": {k: digest(v) for k, v in final.items()}}
            case_results.append(record)
            print(encode({k: record[k] for k in ["stage", "operation", "old_lost_sidebar", "original_forensic_preserved_after_restart"]}).strip(), flush=True)
    receipt = {"legacy_commit": OLD_COMMIT, "current_commit": run(["git", "-C", str(repository), "rev-parse", "HEAD"]).stdout.strip(), "current_tree": run(["git", "-C", str(repository), "rev-parse", "HEAD^{tree}"]).stdout.strip(), "source_status": run(["git", "-C", str(repository), "status", "--porcelain=v1"]).stdout.strip(), "cases": case_results, "source_proof": source_proof, "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(), "driver_sha256": hashlib.sha256(Path(__file__).with_name("driver.rs").read_bytes()).hexdigest(), "binary_sha256": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in drivers.items()}, "lock_sha256": {name: hashlib.sha256((root / (name + "-driver") / "Cargo.lock").read_bytes()).hexdigest() for name in drivers}, "toolchain": {program: run([program, "--version"]).stdout.strip() for program in ["cargo", "rustc"]}, "build_env": {name: env[name] for name in ["CARGO_TARGET_DIR", "CARGO_BUILD_JOBS", "CARGO_BUILD_BUILD_DIR", "TMPDIR"]}, "fixtures_only": True, "power_loss_tested": False}
    (root / "receipt.json").write_text(encode(receipt))
    passed = all(c["retry_contract_passed"] for c in case_results)
    print("Original forensic checkpoint preserved and retry contract passed:", passed, flush=True)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
