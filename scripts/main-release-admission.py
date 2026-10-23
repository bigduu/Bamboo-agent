"""Checkout-free release admission; executed only by the digest-pinned loader.

Full release CI proves its actual synthetic merge inputs. This module authorizes
the equivalent main merge, but never makes the tested binary a release artifact:
the final source SHA and stamped version remain new executable build inputs.
"""

import base64
import datetime
import hashlib
import json
import os
import re
import subprocess

repository = os.environ["GITHUB_REPOSITORY"]
assert repository == "bigduu/Bamboo-agent"
event_name = os.environ["GITHUB_EVENT_NAME"]
mode = os.environ["ADMISSION_MODE"]
assert mode in {"main", "publication"}
assert re.fullmatch(r"[0-9a-f]{64}", ADMISSION_SCRIPT_SHA256)
with open(os.environ["GITHUB_EVENT_PATH"]) as stream:
    event = json.load(stream)
SHA = r"[0-9a-f]{40}"
workflow_source = os.environ["GITHUB_WORKFLOW_SHA"]
assert re.fullmatch(SHA, workflow_source)
if mode == "publication":
    assert event_name == "workflow_run"
    run = event["workflow_run"]
    source = run["head_sha"]
    # Publication's controller comes only from protected dev. Main protection
    # alone never proves that this source followed the normal merge process.
    assert os.environ["GITHUB_REF"] == "refs/heads/dev"
    assert os.environ["GITHUB_REF_PROTECTED"] == "true"
    assert os.environ["GITHUB_WORKFLOW_REF"] == f"{repository}/.github/workflows/publish-crate.yml@refs/heads/dev"
else:
    assert event_name == "push"
    assert os.environ["GITHUB_REF"] == "refs/heads/main"
    assert os.environ["GITHUB_WORKFLOW_REF"] == f"{repository}/.github/workflows/ci.yml@refs/heads/main"
    assert os.environ["GITHUB_SHA"] == workflow_source
    assert os.environ["SOURCE_RESULT"] == "success" and os.environ["FULL_VALIDATION"] == "false"
    source = os.environ["GITHUB_SHA"]
    assert event["ref"] == "refs/heads/main" and event["after"] == source
    assert event["repository"]["full_name"] == repository
assert re.fullmatch(SHA, source)

def api(route):
    # Fixed GitHub origin and GET-only transport; never checkout or execute
    # source code to establish its own publication authority.
    return json.loads(subprocess.check_output(
        ["gh", "api", "--hostname", "github.com", f"repos/{repository}/{route}"],
        text=True, timeout=30,
    ))

def pages(route, field=None):
    result = []
    for page in range(1, 11):
        data = api(f"{route}{'&' if '?' in route else '?'}per_page=100&page={page}")
        rows = data[field] if field else data
        assert isinstance(rows, list) and len(rows) <= 100
        result.extend(rows)
        if len(rows) < 100:
            return result
    raise AssertionError("Publication proof exceeds the bounded API page limit")

def branch(name, protected=False):
    data = api(f"branches/{name}")
    assert data["name"] == name
    if protected:
        assert data["protected"] is True, "dev must remain protected"
    sha = data["commit"]["sha"]
    assert re.fullmatch(SHA, sha)
    return sha

def ancestor(earlier, later):
    proof = api(f"compare/{earlier}...{later}")
    assert proof["base_commit"]["sha"] == earlier
    assert proof["merge_base_commit"]["sha"] == earlier
    assert proof["status"] in {"ahead", "identical"}, "Missing immutable ancestry proof"

def ci_identity(ci, event_type, sha, ref):
    assert type(ci["id"]) is int and ci["id"] > 0
    assert type(ci["run_attempt"]) is int and ci["run_attempt"] > 0
    assert ci["name"] == "CI" and ci["path"] == ".github/workflows/ci.yml"
    assert ci["event"] == event_type and ci["head_sha"] == sha and ci["head_branch"] == ref
    assert ci["repository"]["full_name"] == ci["head_repository"]["full_name"] == repository
    assert ci["status"] == "completed" and ci["conclusion"] == "success"

def instant(value):
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    assert parsed.tzinfo is not None
    return parsed

FULL = (
    "Validation Source", "Test", "E2E Tests", "Lint",
    "MSRV (Rust 1.95)", "Security Audit", "cargo-deny (licenses/bans/advisories/sources)",
    "Documentation", "TLS Fixture (macos-latest)", "TLS Fixture (windows-latest)",
    "ToolEvent Recorder E2E (ubuntu-latest)", "ToolEvent Recorder E2E (macos-latest)",
    "ToolEvent Recorder E2E (windows-latest)",
)

PLATFORMS = ("Build (ubuntu-latest)", "Build (macos-latest)", "Build (windows-latest)")

def full_jobs(ci, promotion=False, before=None):
    # latest is intentionally per job: failed-only reruns retain the successful
    # jobs from earlier attempts. Do not invent an all-jobs-same-attempt rule.
    jobs = pages(f"actions/runs/{ci['id']}/jobs?filter=latest", "jobs")
    for name in FULL + (PLATFORMS + ("Promotion Source",) if promotion else ()):
        found = [job for job in jobs if job["name"] == name]
        assert len(found) == 1, f"Missing or ambiguous full CI job: {name}"
        job = found[0]
        assert job["run_id"] == ci["id"]
        assert type(job["run_attempt"]) is int and 1 <= job["run_attempt"] <= ci["run_attempt"]
        if promotion:
            assert job["head_sha"] == ci["head_sha"] and job["head_branch"] == ci["head_branch"]
        assert job["status"] == "completed" and job["conclusion"] == "success", name
        if before is not None:
            assert instant(job["completed_at"]) < before, "Release CI must finish before merge"
    return jobs

ci = None
if mode == "publication":
    assert type(run["id"]) is int and run["id"] > 0
    ci = api(f"actions/runs/{run['id']}")
    ci_identity(ci, "push", source, "main")
    for key in ("id", "run_attempt", "name", "event", "head_sha", "head_branch", "conclusion"):
        assert ci[key] == run[key], "Stale or mismatched workflow_run delivery"
    assert run["repository"]["full_name"] == run["head_repository"]["full_name"] == repository
assert branch("main") == source, "Queued CI source is no longer current main"
dev = branch("dev", protected=True)
if mode == "publication":
    ancestor(workflow_source, dev)

# Match the actual server-recorded normal merge, never a caller-provided PR,
# a release description, an artifact or a caller-provided status/log.
associated = pages(f"commits/{source}/pulls")
candidates = [pr for pr in associated if pr.get("merge_commit_sha") == source
              and pr.get("base", {}).get("ref") == "main"]
assert len(candidates) == 1, "One normal release PR must authorize this main commit"
number = candidates[0]["number"]
assert type(number) is int and number > 0
pr = api(f"pulls/{number}")
assert pr["number"] == number and pr["state"] == "closed" and pr["merged"] is True
assert pr["draft"] is False and pr["merge_commit_sha"] == source
assert pr["base"]["ref"] == "main"
assert pr["head"]["repo"]["full_name"] == pr["base"]["repo"]["full_name"] == repository
match = re.fullmatch(r"release/([0-9a-f]{40})", pr["head"]["ref"])
assert match, "Main publication requires the normal frozen release branch"
cut = match.group(1)
head, base = pr["head"]["sha"], pr["base"]["sha"]
assert re.fullmatch(SHA, head) and re.fullmatch(SHA, base)
commit = api(f"git/commits/{source}")
release_commit = api(f"git/commits/{head}")
assert commit["sha"] == source and release_commit["sha"] == head
assert [parent["sha"] for parent in commit["parents"]] == [base, head], "Require the normal two-parent merge"
assert re.fullmatch(SHA, commit["tree"]["sha"])
assert commit["tree"]["sha"] == release_commit["tree"]["sha"], "Main must preserve the exact reviewed release-head tree"
ancestor(cut, head)
ancestor(head, dev)

def binds_pr(candidate):
    return any(item.get("number") == number
               and item.get("base", {}).get("ref") == "main"
               and item["base"].get("sha") == base
               and item.get("head", {}).get("ref") == pr["head"]["ref"]
               and item["head"].get("sha") == head
               and item["base"].get("repo", {}).get("id") == pr["base"]["repo"]["id"]
               and item["head"].get("repo", {}).get("id") == pr["head"]["repo"]["id"]
               for item in candidate.get("pull_requests", []))

# GitHub's live PR association can become empty after merge. Authenticate the
# actual tested snapshot in both cases, using only a reviewed producer.
# Pin the complete definition (including global env and the checkout-free
# source job), not merely a job name or arbitrary repository-authored log.
PRODUCER_V1 = "64ced424ec999ec332cbc926d52f1d541721a2f05c2a6361b59cdbb869973f24"
# #2109 adds Node driver checks; checkout-free source admission is unchanged.
PRODUCER_V2 = "987181c31ab13af8fe85ec46586d4cacdf5ba9f3acda38c14a2b0b72671a068a"
REVIEWED_PRODUCERS = frozenset((PRODUCER_V1, PRODUCER_V2))
# Hash the complete current workflow with exactly one helper-pin value replaced
# by a sentinel. Then independently authenticate the actual helper bytes below.
# This breaks the workflow -> helper -> workflow hash cycle, without excluding
# a job, global environment, trigger, command or permission from review.
CURRENT_PRODUCER = "3c3af3e002a8abb341db0bb4020f29491ed32208b0b0953ebb6ed2b289c4390c"
PIN = rb'ADMISSION_HELPER_SHA256: "([0-9a-f]{64})"'
NORMALIZED_PIN = b'ADMISSION_HELPER_SHA256: "<PINNED_ADMISSION_HELPER>"'
SUITE_QUERY = ("query($suite:ID!){node(id:$suite){... on CheckSuite{databaseId "
               "repository{databaseId nameWithOwner} app{databaseId slug} commit{oid} "
               "workflowRun{databaseId event runAttempt workflow{databaseId name} "
               "file{path repositoryName repositoryFileUrl}}}}}")

def producer_definition(revision):
    data = api(f"contents/.github/workflows/ci.yml?ref={revision}")
    assert data["encoding"] == "base64" and len(data["content"]) <= 180000
    raw = base64.b64decode(data["content"], validate=False)
    assert len(raw) <= 128000
    if hashlib.sha256(raw).hexdigest() in REVIEWED_PRODUCERS:
        return "full"
    pins = re.findall(PIN, raw)
    assert len(pins) == 1 and pins[0].decode() == ADMISSION_SCRIPT_SHA256, "Unknown admission helper pin"
    assert hashlib.sha256(re.sub(PIN, NORMALIZED_PIN, raw)).hexdigest() == CURRENT_PRODUCER, "Unknown source-admission producer"
    helper = api(f"contents/scripts/main-release-admission.py?ref={revision}")
    assert helper["encoding"] == "base64" and len(helper["content"]) <= 180000
    body = base64.b64decode(helper["content"], validate=False)
    assert len(body) <= 128000 and hashlib.sha256(body).hexdigest() == ADMISSION_SCRIPT_SHA256, "Admission helper bytes changed"
    return "admission"

def executed_definition(pre, revision):
    """Bind the real check suite to the immutable executed workflow definition."""
    assert type(pre["workflow_id"]) is int and pre["workflow_id"] > 0
    assert type(pre["check_suite_id"]) is int and pre["check_suite_id"] > 0
    assert re.fullmatch(r"[A-Za-z0-9_=-]{1,128}", pre["check_suite_node_id"])
    # Fixed, read-only GraphQL query authenticates the executed definition;
    # REST metadata/content/log requests remain GET-only. No new permissions.
    response = json.loads(subprocess.check_output(
        ["gh", "api", "--hostname", "github.com", "graphql", "-f", f"query={SUITE_QUERY}",
         "-f", f"suite={pre['check_suite_node_id']}"], text=True, timeout=30))
    assert not response.get("errors")
    suite = response["data"]["node"]
    assert suite["databaseId"] == pre["check_suite_id"]
    assert suite["repository"] == {"databaseId": pr["base"]["repo"]["id"], "nameWithOwner": repository}
    assert suite["app"]["databaseId"] == 15368 and suite["app"]["slug"] == "github-actions"
    assert suite["commit"]["oid"] == revision
    official = suite["workflowRun"]
    assert official["databaseId"] == pre["id"] and official["event"] == pre["event"]
    assert official["runAttempt"] == pre["run_attempt"]
    assert official["workflow"] == {"databaseId": pre["workflow_id"], "name": "CI"}
    assert official["file"] == {"path": ".github/workflows/ci.yml", "repositoryName": repository,
        "repositoryFileUrl": f"https://github.com/{repository}/blob/{revision}/.github/workflows/ci.yml"}
    return producer_definition(revision)


def snapshot_binding(pre, jobs):
    assert binds_pr(pre) or pre.get("pull_requests") == [], "Conflicting PR association"
    executed_definition(pre, head)
    job = next(item for item in jobs if item["name"] == "Validation Source")
    job_id, attempt = job["id"], job["run_attempt"]
    assert type(job_id) is int and job_id > 0
    assert type(attempt) is int and 1 <= attempt <= pre["run_attempt"]
    # latest is per job; authenticate its actual original attempt even when
    # another job was rerun later. Never pretend it ran in the latest attempt.
    # Authenticate each full gate's actual job attempt, including earlier green
    # jobs retained by a failed-only rerun. One request per distinct attempt.
    attempts = {}
    for gate in jobs:
        if gate["name"] not in FULL + PLATFORMS + ("Promotion Source",):
            continue
        actual = gate["run_attempt"]
        if actual not in attempts:
            attempts[actual] = pages(f"actions/runs/{pre['id']}/attempts/{actual}/jobs", "jobs")
        members = [item for item in attempts[actual] if item.get("id") == gate["id"]]
        assert len(members) == 1 and members[0] == gate, "Full job does not match its actual attempt"
    assert api(f"actions/jobs/{job_id}") == job, "Source job changed"
    assert job["head_sha"] == head and job["head_branch"] == pr["head"]["ref"]
    assert [step["name"] for step in job["steps"]] == [
        "Set up job", "Bind full validation to the actual event snapshot", "Complete job"]
    assert all(step["status"] == "completed" and step["conclusion"] == "success" for step in job["steps"])
    log = subprocess.check_output(
        ["gh", "api", "--hostname", "github.com", "--allow-escape-sequences",
         f"repos/{repository}/actions/jobs/{job_id}/logs"],
        timeout=30)
    assert len(log) <= 262144, "Source log exceeds bound"
    lines = log.decode("utf-8").splitlines()
    records = [line for line in lines if re.match(r"^\d{4}-\d\d-\d\dT\S+Z Event source:", line)]
    assert len(records) == 1, "Missing or duplicate source-admission record"
    match = re.fullmatch(r"\d{4}-\d\d-\d\dT\S+Z Event source: (" + SHA + r"); full validation: true", records[0])
    assert match, "Malformed or non-full source admission"
    snapshot = match.group(1)
    tested = api(f"git/commits/{snapshot}")
    assert tested["sha"] == snapshot
    assert [parent["sha"] for parent in tested["parents"]] == [base, head], "Tested merge has wrong head/base"
    assert tested["tree"]["sha"] == commit["tree"]["sha"], "Tested tree differs from normal merge"
    producer_definition(snapshot)
    return (pre["id"], pre["run_attempt"], pre["workflow_id"], pre["check_suite_id"], pre["check_suite_node_id"]), snapshot


light_main = False
if ci is not None:
    main_jobs = pages(f"actions/runs/{ci['id']}/jobs?filter=latest", "jobs")
    admissions = [job for job in main_jobs if job["name"] == "Main Release Admission"]
    if not admissions:
        # Preserve the historical publisher contract: a legacy main CI must
        # have genuinely completed the full suite. A skipped stub never counts.
        full_jobs(ci)
    else:
        light_main = True
        assert len(admissions) == 1 and executed_definition(ci, source) == "admission"
        for name in ("Validation Source", "Main Release Admission", "Test"):
            found = [job for job in main_jobs if job["name"] == name]
            assert len(found) == 1
            gate = found[0]
            assert gate["run_id"] == ci["id"]
            assert gate["head_sha"] == source and gate["head_branch"] == "main"
            assert type(gate["run_attempt"]) is int and 1 <= gate["run_attempt"] <= ci["run_attempt"]
            assert gate["status"] == "completed" and gate["conclusion"] == "success", name
            members = pages(f"actions/runs/{ci['id']}/attempts/{gate['run_attempt']}/jobs", "jobs")
            assert [item for item in members if item.get("id") == gate["id"]] == [gate]
        # A job-level false condition is evaluated before matrix expansion.
        # GitHub normally returns one skipped job with the literal matrix name;
        # accept a complete expanded skipped representation as well. Neither is
        # full acceptance: the separate pre-merge proof below requires real jobs.
        skipped = ["Quick Checks", "Promotion Source", "E2E Tests", "Lint", "MSRV (Rust 1.95)",
                   "Security Audit", "cargo-deny (licenses/bans/advisories/sources)", "Documentation"]
        for label, platforms in (("TLS Fixture", ("macos-latest", "windows-latest")),
                                 ("ToolEvent Recorder E2E", ("ubuntu-latest", "macos-latest", "windows-latest")),
                                 ("Build", ("ubuntu-latest", "macos-latest", "windows-latest"))):
            literal = label + " (${{ matrix.os }})"
            expanded = [f"{label} ({platform})" for platform in platforms]
            found = [job["name"] for job in main_jobs if job["name"] in [literal] + expanded]
            assert found == [literal] or (len(found) == len(expanded) and set(found) == set(expanded)), label
            skipped.extend(found)
        for name in skipped:
            found = [job for job in main_jobs if job["name"] == name]
            assert len(found) == 1 and found[0]["status"] == "completed" and found[0]["conclusion"] == "skipped", name
        assert len(main_jobs) == len(skipped) + 3, "Unexpected main CI jobs"
else:
    assert producer_definition(source) == "admission"

def latest_premerge():
    runs = pages(f"actions/workflows/ci.yml/runs?event=pull_request&head_sha={head}", "workflow_runs")
    bound = [candidate for candidate in runs if candidate.get("head_sha") == head
             and (binds_pr(candidate) or candidate.get("pull_requests") == [])]
    assert bound, "Missing exact pre-merge release CI head/base evidence"
    # Never skip a newer failed exact-input run to select an older green result.
    return max(bound, key=lambda item: item["id"])


pre = latest_premerge()
pre = api(f"actions/runs/{pre['id']}")
ci_identity(pre, "pull_request", head, pr["head"]["ref"])
jobs = full_jobs(pre, promotion=True, before=instant(pr["merged_at"]))
binding, tested_snapshot = snapshot_binding(pre, jobs)

current_dev = branch("dev", protected=True)
if current_dev != dev:
    ancestor(dev, current_dev)
fresh_pre = api(f"actions/runs/{pre['id']}")
ci_identity(fresh_pre, "pull_request", head, pr["head"]["ref"])
assert fresh_pre["run_attempt"] == pre["run_attempt"], "Release CI changed during proof"
assert fresh_pre.get("pull_requests") == [] or binds_pr(fresh_pre)
assert tuple(fresh_pre[key] for key in ("id", "run_attempt", "workflow_id", "check_suite_id", "check_suite_node_id")) == binding
assert latest_premerge()["id"] == pre["id"], "A newer exact-input release CI appeared during proof"
if ci is not None:
    fresh_ci = api(f"actions/runs/{ci['id']}")
    ci_identity(fresh_ci, "push", source, "main")
    for key in ("id", "run_attempt", "name", "event", "head_sha", "head_branch", "conclusion"):
        assert fresh_ci[key] == run[key], "Main CI changed during proof"
assert branch("main") == source, "Main changed while authorizing publication"
print(f"Accepted normal main merge source={source} tree={commit['tree']['sha']} PR={number} "
      f"cut={cut} reviewed_release_head={head} mode={mode} "
      f"main_CI={ci['id'] if ci else 'admitting'} main_attempt={ci['run_attempt'] if ci else 'running'} "
      f"release_CI={pre['id']} release_attempt={pre['run_attempt']} tested_snapshot={tested_snapshot} controller={workflow_source}")
print("Authenticated full pre-merge job attempts: " + ", ".join(
    f"{job['name']}={job['id']}@{job['run_attempt']}" for job in jobs
    if job["name"] in FULL + PLATFORMS + ("Promotion Source",)))
print(f"Acceptance covers tested inputs; final executable must be rebuilt for source={source} and its release version")
with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
    stream.write(f"revision={source}\n")
    if light_main:
        # This record is carried into the existing signed publication receipts.
        # Keep main publication admission and real pre-merge execution separate.
        # Legacy full-main reservations retain their original signed shape.
        main_run = {key: ci[key] for key in ("id", "run_attempt", "name", "event", "head_sha", "head_branch", "status", "conclusion")}
        main_run.update(repository={"full_name": repository}, head_repository={"full_name": repository},
                        html_url=f"https://github.com/{repository}/actions/runs/{ci['id']}")
        validation = {"schemaVersion": 1, "kind": "bamboo-main-release-admission/v1", "repository": repository,
                      "sourceRevision": source, "sourceTree": commit["tree"]["sha"], "releasePr": number,
                      "cutRevision": cut, "releaseHead": head, "baseRevision": base, "ciRun": main_run,
                      "premerge": {"runId": pre["id"], "runAttempt": pre["run_attempt"],
                                   "workflowId": pre["workflow_id"], "checkSuiteId": pre["check_suite_id"],
                                   "snapshotRevision": tested_snapshot,
                                   "url": f"https://github.com/{repository}/actions/runs/{pre['id']}"}}
        stream.write("validation=" + json.dumps(validation, sort_keys=True, separators=(",", ":")) + "\n")
