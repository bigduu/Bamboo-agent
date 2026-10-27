"use strict";
// Inert fixtures only: no Docker, Cargo, source binary or browser execution.
const test = require("node:test"), assert = require("node:assert/strict"), fs = require("node:fs"), path = require("node:path"), vm = require("node:vm");
const { spawnSync } = require("node:child_process"), os = require("node:os");
const arm = require("./docker-arm-original.cjs"), consumer = require("./docker-from-release.cjs"), core = require("./crate-release.cjs");
const ROOT = path.resolve(__dirname, ".."), text = file => fs.readFileSync(path.join(ROOT, file), "utf8");
const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "bamboo2353-unit-")), previousTemp = process.env.RUNNER_TEMP;
test.describe("ARM original owner", { concurrency: false }, () => {
test.before(() => { process.env.RUNNER_TEMP = temporary; });
test.after(() => { if (previousTemp === undefined) delete process.env.RUNNER_TEMP; else process.env.RUNNER_TEMP = previousTemp; });
const sha = bytes => core.sha256(bytes), at = n => "2026-10-27T00:00:" + String(n).padStart(2,"0") + "Z";
function job(id = 101, attempt = 1, reuse = false) {
  const names = [arm.SELECT, ...arm.PRODUCTION, arm.REUSE], offset = reuse ? 30 : 0;
  return { id, run_id: 50, run_attempt: attempt, head_sha: "e".repeat(40), head_branch: "main", name: consumer.ARM_JOB,
    status: "completed", conclusion: "success", started_at: at(offset), completed_at: at(offset + 20),
    steps: names.map((name,i) => ({ name, number: i + 1, status: "completed", conclusion: (reuse ? [arm.SELECT,arm.REUSE].includes(name) : name !== arm.REUSE) ? "success" : "skipped",
      started_at: (reuse ? [arm.SELECT,arm.REUSE].includes(name) : name !== arm.REUSE) ? at(offset + i + 1) : null,
      completed_at: (reuse ? [arm.SELECT,arm.REUSE].includes(name) : name !== arm.REUSE) ? at(offset + i + 2) : null })) };
}
function zip(rows) {
  const code = `import base64,io,json,stat,sys,zipfile
out=io.BytesIO()
with zipfile.ZipFile(out,'w',compression=zipfile.ZIP_DEFLATED) as z:
 for row in json.load(sys.stdin):
  info=zipfile.ZipInfo(row['name']); info.compress_type=zipfile.ZIP_DEFLATED; info.external_attr=(stat.S_IFLNK if row.get('link') else stat.S_IFREG)<<16
  z.writestr(info,base64.b64decode(row['bytes']))
sys.stdout.buffer.write(out.getvalue())`;
  const result = spawnSync("python3", ["-I","-c",code], { input: JSON.stringify(rows), env: arm.childEnv(temporary), timeout: 10000 });
  assert.equal(result.status,0); return result.stdout;
}
function fixture() {
  const facts = { source: { revision: "a".repeat(40), tree: "f".repeat(40) }, release: { id: 30, version: "2026.10.27" },
    controller: { runId: 10, revision: "b".repeat(40) }, completionSha256: "c".repeat(64),
    frontend: [{ name: "frontend-manifest.json", assetId: 7, sha256: "1".repeat(64), sizeBytes: 254 }, { name: "lotus-frontend.zip", assetId: 8, sha256: "2".repeat(64), sizeBytes: 4096 }] };
  const state = { controller: "d".repeat(40), input: { releaseRun: 10 }, facts, reads: {} };
  const run = { id: 50, run_attempt: 2, workflow_id: 70, head_sha: "e".repeat(40), head_branch: "main", created_at: at(0) }, physical = job();
  const context = { run, jobs: [physical] }, binary = Buffer.alloc(65536); binary.write("7f454c460201",0,"hex"); binary.writeUInt16LE(3,16); binary.writeUInt16LE(183,18);
  const receipt = { schemaVersion: 1, kind: arm.KIND, identity: arm.binding(state,context,physical),
    build: { command: ["cargo",...arm.ARGS], target: arm.TARGET, features: [], profile: "release", frontendMode: "embedded", builder: arm.BUILDER, builderConfig: arm.CONFIG,
      rustc: "rustc 1.99.1 (fixture)\nrelease: 1.99.1\nhost: aarch64-unknown-linux-gnu", cargo: "cargo 1.99.1 (fixture)", versionOutput: "bamboo 2026.10.27",
      sourceLockSha256: "3".repeat(64), stampedLockSha256: "4".repeat(64), stampedDiffSha256: "5".repeat(64), stampedPaths: ["Cargo.lock","Cargo.toml"], metadataSha256: "6".repeat(64), cargoMessagesSha256: "7".repeat(64) },
    binary: { sha256: sha(binary), sizeBytes: binary.length }, abi: { needed: ["libc.so.6","libssl.so.3"], symbolVersions: ["GLIBC_2.36"], interpreter: "/lib/ld-linux-aarch64.so.1" } };
  const f = { state,context,run,physical,binary,receipt,readJobs: [], byteCalls: 0, artifacts: [] };
  f.pack = () => {
    f.receiptBytes = Buffer.from(JSON.stringify(receipt,null,2) + "\n");
    f.rows = [{ name: "bamboo", bytes: f.binary.toString("base64") }, { name: "receipt.json", bytes: f.receiptBytes.toString("base64") }];
    f.raw = zip(f.rows); f.artifact = { id: 900, name: arm.artifactName(50,1), expired: false, size_in_bytes: f.raw.length, digest: "sha256:" + sha(f.raw), created_at: at(5), workflow_run: { id: 50, head_sha: run.head_sha, head_branch: "main" } }; f.artifacts = [f.artifact];
    const value = arm.completion(state,context,physical,f.artifact,{ receipt,receiptSha256: sha(f.receiptBytes) });
    f.log = at(5) + ` Artifact ${f.artifact.name} has been successfully uploaded! Final size is ${f.artifact.size_in_bytes} bytes. Artifact ID is 900\n` + at(5) + " Artifact download URL: https://github.com/bigduu/Bamboo-agent/actions/runs/50/artifacts/900\n" +
      at(6) + " " + arm.PREFIX + Buffer.from(core.canonicalJson(value)).toString("base64");
  }; f.pack();
  context.get = async route => {
    if (route === "actions/jobs/101") return physical;
    if (route === "actions/artifacts/900") return f.artifact;
    if (route === "actions/runs/10") return { id: 10,created_at: at(0) };
    throw Error("Unexpected fixed read " + route);
  };
  context.paged = async route => route.includes("/artifacts") ? f.artifacts : route.includes("/attempts/") ? [physical] : route.includes("/runs?") ? [] : assert.fail(route);
  state.reads.jobLog = async id => { f.readJobs.push(id); assert.equal(id,101); return f.log; };
  state.reads.artifactBytes = async (id,maximum) => { assert.equal(id,900); assert.equal(maximum,f.artifact.size_in_bytes); f.byteCalls++; return f.raw; };
  return f;
}
test("ARM immutable builder and command preserve a single additional default release context", () => {
  assert.equal(arm.BUILDER,"docker.io/library/rust:1.99.1-bookworm@sha256:32f7944ed00949c44d3944a0a852b3e5ea862a610a38b43ac92a33aa87256b75");
  assert.deepEqual(arm.ARGS,["build","--locked","--offline","--release","-p","bamboo-agent","--bin","bamboo","--target","aarch64-unknown-linux-gnu","--message-format=json-render-diagnostics"]);
  const code = text("scripts/docker-arm-original.cjs");
  assert.equal((code.match(/containerArgs\(work, "compile", \["cargo", \.\.\.ARGS\]\)/g)||[]).length,1);
  assert.doesNotMatch(code,/SIGNING_KEY|makeFrozenSourceContext|createLifecycle|npm|vite|cargo-chef|--all-features|--workspace|GITHUB_SHA\s*=/);
  assert.ok(code.includes('await consumer.authenticated()')); assert.ok(code.includes('await consumer.refreshPublication(state)'));
});
test("ARM builder mounts only owned scratch and compiles offline without controller credentials", () => {
  const work = Object.fromEntries(["source","target","cargo","home"].map(name => [name,path.join(temporary,name)]));
  const args = arm.containerArgs(work,"compile",["cargo",...arm.ARGS]), inspect = arm.containerArgs(work,"inspect",["bamboo","--version"]);
  assert.equal(args[args.indexOf("--network") + 1],"none"); assert.ok(args.includes("linux/arm64"));
  assert.ok(args.includes(`type=bind,source=${work.source},target=${work.source},readonly`));
  assert.ok(inspect.includes(`type=bind,source=${work.target},target=${work.target},readonly`));
  const online=arm.containerArgs(work,"prepare",["cargo","fetch"]); assert.equal(online[online.indexOf("--network")+1],"bridge");
  for (const name of ["GH_TOKEN","GITHUB_TOKEN","ACTIONS_RUNTIME_TOKEN","CARGO_REGISTRY_TOKEN","BAMBOO_API_KEY","CLOUDFLARE_TUNNEL_TOKEN","RUSTFLAGS","NODE_OPTIONS"]) assert.equal(Object.hasOwn(arm.childEnv(temporary),name),false);
  assert.doesNotMatch(args.join(" "),/docker\.sock|--privileged|--env-file|GH_TOKEN|GITHUB_OUTPUT/);
});
test("ARM frontend inputs accept only the two authenticated exact bounded roles", () => {
  const f = fixture(); assert.equal(arm.frontend(f.state.facts).length,2);
  for (const [index,maximum] of [[0,1024**2],[1,16*1024**2]]) {
    const facts = structuredClone(f.state.facts); facts.frontend[index].sizeBytes=maximum; arm.frontend(facts);
    facts.frontend[index].sizeBytes++; assert.throws(()=>arm.frontend(facts));
  }
  for (const change of [facts=>facts.frontend.reverse(),facts=>facts.frontend.pop(),facts=>facts.frontend[0].assetId=0]) { const facts=structuredClone(f.state.facts); change(facts); assert.throws(()=>arm.frontend(facts)); }
});
test("ARM history reuses copied physical success and a real read-only full-rerun selection", () => {
  const first=job(), copied=structuredClone(first); copied.id=202; copied.run_attempt=2;
  const run={id:50,run_attempt:2,head_sha:first.head_sha,head_branch:"main"};
  for (const latest of [copied,job(202,2,true)]) assert.equal(arm.history([first,latest],run,3,true).physical.id,101);
  const failed=job(101,1,true); failed.conclusion="failure"; failed.steps.forEach(s=>{s.conclusion="skipped";s.started_at=null;s.completed_at=null;});
  assert.equal(arm.history([failed],run,2).physical,undefined);
});
for (const variant of ["compile-failed","compile-cancelled","active","failed-owner","upload-failed","seal-failed","missing-step","two-physical","duplicate-attempt","foreign-header","fake-reuse","lost-original"]) {
  test("ARM history stops " + variant, () => {
    const first=job(),run={id:50,run_attempt:2,head_sha:first.head_sha,head_branch:"main"}; let rows=[first];
    if(variant==="compile-failed") {first.steps.find(s=>s.name===arm.COMPILE).conclusion="failure";first.conclusion="failure";}
    if(variant==="compile-cancelled") {first.steps.find(s=>s.name===arm.COMPILE).conclusion="cancelled";first.conclusion="cancelled";}
    if(variant==="active") first.status="in_progress";
    if(variant==="failed-owner") first.conclusion="failure";
    if(variant==="upload-failed") first.steps.find(s=>s.name===arm.UPLOAD).conclusion="failure";
    if(variant==="seal-failed") first.steps.find(s=>s.name===arm.SEAL).conclusion="failure";
    if(variant==="missing-step") first.steps.pop();
    if(variant==="two-physical") { const second=job(202,2);second.started_at=at(30);second.completed_at=at(50);second.steps.forEach(s=>{if(s.started_at){s.started_at=at(31);s.completed_at=at(32);}});rows.push(second); }
    if(variant==="duplicate-attempt") rows.push({...first,id:202});
    if(variant==="foreign-header") first.head_sha="f".repeat(40);
    if(variant==="fake-reuse") {const second=job(202,2,true);second.steps.find(s=>s.name===arm.REUSE).conclusion="skipped";rows.push(second);}
    if(variant==="lost-original") rows=[job(202,2,true)];
    assert.throws(()=>arm.history(rows,run,3,true));
  });
}
test("ARM original read uses the original physical job, official upload and exact raw ZIP", async () => {
  const f=fixture(),copy=structuredClone(f.physical);copy.id=202;copy.run_attempt=2;f.context.jobs.push(copy);
  const result=await arm.readOriginal(f.state,f.context);
  assert.deepEqual(fs.readFileSync(result.binaryFile),f.binary);assert.deepEqual(result.receipt,f.receipt);
  assert.deepEqual(f.readJobs,[101]);assert.equal(f.byteCalls,1);assert.equal(result.reference.artifactId,900);
  assert.equal(result.reference.jobId,101);assert.notEqual(f.run.head_sha,f.state.controller);
});
for(const variant of ["expired","missing","orphan","foreign-run","wrong-metadata-head","outside-upload","log-id","log-url","duplicate-upload","missing-frame","duplicate-frame","late-frame","bytes","source","version","builder","frontend","physical","binary","architecture","ABI","fresh-artifact","fresh-job","original-attempt"]) {
  test("ARM original reader rejects " + variant,async()=>{
    const f=fixture();
    if(variant==="expired") f.artifact.expired=true;
    if(variant==="missing") f.artifacts=[];
    if(variant==="orphan") f.artifacts.push({...f.artifact,id:901,name:arm.artifactName(50,2)});
    if(variant==="foreign-run") f.artifact.workflow_run.id=51;
    if(variant==="wrong-metadata-head") f.artifact.workflow_run.head_sha=f.state.controller;
    if(variant==="outside-upload") f.artifact.created_at=at(0);
    if(variant==="log-id") f.log=f.log.replace("Artifact ID is 900","Artifact ID is 901");
    if(variant==="log-url") f.log=f.log.replace("/artifacts/900","/artifacts/901");
    if(variant==="duplicate-upload") f.log=f.log+"\n"+f.log.split("\n")[0];
    if(variant==="missing-frame") f.log=f.log.split("\n").slice(0,2).join("\n");
    if(variant==="duplicate-frame") f.log=f.log+"\n"+f.log.split("\n").at(-1);
    if(variant==="late-frame") f.log=f.log.replace(at(6)+" "+arm.PREFIX,at(20)+" "+arm.PREFIX);
    if(variant==="bytes") f.raw=Buffer.from("tampered");
    if(variant==="source") {f.receipt.identity.source="f".repeat(40);f.pack();}
    if(variant==="version") {f.receipt.identity.standard.version="2026.10.28";f.pack();}
    if(variant==="builder") {f.receipt.build.builder="rust:latest";f.pack();}
    if(variant==="frontend") {f.receipt.identity.frontend[1].sha256="0".repeat(64);f.pack();}
    if(variant==="physical") {f.receipt.identity.physicalAttempt=2;f.pack();}
    if(variant==="binary") {f.receipt.binary.sha256="0".repeat(64);f.pack();}
    if(variant==="architecture") {f.binary.writeUInt16LE(62,18);f.receipt.binary.sha256=sha(f.binary);f.pack();}
    if(variant==="ABI") {f.receipt.abi.symbolVersions=["GLIBC_2.39"];f.pack();}
    if(variant==="fresh-artifact") {const get=f.context.get;f.context.get=async route=>route==="actions/artifacts/900"?{...f.artifact,expired:true}:get(route);}
    if(variant==="fresh-job") {let n=0;const get=f.context.get;f.context.get=async route=>route==="actions/jobs/101"&&++n===2?{...f.physical,run_attempt:2}:get(route);}
    if(variant==="original-attempt") {const paged=f.context.paged;f.context.paged=async route=>route.includes("/attempts/")?[{...f.physical,run_attempt:2}]:paged(route);}
    await assert.rejects(arm.readOriginal(f.state,f.context));
  });
}
for(const variant of ["extra","duplicate","traversal","symlink","missing","oversize-receipt","short-binary","noncanonical","truncated"]) {
  test("ARM strict two-file ZIP rejects " + variant,()=>{
    const f=fixture(), rows=f.rows.map(row=>({...row}));
    if(variant==="extra") rows.push({name:"extra",bytes:"eA=="});
    if(variant==="duplicate") rows[1]={...rows[0]};
    if(variant==="traversal") rows[0].name="../bamboo";
    if(variant==="symlink") rows[0].link=true;
    if(variant==="missing") rows.pop();
    if(variant==="oversize-receipt") rows[1].bytes=Buffer.alloc(arm.MAX_RECEIPT+1,32).toString("base64");
    if(variant==="short-binary") rows[0].bytes=Buffer.alloc(65535).toString("base64");
    if(variant==="noncanonical") rows[1].bytes=Buffer.from(JSON.stringify(f.receipt)).toString("base64");
    let raw=zip(rows);if(variant==="truncated")raw=raw.subarray(0,raw.length-20);
    assert.throws(()=>arm.decodeOriginal(raw,path.join(fs.mkdtempSync(path.join(temporary,"bad-")),"bamboo")));
  });
}
test("ARM orphan guard and archive caps cannot authorize a replacement compilation",async()=>{
  const f=fixture();await assert.rejects(arm.noOrphan(f.context),/Orphan/);
  f.artifacts=[];await arm.noOrphan(f.context);
  assert.equal(arm.MAX_ZIP,1024**3);assert.ok(arm.MAX_BINARY+arm.MAX_RECEIPT<arm.MAX_ZIP);
  assert.throws(()=>arm.artifactName(0,1));assert.throws(()=>arm.artifactName(50,"2\nevil"));
});
function inertCli(f,phase,variant) {
  const directory=fs.mkdtempSync(path.join(temporary,"cli-")),work=path.join(directory,"bamboo-arm-original");
  fs.mkdirSync(work);for(const name of ["source","target","cargo","home","out"])fs.mkdirSync(path.join(work,name));
  f.run.run_attempt=1; f.context.current=f.physical;f.context.jobs=[f.physical];
  f.physical.status="in_progress";f.physical.conclusion=null;
  const active=phase==="seal"?arm.SEAL:arm.COMPILE;
  f.physical.steps.find(s=>s.name===active).status="in_progress";f.physical.steps.find(s=>s.name===active).conclusion=null;
  const metadata=Buffer.from('{"packages":[],"workspace_members":[]}');fs.writeFileSync(path.join(work,"metadata.json"),metadata);
  const rawDiff="fixture stamp diff", prepared={identity:arm.binding(f.state,f.context,f.physical),manifests:["Cargo.toml"],metadataSha256:sha(metadata),stamped:{paths:["Cargo.lock","Cargo.toml"],diffSha256:sha(rawDiff)}};
  fs.writeFileSync(path.join(work,"prepared.json"),JSON.stringify(prepared));
  if(phase==="seal") {
    f.pack();f.log=f.log.split("\n").slice(0,2).join("\n");
    fs.writeFileSync(path.join(work,"out/bamboo"),f.binary);fs.writeFileSync(path.join(work,"out/receipt.json"),f.receiptBytes);
  } else f.artifacts=[];
  if(variant==="prior-output")fs.mkdirSync(path.join(work,"target/release"));
  const calls={compile:0,refresh:0,frames:[],factory:0};
  const fakeConsumer={...consumer,authenticated:async(...args)=>{assert.equal(args.length,0);calls.factory++;return f.state;},
    consumerExecution:async(state,role)=>{assert.equal(state,f.state);assert.equal(role,"arm-originals");return f.context;},
    refreshPublication:async(state)=>{assert.equal(state,f.state);calls.refresh++;if(variant==="changed-source")throw Error("Fresh publication changed");},
    freshConsumer:async(context,name)=>{assert.equal(context,f.context);assert.equal(name,active);}};
  const instance=new (require("node:module"))(path.join(__dirname,"docker-arm-original.cjs"),module);
  instance.filename=path.join(__dirname,"docker-arm-original.cjs");instance.paths=module.paths;
  instance.require=name=>{
    if(name==="./docker-from-release.cjs")return fakeConsumer;
    if(name==="./sidecar-build.cjs")return {verifyStampedVersions:(value,version,options)=>{assert.equal(version,"2026.10.27");assert.equal(options.root,path.join(work,"source"));return{id:"server-fixture"};}};
    if(name==="node:child_process")return{spawnSync:(program,args,options)=>{
      if(program==="python3"){assert.deepEqual(args.slice(0,2),["-I","-c"]);return spawnSync(program,args,options);} // tiny ZIP only
      if(program==="git") {
        const value=args.at(-1)==="HEAD"?f.state.facts.source.revision:args.at(-1)==="HEAD^{tree}"?f.state.facts.source.tree:
          args.includes("--binary")?rawDiff:args.includes("--cached")||args.includes("--others")?"":"Cargo.lock\nCargo.toml";
        return{status:0,stdout:options.encoding===null?Buffer.from(value):value};
      }
      assert.equal(program,"docker");assert.ok(args.includes(arm.BUILDER));assert.equal(args[args.indexOf("--network")+1],"none");
      assert.ok(args.includes("build"));assert.ok(args.includes("--locked"));assert.ok(args.includes("--offline"));calls.compile++;
      for(const name of ["GH_TOKEN","GITHUB_OUTPUT","CARGO_REGISTRY_TOKEN"])assert.equal(Object.hasOwn(options.env,name),false);
      return{status:0,stdout:Buffer.alloc(0)};
    }};
    if(name==="node:process")assert.fail("No hidden process import");return require(name);
  };
  const environment={PATH:process.env.PATH,RUNNER_TEMP:directory,ARM_ARTIFACT_ID:"900",ARM_ARTIFACT_DIGEST:f.artifact.digest.slice(7),GITHUB_OUTPUT:path.join(directory,"outputs")};
  const fakeProcess={argv:["node",instance.filename,phase],env:environment,execPath:process.execPath,getuid:()=>1001,getgid:()=>1001};
  // Evaluate the actual module in this realm, adding only a fixture entrypoint;
  // the injected process and inert ports are never production CLI options.
  instance.fixtureProcess=fakeProcess;instance.fixtureConsole={log:line=>calls.frames.push(line),error:()=>{}};
  instance._compile('"use strict";const process=module.fixtureProcess,console=module.fixtureConsole;\n'+text("scripts/docker-arm-original.cjs").replace(/^#![^\n]*\n/,"")+"\nmodule.exports.fixtureMain=main;",instance.filename);
  return {calls,run:()=>instance.exports.fixtureMain()};
}
test("ARM actual compile CLI calls the sole isolated compiler once after fresh proof",async()=>{
  const f=fixture(),cli=inertCli(f,"compile");await cli.run();
  assert.equal(cli.calls.compile,1);assert.equal(cli.calls.refresh,1);assert.equal(cli.calls.factory,1);assert.deepEqual(cli.calls.frames,[]);
});
for(const variant of ["prior-output","changed-source"])test("ARM actual compile CLI stops "+variant+" without compiling",async()=>{
  const cli=inertCli(fixture(),"compile",variant);await assert.rejects(cli.run());assert.equal(cli.calls.compile,0);
});
test("ARM actual seal CLI authenticates its uploaded original without compiling or replacing it",async()=>{
  const f=fixture(),cli=inertCli(f,"seal");f.state.reads.jobLog=async()=>assert.fail("Official in-progress job logs are unavailable");await cli.run();
  assert.equal(cli.calls.compile,0);assert.equal(cli.calls.refresh,1);assert.equal(cli.calls.frames.length,1);
  assert.ok(cli.calls.frames[0].startsWith(arm.PREFIX));assert.deepEqual(f.readJobs,[]);
});
test("ARM consumer stage copies the authenticated ARM file instead of seeking a fourth Release asset",async()=>{
  const temporaryStage=fs.mkdtempSync(path.join(temporary,"stage-")),binaryFile=path.join(temporaryStage,"original"),binary=Buffer.from("inert original ARM bytes");fs.writeFileSync(binaryFile,binary);
  let originalCalls=0,armReads=0;
  const facts={source:{revision:"a".repeat(40)},release:{version:"2026.10.27"},controller:{runId:10,revision:"b".repeat(40)},native:{assets:[{name:"bamboo-browser-host-v1.tar.gz"},{name:"bamboo-sidecars-v2.json"}]}};
  const state={facts,controller:"d".repeat(40)},execution={current:{steps:[{name:consumer.SELECT,status:"completed",conclusion:"success"}]},jobs:[],run:{run_attempt:1}};
  const context={module:{exports:{}},__dirname,Buffer,console,process:{env:{RUNNER_TEMP:temporaryStage,DOCKER_PLATFORM:"linux/arm64",GITHUB_RUN_ID:"50",GITHUB_RUN_ATTEMPT:"1"}},
    require:name=>name==="./docker-arm-original.cjs"?{readOriginal:async(s,e)=>{assert.equal(s,state);assert.equal(e,execution);armReads++;return{binaryFile,reference:{kind:arm.KIND,artifactId:900,sha256:sha(binary),sizeBytes:binary.length}};}}:
      name==="node:child_process"?{spawnSync:program=>{assert.equal(program,"readelf");return{status:0,stdout:"Class: ELF64\nMachine: AArch64\n(NEEDED) [libc.so.6]\nGLIBC_2.36"};}}:require(name)};
  vm.runInNewContext(text("scripts/docker-from-release.cjs"),context);
  Object.assign(context,{authenticated:async()=>state,consumerExecution:async()=>execution,imageHistory:()=>({}),noPriorCarrier:async()=>{},
    original:async(reads,ref)=>{originalCalls++;assert.ok(!ref.name.startsWith("bamboo-aarch64"));return ref.name.includes("sidecars")?Buffer.from(JSON.stringify({sourceRevision:facts.source.revision,version:facts.release.version,browserHost:{files:[]}})):Buffer.alloc(0);},
    hostFiles:()=>({"browser-runtime/host.cjs":Buffer.from("// inert"),"browser-runtime/package.json":Buffer.from("{}"),"browser-runtime/package-lock.json":Buffer.from("{}")}),output:()=>{}});
  await context.stage();assert.equal(armReads,1);assert.equal(originalCalls,2);
  assert.deepEqual(fs.readFileSync(path.join(temporaryStage,"bamboo-docker-context/bamboo")),binary);
});
test("ARM workflow has one native producer and required retained-original dependency without added writer authority",()=>{
  const workflow=text(".github/workflows/docker-publish.yml"),lane=workflow.split("\n  arm-originals:\n")[1].split("\n  image:\n")[0];
  assert.match(lane,/runs-on: ubuntu-24.04-arm/);assert.doesNotMatch(lane,/packages: write|contents: write|SIGNING|secret|docker\/login|cargo|npm|vite|download-artifact/);
  for(const name of [arm.SELECT,...arm.PRODUCTION,arm.REUSE])assert.equal(lane.split("- name: "+name+"\n").length,2,name);
  for(const phase of ["select","prepare","compile","capture","seal","reuse"])assert.equal(lane.split("node scripts/docker-arm-original.cjs "+phase+"\n").length,2,phase);
  assert.match(workflow,/image:\n    name: Repackage originals[^\n]+\n    needs: \[source, arm-originals\]/);
  assert.match(lane,/retention-days: 90/);assert.match(lane,/name: docker-arm-original-\$\{\{ github.run_id \}\}-\$\{\{ github.run_attempt \}\}/);
  assert.match(lane,/out\/bamboo\n.*out\/receipt.json/);assert.doesNotMatch(lane,/overwrite:|\*|merge-multiple/);
  assert.equal((text("scripts/ci-policy.test.cjs").match(/^require\("\.\/docker-arm-original.test.cjs"\)/gm)||[]).length,1);
  const source=text("scripts/docker-arm-original.cjs");assert.ok(source.indexOf('stamper, "stamp"')<source.indexOf('"cargo", "metadata"'));
  assert.ok(source.includes('path.join(ROOT, "scripts/frontend-package.cjs"), "verify", "--source-root", work.source'));
  assert.doesNotMatch(source,/preparedFrontend\(|workflowIdentity\(|sourceState\(allowed\)|\.binaryArchitecture\(/);
});
test("ARM stamped-manifest snapshot rejects escapes, symlinks and observes exact content drift",()=>{
  const directory=fs.mkdtempSync(path.join(temporary,"manifests-")),work={source:directory},file=path.join(directory,"Cargo.toml");fs.writeFileSync(file,'[package]\nname="fixture"\nversion="1.0.0"\n');
  const first=arm.manifestHashes(work,["Cargo.toml"]);assert.deepEqual(Object.keys(first),["Cargo.toml"]);
  fs.appendFileSync(file,'edition="2021"\n');assert.notDeepEqual(arm.manifestHashes(work,["Cargo.toml"]),first);
  assert.throws(()=>arm.manifestHashes(work,["../Cargo.toml"]));
  fs.renameSync(file,file+".saved");fs.symlinkSync(file+".saved",file);assert.throws(()=>arm.manifestHashes(work,["Cargo.toml"]));
});
test("ARM stamper glob validates ordinary directories before any Python manifest access",()=>{
  const directory=fs.mkdtempSync(path.join(temporary,"glob-")),work={source:directory};
  fs.mkdirSync(path.join(directory,"crates/infra/good"),{recursive:true});
  for(const file of ["Cargo.toml","crates/infra/good/Cargo.toml"])fs.writeFileSync(path.join(directory,file),'[package]\nname="fixture"\n');
  assert.deepEqual(arm.stampInputs(work),["Cargo.toml","crates/infra/good/Cargo.toml"]);
  const outside=fs.mkdtempSync(path.join(temporary,"outside-"));fs.writeFileSync(path.join(outside,"Cargo.toml"),'[workspace.package]\nversion="0.0.0"\n');
  fs.symlinkSync(outside,path.join(directory,"crates/infra/foreign"));assert.throws(()=>arm.stampInputs(work),/link/);
});
test("ARM builder config must match the actual fixed image and native architecture",()=>{
  const image={Id:arm.CONFIG,Os:"linux",Architecture:"arm64",Config:{Env:["RUST_VERSION=1.99.1"]}};arm.checkBuilder(image);
  for(const field of ["Id","Os","Architecture","Config"]) {
    const bad={...image,[field]:field==="Config"?{Env:["RUST_VERSION=1.99.0"]}:"foreign"};assert.throws(()=>arm.checkBuilder(bad));
  }
});
test("ARM real Docker29 manifest-store shape labels observed identity separately from pinned config",()=>{
  // Captured selected fields on local OrbStack29.8.2, 2026-10-27. The original
  // once-only pull passed; the old config-ID assertion failed before any run.
  const manifest="sha256:32f7944ed00949c44d3944a0a852b3e5ea862a610a38b43ac92a33aa87256b75";
  const descriptor={digest:manifest,mediaType:"application/vnd.oci.image.manifest.v1+json",size:1942};
  const observed={Id:manifest,RepoDigests:["rust@"+manifest],Descriptor:descriptor,Os:"linux",Architecture:"arm64",Config:{Env:["RUST_VERSION=1.99.1"]}};
  assert.deepEqual(arm.checkBuilder(observed),{role:"manifest",id:manifest,repositoryDigest:"rust@"+manifest,descriptor});
});
});
