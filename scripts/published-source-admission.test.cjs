"use strict";
// Inert transport/byte fixtures. No fixture workflow or archive was published.
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createHash } = require("node:crypto");
const source = require("./frozen-source-admission.cjs");
const core = require("./crate-release.cjs");
const native = require("./native-originals.cjs");
const sidecar = require("./sidecar-release.cjs");
const proof = require("./published-source-admission.cjs");
const { frozenSourceFixture } = require("./tests/source-admission-fixture.cjs");
const fixtures = require("./tests/frozen-source-admission-fixtures.json");
const { S, C, anchor } = fixtures.identities;
const hash = core.sha256, clone = value => structuredClone(value);
const oid = value => createHash("sha1").update("blob " + Buffer.byteLength(value) + "\0").update(value).digest("hex");
const clock = seconds => new Date(Date.parse("2026-10-27T02:00:00Z") + seconds * 1000).toISOString();
const steps = {
  automatic: ["Authorize the frozen accepted dev source", "Reserve or resume the exact release identity", "Prepare and verify original native outputs"],
  "Publish and verify the crate closure": ["Publish verified original crates"],
  "Retain original native outputs": ["Retain and verify original native outputs"],
  ...Object.fromEntries(sidecar.TARGETS.map(target => [native.jobName(target), ["Select original native output"]])),
  "Publish and verify complete executable assets": ["Verify all original outputs and publish whole release"],
};
async function fixture() {
  const f = frozenSourceFixture(), originalReads = f.reads, data = new Map(), version = "2026.10.27", calls = [];
  const facts = await source.verifyAcceptedDevFacts({ sourceRevision:S, featurePr:f.input.featurePr, ciRun:f.input.ciRun }, f.reads, f.policy);
  const stable = clone(facts); delete stable.source.devObservedBefore; delete stable.source.devObservedAfter;
  stable.kind = "bamboo-frozen-source-context/v2"; stable.source.releaseRef = "release/" + S; stable.source.mainHistoryAnchor = anchor;
  stable.controller = { revision:C, workflowPath:source.CONTROLLER, event:"workflow_dispatch", ref:"refs/heads/main" };
  const marker = core.sourceDispatchContract(stable, "900");
  const workflowText = "name: Publish Crate\n# inert contract fixture; never executed\n";
  f.files[C] = Object.fromEntries(proof.CONTROLLER_HELPERS.map(name => [name, "// inert controller helper " + name + "\n"]));
  f.files[C][source.CONTROLLER] = workflowText;
  const helpers = Object.fromEntries(proof.CONTROLLER_HELPERS.map(name => [name, hash(Buffer.from(f.files[C][name]))]));
  const contract = { complete:true, workflowId:901, workflowName:"Publish Crate", mainHistoryAnchor:anchor, helpers, requiredJobs:clone(steps),
    finalizer:{jobName:"Publish and verify complete executable assets", stepName:steps["Publish and verify complete executable assets"][0]},
    crateJobName:"Publish and verify the crate closure" };
  const repository = clone(f.run.repository);
  const run = { id:900, run_attempt:1, workflow_id:901, check_suite_id:902, check_suite_node_id:"CS_release_fixture", repository, head_repository:repository,
    event:"workflow_dispatch", head_sha:C, head_branch:"main", name:"Publish Crate", path:source.CONTROLLER, status:"completed", conclusion:"success",
    created_at:clock(0), updated_at:clock(300) };
  const suite = { databaseId:902, repository:{databaseId:repository.id,nameWithOwner:source.REPOSITORY}, app:{databaseId:15368,slug:"github-actions"},
    commit:{oid:C}, workflowRun:{databaseId:900,event:"workflow_dispatch",runAttempt:1,workflow:{databaseId:901,name:"Publish Crate"},
      file:{path:source.CONTROLLER,repositoryName:source.REPOSITORY,repositoryFileUrl:"https://github.com/"+source.REPOSITORY+"/blob/"+C+"/"+source.CONTROLLER}} };
  const physical = Object.entries(steps).map(([name,names],i) => ({ id:2000+i, run_id:900, run_attempt:1, name, head_sha:C, head_branch:"main",
    status:"completed", conclusion:"success", started_at:clock(i===6 ? 240 : 1), completed_at:clock(i===6 ? 299 : 180),
    steps:names.map((name,n) => ({number:n+1,name,status:"completed",conclusion:"success",started_at:clock(i===6 ? 241+n : 2+n),completed_at:clock(i===6 ? 250+n : 3+n)})) }));
  const state = { run, suite, physical, latest:clone(physical), assets:[], afterRead:null, binaryReads:[], registry:new Map(), treeRoutes:new Map(), extraPr:null };
  const addAsset = (name, bytes) => {
    bytes=Buffer.from(bytes); const id=3000+state.assets.length, ref={assetId:id,name,sha256:hash(bytes),sizeBytes:bytes.length};
    state.assets.push({id,name,state:"uploaded",digest:"sha256:"+ref.sha256,size:bytes.length}); data.set(id,bytes); return ref;
  };
  const frontend = [addAsset("frontend-manifest.json", f.files[S][source.INPUT_PATHS.frontendManifest]), addAsset("lotus-frontend.zip", f.files[S][source.INPUT_PATHS.frontendZip])];
  const crateName="bamboo-agent", archive=Buffer.from("inert verified archive identity; not archive acceptance"), checksum=hash(archive);
  const crate={name:crateName,archiveSha256:checksum,zipAssetRef:addAsset("bamboo-verified-crate-"+crateName+"-"+version+".zip","inert retained ZIP"),
    origin:{jobId:2001,attempt:1},verificationSha256:"a".repeat(64),registryChecksum:checksum}; state.registry.set(crateName,checksum);
  const nativeAssets=sidecar.TARGETS.map((target,i) => addAsset(sidecar.artifactName(target),Buffer.alloc(65536,i+1)));
  const host=addAsset(sidecar.HOST_ASSET,"inert host archive"), provenance=addAsset(sidecar.PROVENANCE,"inert HMAC-attested native provenance");
  const manifest={schemaVersion:2,version,sourceRevision:S,artifacts:sidecar.TARGETS.map((target,i) => ({target,buildMode:"embedded",embeddedFrontend:true,format:"raw",
    assetName:nativeAssets[i].name,sha256:nativeAssets[i].sha256,sizeBytes:nativeAssets[i].sizeBytes})),
    browserHost:{assetName:host.name,sha256:host.sha256,sizeBytes:host.sizeBytes,files:sidecar.HOST_FILES.map(path=>({path,sha256:"b".repeat(64),sizeBytes:1}))}};
  const manifestRef=addAsset(sidecar.MANIFEST,JSON.stringify(manifest));
  const originals=sidecar.TARGETS.map(target=>({target,zipAssetRef:addAsset(native.archiveName(target),"inert native original "+target),checkpointRef:addAsset(native.checkpointName(target),"inert authenticated checkpoint "+target)}));
  const hostCheckpointRef=addAsset(native.HOST_CHECKPOINT,"inert authenticated Host checkpoint");
  const wrapper=JSON.parse(f.files[S][source.INPUT_PATHS.frontendManifest]), identity={repository:source.REPOSITORY,sourceRevision:S,
    frontend:core.frontendIdentity(facts.inputs.frontend.packageName,facts.inputs.frontend.packageVersion,wrapper,facts.inputs.frontend)};
  const receipt={schemaVersion:1,version,identity,crates:[crateName],automatic:true,completed:true,ciRun:"https://github.com/"+source.REPOSITORY+"/actions/runs/123",
    frontendBytes:{manifestSha256:facts.inputs.frontendManifest.sha256,archiveSha256:facts.inputs.frontendZip.sha256},packageChecksums:{[crateName]:checksum},sourceDispatch:marker,
    authentication:{schemaVersion:1,algorithm:"hmac-sha256",mac:"c".repeat(64)}};
  const receiptAsset=addAsset("release-provenance.json",JSON.stringify(receipt));
  const release={id:4000,tag_name:"v"+version,target_commitish:S,draft:false,prerelease:false,body:core.receiptBody(receipt)};
  const tag={object:{type:"commit",sha:S,url:"https://api.github.com/repos/"+source.REPOSITORY+"/git/commits/"+S}};
  const record={schemaVersion:1,kind:proof.KIND,publicationAuthorized:false,repository:{id:repository.id,fullName:source.REPOSITORY},
    source:{revision:S,tree:facts.source.tree,releaseRef:"release/"+S,featurePr:f.input.featurePr,featureRunId:123,sourceDispatchSha256:hash(Buffer.from(core.canonicalJson(marker)))},
    controller:{revision:C,workflowPath:source.CONTROLLER,workflowId:901,runId:900,attempt:1,checkSuiteId:902,checkSuiteNodeId:run.check_suite_node_id,finalizerJobId:2006},
    release:{id:4000,tag:"v"+version,version,targetCommit:S,receiptAsset,receiptSha256:hash(Buffer.from(core.canonicalJson(receipt)))},frontend,crates:[crate],
    native:{manifestSha256:manifestRef.sha256,assets:[...nativeAssets,host,provenance,manifestRef],originals,hostCheckpointRef},
    jobs:physical.slice(0,6).map(job=>({name:job.name,logical:{id:job.id,attempt:1},physical:{id:job.id,attempt:1},stepsSha256:hash(Buffer.from(native.executionKey(job)))}))};
  const page=(rows,route,key)=>({total_count:rows.length,[key]:clone(rows.slice((Number(new URL("https://fixture.invalid/"+route).searchParams.get("page"))-1)*100,Number(new URL("https://fixture.invalid/"+route).searchParams.get("page"))*100))});
  const reads={
    async json(route) {
      calls.push(route); let value;
      if(route==="actions/runs/900") value=state.run;
      else if(route==="actions/workflows/901") value={id:901,path:source.CONTROLLER,state:"active"};
      else if(route.startsWith("actions/runs/900/jobs?")) value=page(route.includes("filter=all")?state.physical:state.latest,route,"jobs");
      else if(route.startsWith("actions/runs/900/attempts/")) value=page(state.physical.filter(job=>job.run_attempt===Number(route.split("/")[4])),route,"jobs");
      else if(/^actions\/jobs\/[1-9][0-9]*$/.test(route)&&state.physical.some(job=>job.id===Number(route.slice(13)))) value=state.physical.find(job=>job.id===Number(route.slice(13)));
      else if(route==="releases/4000") value=state.release;
      else if(route.startsWith("releases/4000/assets?")) value=state.assets;
      else if(route==="git/ref/tags/v"+version) value=state.tag;
      else if(state.treeRoutes.has(route)) value=state.treeRoutes.get(route);
      else if(state.extraPr&&route==="pulls/88") value=state.extraPr;
      else value=await originalReads.json(route);
      value=clone(value); if(state.afterRead) state.afterRead(route,value,calls.filter(item=>item===route).length); return value;
    },
    checkSuite:async id=>id===run.check_suite_node_id?clone(state.suite):originalReads.checkSuite(id),
    jobLog:async id=>id===record.controller.finalizerJobId?clock(251)+" "+proof.PREFIX+Buffer.from(core.canonicalJson(state.record)).toString("base64")+"\n":originalReads.jobLog(id),
    assetBytes:async(id,maximum)=>{state.binaryReads.push(id);const bytes=data.get(id);assert.ok(bytes.length<=maximum);return Buffer.from(bytes)},
    registryChecksum:async(name,requestedVersion)=>{assert.equal(requestedVersion,version);return state.registry.get(name)},
  };
  Object.assign(state,{record,release,tag,receipt,data});
  const policy={...f.policy,releaseProducers:{[hash(Buffer.from(workflowText))]:contract}};
  const input={sourceRevision:S,featurePr:f.input.featurePr,ciRun:123,releaseRun:900};
  return {f,state,policy,input,reads,calls,contract,verify:()=>proof.verifyPublishedSource(input,reads,policy)};
}
test("public Main proof defaults inactive and never accepts a self-authored completion as authority",async()=>{
  const f=await fixture(); await assert.rejects(proof.verifyPublishedSource(f.input,f.reads),/inactive/); assert.equal(f.state.binaryReads.length,0);
  assert.deepEqual(proof.ACCEPTED_RELEASE_PRODUCERS,{});
});
test("public Main proof reads the real Source verifier and all original bytes with no signing key or writes",async()=>{
  const f=await fixture(),result=await f.verify(); assert.equal(result.publicationAuthorized,false);assert.equal(result.source.revision,S);
  assert.equal(result.feature.snapshotRevision,fixtures.identities.M);assert.equal(result.controller.finalizerJobId,2006);
  assert.equal(new Set(f.state.binaryReads).size,f.state.assets.length);assert.ok(f.f.calls.includes("pulls/2333"));
});
for(const [name,mutate] of [
  ["foreign Actions app",f=>{f.state.suite.app.databaseId=99}],
  ["wrong executed controller",f=>{f.state.suite.workflowRun.file.repositoryFileUrl=f.state.suite.workflowRun.file.repositoryFileUrl.replace(C,S)}],
  ["unknown complete producer",f=>{f.contract.complete=false}],
  ["missing complete helper",f=>{delete f.contract.helpers[proof.CONTROLLER_HELPERS[0]]}],
  ["changed controller helper",f=>{f.f.files[C][proof.CONTROLLER_HELPERS[0]]+="drift"}],
  ["latest failed finalizer",f=>{f.state.latest[6].conclusion=f.state.physical[6].conclusion="failure"}],
  ["required step skipped",f=>{f.state.latest[1].steps[0].conclusion=f.state.physical[1].steps[0].conclusion="skipped"}],
  ["foreign completed source",f=>{f.state.record.source.revision=C}],
  ["forged controller identity",f=>{f.state.record.controller.finalizerJobId=999}],
  ["crate-only receipt marked complete",f=>{f.state.record.native.assets.pop()}],
  ["missing original asset",f=>{f.state.assets.splice(0,1)}],
  ["replaced original asset",f=>{f.state.assets[0].id=999}],
  ["changed downloaded bytes",f=>{f.state.data.set(f.state.assets[0].id,Buffer.alloc(f.state.assets[0].size))}],
  ["registry checksum drift",f=>{f.state.registry.set("bamboo-agent","0".repeat(64))}],
  ["foreign archive origin",f=>{f.state.record.crates[0].origin.jobId=2000}],
  ["native target omitted",f=>{f.state.record.native.originals.pop()}],
  ["source feature was not merged",f=>{f.f.pr.merged=false}],
  ["source feature completed after merge",f=>{f.f.pr.merged_at=fixtures.times.created}],
]) test("public Main proof rejects "+name,async()=>{const f=await fixture();mutate(f);await assert.rejects(f.verify())});
test("public Main proof authenticates earliest physical jobs when a rerun copies old successes",async()=>{
  const f=await fixture();f.state.run.run_attempt=2;f.state.suite.workflowRun.runAttempt=2;
  const copies=f.state.physical.map(job=>({...clone(job),id:job.id+100,run_attempt:2}));f.state.physical.push(...copies);f.state.latest=clone(copies);
  const result=await f.verify();assert.equal(result.controller.attempt,1);assert.equal(result.jobs[0].physical.id,2000);assert.equal(result.jobs[0].logical.id,2000); // The old physical log is immutable.
});
test("public Main proof preserves a sealed crate from an earlier failed physical worker",async()=>{
  const f=await fixture(),old=clone(f.state.physical[1]);old.id=1999;old.conclusion="failure";old.steps[0].conclusion="failure";
  f.state.physical[1].run_attempt=f.state.latest[1].run_attempt=2;f.state.physical.push(old);f.state.run.run_attempt=2;f.state.suite.workflowRun.runAttempt=2;
  f.state.record.jobs[1].logical.attempt=f.state.record.jobs[1].physical.attempt=2;f.state.record.crates[0].origin.jobId=1999;
  const result=await f.verify();assert.equal(result.crates[0].origin.jobId,1999);assert.equal(result.jobs[1].physical.attempt,2);
});
for(const field of ["run","assets","tag","release","frozen","jobs"])
 test("public Main proof rejects mutable "+field+" changing during original-byte verification",async()=>{
   const f=await fixture();f.state.afterRead=(route,value,n)=>{
     if(field==="run"&&route==="actions/runs/900"&&n>1)value.run_attempt++;
     if(field==="assets"&&route.startsWith("releases/4000/assets?")&&n>1)value.pop();
     if(field==="tag"&&route.startsWith("git/ref/tags/")&&n>1)value.object.sha=C;
     if(field==="release"&&route==="releases/4000"&&n>1)value.draft=true;
     if(field==="frozen"&&route.startsWith("branches/release")&&n>1)value.commit.sha=C;
     if(field==="jobs"&&route.startsWith("actions/runs/900/jobs?filter=latest")&&n>1)value.jobs[0].conclusion="failure";
   };await assert.rejects(f.verify());
 });
test("whole completion parser rejects duplicates, noncanonical bytes, oversized frames and authority booleans",()=>{
  const record={schemaVersion:1,kind:proof.KIND,publicationAuthorized:false,repository:{id:1164779771,fullName:source.REPOSITORY},source:{},controller:{},release:{},frontend:[],crates:[],native:{},jobs:[]};
  const log=value=>clock(1)+" "+proof.PREFIX+Buffer.from(value).toString("base64")+"\n",valid=log(core.canonicalJson(record));assert.deepEqual(proof.frame(valid),record);
  for(const text of [valid+valid,log(JSON.stringify(record,null,2)),log(core.canonicalJson({...record,publicationAuthorized:true})),log(" ".repeat(65537))])assert.throws(()=>proof.frame(text));
});
async function promotionFixture(overlay=false){
  const f=await fixture(),sourceTree=f.state.record.source.tree,baseTree="8".repeat(40),headTree=overlay?"9".repeat(40):sourceTree,F=overlay?"a".repeat(40):S,M="0".repeat(40);
  const controls=[source.WORKFLOW,source.CONTROLLER,"scripts/published-source-admission.cjs",...proof.CONTROLLER_HELPERS];f.policy.controlPaths=[...new Set(controls)];
  const sourceLeaves=new Map(Object.entries(f.f.files[S]).map(([path,bytes])=>[path,{path,mode:"100644",type:"blob",sha:oid(bytes)}]));
  sourceLeaves.set("src/lib.rs",{path:"src/lib.rs",mode:"100644",type:"blob",sha:"d".repeat(40)});
  sourceLeaves.set("vendor/module",{path:"vendor/module",mode:"160000",type:"commit",sha:"f".repeat(40)});
  const baseLeaves=new Map(sourceLeaves);baseLeaves.set("src/lib.rs",{...sourceLeaves.get("src/lib.rs"),sha:"b".repeat(40)});
  for(const path of controls)baseLeaves.set(path,{path,mode:"100644",type:"blob",sha:oid("trusted B control "+path)});
  const headLeaves=new Map(sourceLeaves);if(overlay)for(const path of controls)headLeaves.set(path,baseLeaves.get(path));
  for(const [tree,leaves]of[[sourceTree,sourceLeaves],[baseTree,baseLeaves],[headTree,headLeaves]])f.state.treeRoutes.set("git/trees/"+tree+"?recursive=1",{sha:tree,truncated:false,tree:clone([...leaves.values()])});
  f.state.treeRoutes.set("git/commits/"+C,{sha:C,tree:{sha:baseTree},parents:[{sha:anchor}]});
  if(F!==S)f.state.treeRoutes.set("git/commits/"+F,{sha:F,tree:{sha:headTree},parents:[{sha:S}]});
  f.state.treeRoutes.set("git/commits/"+M,{sha:M,tree:{sha:headTree},parents:[{sha:C},{sha:F}]});
  f.state.extraPr={number:88,state:"open",draft:false,merged:false,merge_commit_sha:M,base:{ref:"main",sha:C,repo:clone(f.f.run.repository)},head:{ref:"promotion/"+S,sha:F,repo:clone(f.f.run.repository)}};
  f.promotionInput={...f.input,pullRequest:88,baseRevision:C,headRevision:F,snapshotRevision:M};
  f.promote=()=>proof.verifyPromotion(f.promotionInput,f.reads,f.policy);return f;
}
for(const overlay of [true])test("Main promotion admits one complete trusted base control closure without running candidate code",async()=>{
  const f=await promotionFixture(overlay),result=await f.promote();assert.equal(result.publicationAuthorized,false);assert.equal(result.promotion.controlMode,overlay?"complete-trusted-base":"released-source");
});
for(const fault of ["product","gitlink","mode","mixed-controls","truncated","wrong-parent","head","base-race"])
 test("Main promotion rejects "+fault,async()=>{
   const f=await promotionFixture(true),tree=f.state.treeRoutes.get("git/trees/"+"9".repeat(40)+"?recursive=1");
   if(fault==="product")tree.tree.find(x=>x.path==="src/lib.rs").sha="1".repeat(40);
   if(fault==="gitlink")tree.tree.find(x=>x.path==="vendor/module").sha="1".repeat(40);
   if(fault==="mode")tree.tree.find(x=>x.path==="src/lib.rs").mode="100755";
   if(fault==="mixed-controls")tree.tree.find(x=>x.path===source.WORKFLOW).sha="1".repeat(40);
   if(fault==="truncated")tree.truncated=true;
   if(fault==="wrong-parent")f.state.treeRoutes.get("git/commits/"+f.promotionInput.snapshotRevision).parents.reverse();
   if(fault==="head")f.state.extraPr.head.sha=S;
   if(fault==="base-race")f.state.afterRead=(route,value,n)=>{if(route==="branches/main"&&n>1)value.commit.sha=S};
   await assert.rejects(f.promote());
 });

test("Main promotion rejects full released source that would remove the installed Main control closure",async()=>{
  const f=await promotionFixture(false); await assert.rejects(f.promote(),/trusted|controls|control/);
});

test("Main promotion rejects a same-tree copy with absent released source ancestry",async()=>{
  const f=await promotionFixture(true);
  f.state.afterRead=(route,value)=>{if(route==="compare/"+S+"..."+f.promotionInput.headRevision)value.status="diverged"};
  await assert.rejects(f.promote(),/ancestry/);
});

for(const field of ["sha256","sizeBytes"]) test("public Main proof rejects contradictory native Host manifest "+field,async()=>{
  const f=await fixture(),ref=f.state.record.native.assets.find(x=>x.name===sidecar.MANIFEST);
  const manifest=JSON.parse(f.state.data.get(ref.assetId));
  manifest.browserHost[field]=field==="sha256"?"0".repeat(64):manifest.browserHost.sizeBytes+1;
  const bytes=Buffer.from(JSON.stringify(manifest));f.state.data.set(ref.assetId,bytes);
  ref.sha256=hash(bytes);ref.sizeBytes=bytes.length;f.state.record.native.manifestSha256=ref.sha256;
  const asset=f.state.assets.find(x=>x.id===ref.assetId);asset.digest="sha256:"+ref.sha256;asset.size=bytes.length;
  await assert.rejects(f.verify());
});
test("public Main proof accepts download counters changing without original identity or byte changes",async()=>{
  const f=await fixture();f.state.afterRead=(route,value,n)=>{
    if(route==="releases/4000")value.assets=[{id:99999,download_count:n}];
    if(route.startsWith("releases/4000/assets?"))for(const asset of value)asset.download_count=n;
  };
  assert.equal((await f.verify()).publicationAuthorized,false);
});

test("Main promotion ignores unrelated PR counters while keeping exact head/base/merge identity",async()=>{
  const f=await promotionFixture(true);f.state.afterRead=(route,value,n)=>{if(route==="pulls/88"){value.comments=n;value.updated_at=clock(n)}};
  assert.equal((await f.promote()).publicationAuthorized,false);
});
