"use strict";
// Inert transport/byte fixtures. No fixture workflow or archive was published.
const assert = require("node:assert/strict");
const { test } = require("node:test");
const { createHash } = require("node:crypto");
const { readFileSync } = require("node:fs");
const path = require("node:path");
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
  const current=f.state.physical[1];current.run_attempt=2;current.started_at=clock(311);current.completed_at=clock(360);
  current.steps[0].started_at=clock(312);current.steps[0].completed_at=clock(313);f.state.latest[1]=clone(current);
  const finalizer=clone(f.state.physical[6]);finalizer.id=2106;finalizer.run_attempt=2;finalizer.started_at=clock(400);finalizer.completed_at=clock(450);
  finalizer.steps[0].started_at=clock(402);finalizer.steps[0].completed_at=clock(403);
  f.state.physical[6].conclusion="skipped";f.state.physical[6].steps=[];
  f.state.physical.push(old,finalizer);f.state.latest[6]=clone(finalizer);
  f.state.run.run_attempt=2;f.state.run.updated_at=clock(500);f.state.suite.workflowRun.runAttempt=2;
  f.state.record.controller.attempt=2;f.state.record.controller.finalizerJobId=2106;
  f.state.record.jobs[1].logical.attempt=f.state.record.jobs[1].physical.attempt=2;
  f.state.record.jobs[1].stepsSha256=hash(Buffer.from(native.executionKey(current)));f.state.record.crates[0].origin.jobId=1999;
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
  const controls=proof.MAIN_CONTROLS;
  for(const name of controls)if(!(name in f.f.files[C]))f.f.files[C][name]="trusted B control "+name;
  f.f.files[C][proof.MAIN_WORKFLOW]=readFileSync(path.join(__dirname,"..",proof.MAIN_WORKFLOW),"utf8");
  const sourceLeaves=new Map(Object.entries(f.f.files[S]).map(([path,bytes])=>[path,{path,mode:"100644",type:"blob",sha:oid(bytes)}]));
  sourceLeaves.set("src/lib.rs",{path:"src/lib.rs",mode:"100644",type:"blob",sha:"d".repeat(40)});
  sourceLeaves.set("vendor/module",{path:"vendor/module",mode:"160000",type:"commit",sha:"f".repeat(40)});
  const baseLeaves=new Map(sourceLeaves);baseLeaves.set("src/lib.rs",{...sourceLeaves.get("src/lib.rs"),sha:"b".repeat(40)});
  for(const path of controls)baseLeaves.set(path,{path,mode:"100644",type:"blob",sha:oid(f.f.files[C][path])});
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

async function mergedFixture(){
  const f=await promotionFixture(true),accepted=await f.promote(),P="c".repeat(40),F=f.promotionInput.headRevision;
  const locator={mode:"promotion",...f.input};
  Object.assign(f.state.extraPr,{state:"closed",merged:true,merged_at:clock(600),merge_commit_sha:P,
    body:"<!-- bamboo-main-locator/v1\n"+JSON.stringify(locator)+"\n-->"});
  // Closed PR base.sha is not the historical B authority. Immutable P parents are.
  f.state.extraPr.base.sha=P;f.f.main.commit.sha=P;
  f.f.main.protection={required_status_checks:{enforcement_level:"everyone",contexts:[proof.MAIN_JOB],checks:[{context:proof.MAIN_JOB,app_id:15368}]}};
  f.state.treeRoutes.set("git/commits/"+P,{sha:P,tree:{sha:accepted.promotion.snapshotTree},parents:[{sha:C},{sha:F}]});
  const run={id:700,run_attempt:1,workflow_id:701,check_suite_id:702,check_suite_node_id:"CS_inert_main",repository:clone(f.f.run.repository),
    head_repository:clone(f.f.run.repository),name:"Bamboo Main Admission",event:"pull_request_target",path:proof.MAIN_WORKFLOW,
    head_sha:F,head_branch:f.state.extraPr.head.ref,status:"completed",conclusion:"success",created_at:clock(399),updated_at:clock(501)};
  const suite={databaseId:702,repository:{databaseId:1164779771,nameWithOwner:source.REPOSITORY},app:{databaseId:15368,slug:"github-actions"},
    commit:{oid:F},workflowRun:{databaseId:700,event:"pull_request_target",runAttempt:1,workflow:{databaseId:701,name:run.name},
      file:{path:proof.MAIN_WORKFLOW,repositoryName:source.REPOSITORY,repositoryFileUrl:`https://github.com/${source.REPOSITORY}/blob/${F}/${proof.MAIN_WORKFLOW}`}}};
  const job={id:703,run_id:700,run_attempt:1,name:proof.MAIN_JOB,head_sha:F,head_branch:run.head_branch,status:"completed",conclusion:"success",
    started_at:clock(400),completed_at:clock(500),steps:[{number:1,name:proof.MAIN_STEP,status:"completed",conclusion:"success",started_at:clock(401),completed_at:clock(499)}]};
  const record={schemaVersion:1,kind:proof.MAIN_KIND,publicationAuthorized:false,repository:{id:1164779771,fullName:source.REPOSITORY},
    locatorSha256:hash(Buffer.from(core.canonicalJson(locator))),source:{revision:S,tree:accepted.source.tree},
    release:{runId:900,completionSha256:accepted.completionSha256},promotion:accepted.promotion,
    execution:{revision:C,metadataRevision:F,workflowPath:proof.MAIN_WORKFLOW,workflowId:701,runId:700,physicalAttempt:1,jobId:703,checkSuiteId:702,checkSuiteNodeId:run.check_suite_node_id}};
  const state={run,suite,all:[job],latest:[clone(job)],record,log:null},original=f.reads;
  const page=rows=>({total_count:rows.length,jobs:clone(rows)});
  const reads={...original,json:async route=>{
    let value;
    if(route==="")value={id:1164779771,full_name:source.REPOSITORY,default_branch:"main"};
    else if(route==="actions/runs/700")value=state.run;
    else if(route==="actions/workflows/main-admission.yml")value={id:701,path:proof.MAIN_WORKFLOW,state:"active"};
    else if(route.startsWith("actions/runs/700/jobs?"))value=page(route.includes("filter=all")?state.all:state.latest);
    else if(route.startsWith("actions/runs/700/attempts/"))value=page(state.all.filter(job=>job.run_attempt===Number(route.split("/")[4])));
    else if(/^actions\/jobs\/[1-9][0-9]*$/.test(route)&&state.all.some(job=>job.id===Number(route.slice(13))))value=state.all.find(job=>job.id===Number(route.slice(13)));
    else return original.json(route);
    f.calls.push(route);value=clone(value);if(f.state.afterRead)f.state.afterRead(route,value,f.calls.filter(x=>x===route).length);return value;
  },checkSuite:async id=>id===run.check_suite_node_id?clone(state.suite):original.checkSuite(id),
  jobLog:async id=>state.all.some(job=>job.id===id)?(state.log??clock(499)+" "+proof.mainFrameLine(state.record)+"\n"):original.jobLog(id)};
  return {f,state,P,F,reads,input:{pullRequest:88,admissionRun:700},verify:()=>proof.verifyMergedPromotion({pullRequest:88,admissionRun:700},reads,f.policy)};
}
test("Main2351 merged proof retains distinct feature M, promotion M and actual normal P",async()=>{
  const m=await mergedFixture(),result=await m.verify();
  assert.equal(result.publicationAuthorized,false);assert.equal(result.source.revision,S);assert.equal(result.promotion.mergeRevision,m.P);
  assert.notEqual(result.promotion.snapshotRevision,m.P);assert.equal(result.feature.snapshotRevision,fixtures.identities.M);
  assert.equal(result.admission.revision,C);assert.equal(result.admission.metadataRevision,m.F);
  assert.equal(result.featureDocs.artifactName,"feature-docs-123-1");assert.equal("artifactId" in result.featureDocs,false);
});
test("Main2351 merged proof preserves original physical frame when GitHub copies its success",async()=>{
  const m=await mergedFixture(),copyJob={...clone(m.state.all[0]),id:1703,run_attempt:2};
  m.state.all.push(copyJob);m.state.latest=[clone(copyJob)];m.state.run.run_attempt=2;m.state.suite.workflowRun.runAttempt=2;
  const result=await m.verify();assert.equal(result.admission.jobId,703);assert.equal(result.admission.physicalAttempt,1);
  assert.equal(result.admission.logicalJobId,1703);assert.equal(result.admission.logicalAttempt,2);
});
test("Main2351 merged proof permits normal Main advance and a changing closed PR base view",async()=>{
  const m=await mergedFixture(),next="7".repeat(40);
  m.f.state.afterRead=(route,value,n)=>{
    if(route==="branches/main"&&n>1)value.commit.sha=next;
    if(route==="pulls/88"&&n>1)value.base.sha=next;
  };
  assert.equal((await m.verify()).promotion.mergeRevision,m.P);
});
test("Main2351 merged proof accepts official same-second physical completion and merge",async()=>{
  const m=await mergedFixture();m.f.state.extraPr.merged_at=m.state.all[0].completed_at;
  assert.equal((await m.verify()).promotion.mergeRevision,m.P);
});
test("Main2351 merged proof rejects physical completion one second after merge",async()=>{
  const m=await mergedFixture();m.f.state.extraPr.merged_at=clock(499);
  await assert.rejects(m.verify(),/no later than recorded promotion/);
});
for(const fault of ["candidate-event","candidate-path","different-workflow","file-as-B","green-stub","skipped-step","duplicate-job","missing-original",
  "late-execution","fake-P","P-tree","source-history","frame-B","frame-M","frame-source","frame-release","frame-attempt","bootstrap","locator-race","main-rewrite","default-dev","required-app"])
test("Main2351 merged proof rejects "+fault,async()=>{
  const m=await mergedFixture();
  if(fault==="candidate-event")m.state.run.event="pull_request";
  if(fault==="candidate-path")m.state.run.path=".github/workflows/candidate.yml";
  if(fault==="different-workflow")m.state.run.workflow_id++;
  if(fault==="file-as-B")m.state.suite.workflowRun.file.repositoryFileUrl=m.state.suite.workflowRun.file.repositoryFileUrl.replace(m.F,C);
  if(fault==="green-stub"){m.state.all[0].steps=[];m.state.latest=clone(m.state.all)}
  if(fault==="skipped-step"){m.state.all[0].steps[0].status="in_progress";m.state.latest=clone(m.state.all)}
  if(fault==="duplicate-job")m.state.all.push({...clone(m.state.all[0]),id:704});
  if(fault==="missing-original")m.state.all=[];
  if(fault==="late-execution"){m.state.all[0].completed_at=clock(700);m.state.latest=clone(m.state.all)}
  if(fault==="fake-P")m.f.state.treeRoutes.get("git/commits/"+m.P).parents.reverse();
  if(fault==="P-tree")m.f.state.treeRoutes.get("git/commits/"+m.P).tree.sha="1".repeat(40);
  if(fault==="source-history")m.f.state.afterRead=(route,value)=>{if(route===`compare/${S}...${m.F}`)value.status="diverged"};
  if(fault==="frame-B")m.state.record.execution.revision=S;
  if(fault==="frame-M")m.state.record.promotion.snapshotRevision=S;
  if(fault==="frame-source")m.state.record.source.revision=C;
  if(fault==="frame-release")m.state.record.release.completionSha256="0".repeat(64);
  if(fault==="frame-attempt")m.state.record.execution.physicalAttempt=2;
  if(fault==="bootstrap")m.f.state.extraPr.body="<!-- bamboo-main-locator/v1\n"+JSON.stringify({mode:"bootstrap",sourceRevision:S,featurePr:2333,ciRun:123})+"\n-->";
  if(fault==="locator-race")m.f.state.afterRead=(route,value,n)=>{if(route==="pulls/88"&&n>1)value.body=value.body.replace('"releaseRun":900','"releaseRun":901')};
  if(fault==="main-rewrite")m.f.state.afterRead=(route,value,n)=>{if(route==="branches/main"&&n>1)value.commit.sha=S;if(route===`compare/${m.P}...${S}`)value.status="diverged"};
  if(fault==="default-dev")m.f.state.afterRead=(route,value)=>{if(route==="")value.default_branch="dev"};
  if(fault==="required-app")m.f.f.main.protection.required_status_checks.checks[0].app_id=99;
  await assert.rejects(m.verify());
});
test("Main2351 fixed control closure rejects caller policy and every nonordinary/missing control",async()=>{
  const f=await promotionFixture(true);f.policy.controlPaths=[source.WORKFLOW];await assert.rejects(f.promote(),/Caller-selected/);
  for(const path of proof.MAIN_CONTROLS){
    const leaves=new Map(proof.MAIN_CONTROLS.map(name=>[name,{mode:"100644",sha:"1".repeat(40)}]));
    leaves.delete(path);assert.throws(()=>proof.closure(leaves));leaves.set(path,{mode:"120000",sha:"1".repeat(40)});assert.throws(()=>proof.closure(leaves));
  }
});
test("Main2351 physical log frame rejects echo, duplicate, noncanonical, oversized and bootstrap data",async()=>{
  const m=await mergedFixture(),record=m.state.record,valid=clock(499)+" "+proof.mainFrameLine(record)+"\n";
  assert.deepEqual(proof.logFrame(valid),record);
  const line=value=>clock(499)+" "+proof.MAIN_PREFIX+Buffer.from(value).toString("base64")+"\n";
  for(const log of [valid+valid,proof.mainFrameLine(record),line(JSON.stringify(record,null,2)),line(" ".repeat(65537)),
    line(core.canonicalJson({...record,kind:"bamboo-main-bootstrap-ops/v1"})),line(core.canonicalJson({...record,publicationAuthorized:true}))])assert.throws(()=>proof.logFrame(log));
});
test("Main2351 public receipt accepts real thirty-crate checkpoint shape above 64 KiB within producer 1 MiB",async()=>{
  const f=await fixture(),original=require("./release-crate-output.cjs"),sandbox=require("./release-cargo-sandbox.cjs");
  const members=readFileSync(path.join(__dirname,"..","Cargo.toml"),"utf8").match(/default-members\s*=\s*\[([\s\S]*?)\]/)[1];
  const crates=[...members.matchAll(/"([^"]+)"/g)].map(([,directory])=>({directory,
    name:readFileSync(path.join(__dirname,"..",directory,"Cargo.toml"),"utf8").match(/(?:^|\n)name\s*=\s*"([^"]+)"/)[1]}));
  assert.equal(crates.length,30);assert.equal(new Set(crates.map(x=>x.name)).size,30);
  const receipt=f.state.receipt,version=receipt.version,checksum=f.state.record.crates[0].archiveSha256;
  receipt.crates=crates.map(x=>x.name);receipt.packageChecksums={};receipt.crateOriginals={};
  receipt.frontendOriginals={schemaVersion:1,complete:true,assets:clone(f.state.record.frontend)};
  receipt.nativeOriginals=core.nativeOriginalContract({workflowRevision:C,runId:"900"});
  f.state.record.crates=[];
  for(const [index,{name,directory}]of crates.entries()){
    const assetName=`bamboo-verified-crate-${name}-${version}.zip`,existing=f.state.assets.find(asset=>asset.name===assetName);
    const assetId=existing?.id??6000+index,bytes=existing?f.state.data.get(assetId):Buffer.from("inert retained crate transport "+name);
    const ref={assetId,name:assetName,sizeBytes:bytes.length,sha256:hash(bytes)};
    if(!existing)f.state.assets.push({id:assetId,name:assetName,size:bytes.length,digest:"sha256:"+ref.sha256,state:"uploaded"});
    f.state.data.set(assetId,bytes);f.state.registry.set(name,checksum);
    const inputs={repository:source.REPOSITORY,releaseId:4000,sourceRevision:S,sourceTree:f.state.record.source.tree,version,name,
      pathInVcs:directory==="."?"":directory,frontendBytes:clone(receipt.frontendBytes)};
    const origin={controllerRevision:C,runId:900,attempt:1,jobId:2001,jobKey:"publish-crates",jobName:original.JOB,workflowPath:original.WORKFLOW};
    const archiveProof={schemaVersion:1,kind:original.KIND,inputs,origin,archive:{name:`${name}-${version}.crate`,size:1,sha256:checksum},
      verification:{preparationCommand:["package","--locked","--allow-dirty","--no-verify","-p",name],
        command:["package","--locked","--allow-dirty","-p",name],image:sandbox.IMAGE,exitCode:0}};
    const state={schemaVersion:1,kind:original.KIND,inputs,origin,phase:"published",sealed:{size:bytes.length,sha256:ref.sha256,proof:archiveProof},
      asset:{id:assetId,name:assetName,size:bytes.length,digest:"sha256:"+ref.sha256}};
    original.record(state,inputs);receipt.crateOriginals[name]=state;receipt.packageChecksums[name]=checksum;
    f.state.record.crates.push({name,archiveSha256:checksum,zipAssetRef:ref,origin:{jobId:2001,attempt:1},verificationSha256:hash(Buffer.from(core.canonicalJson(archiveProof))),registryChecksum:checksum});
  }
  core.signReceipt(receipt,"d".repeat(64));core.validateReceipt(receipt,receipt.identity,receipt.crates,version);
  const pretty=Buffer.from(JSON.stringify(receipt,null,2));assert.ok(pretty.length>65536&&pretty.length<=1024*1024);
  const ref=f.state.record.release.receiptAsset;ref.sha256=hash(pretty);ref.sizeBytes=pretty.length;f.state.data.set(ref.assetId,pretty);
  Object.assign(f.state.assets.find(asset=>asset.id===ref.assetId),{size:pretty.length,digest:"sha256:"+ref.sha256});
  f.state.record.release.receiptSha256=hash(Buffer.from(core.canonicalJson(receipt)));f.state.release.body=core.receiptBody(receipt);
  assert.equal((await f.verify()).crates.length,30);
});
for(const [role,maximum]of[["receipt",1024*1024],["wrapper",1024*1024],["accepted-source-zip",16*1024*1024]])
test("Main2351 public "+role+" retains its own strict role bound",async()=>{
  const f=await fixture(),ref=role==="receipt"?f.state.record.release.receiptAsset:f.state.record.frontend[role==="wrapper"?0:1];
  ref.sizeBytes=maximum+1;f.state.assets.find(asset=>asset.id===ref.assetId).size=ref.sizeBytes;
  await assert.rejects(f.verify(),/exceeds its role bound/);assert.ok(!f.state.binaryReads.includes(ref.assetId));
});
test("Controller2354 locator accepts only a controller source locator without release fields",()=>{
  const value={mode:"controller",sourceRevision:S,featurePr:2333,ciRun:123};
  const text=value=>"<!-- bamboo-main-locator/v1\n"+JSON.stringify(value)+"\n-->";
  assert.deepEqual(proof.mainLocator(text(value)),value);
  for(const extra of [{releaseRun:900},{version:"2026.10.27"},{controlPaths:["src/lib.rs"]},{approved:true}])
    assert.throws(()=>proof.mainLocator(text({...value,...extra})));
});
function controllerFixture(){
  const f=frozenSourceFixture(),D=f.input.sourceRevision,B=C,F="a".repeat(40),M="0".repeat(40),baseTree="8".repeat(40),headTree="9".repeat(40);
  const file=(path,bytes)=>({path,mode:"100644",type:"blob",sha:oid(bytes)});
  for(const name of proof.MAIN_CONTROLS)if(!(name in f.files[D]))f.files[D][name]="inert accepted D control "+name;
  f.files[D][proof.MAIN_WORKFLOW]=readFileSync(path.join(__dirname,"..",proof.MAIN_WORKFLOW),"utf8");
  f.files[B]=Object.fromEntries(proof.MAIN_CONTROLS.map(name=>[name,name===proof.MAIN_WORKFLOW?f.files[D][name]:"trusted B control "+name]));
  const product={"src/lib.rs":"stable product","Cargo.toml":'version = "0.0.0"',"Cargo.lock":"stable lock",
    "scripts/frontend-package-lock.json":"stable locked frontend","frontend/zip":"stable published frontend bytes"};
  const base=new Map([...Object.entries(f.files[B]),...Object.entries(product)].map(([name,bytes])=>[name,file(name,bytes)]));
  base.set("vendor/module",{path:"vendor/module",mode:"160000",type:"commit",sha:"7".repeat(40)});
  base.set("bin/launch",{path:"bin/launch",mode:"100755",type:"blob",sha:"6".repeat(40)});
  const selected=new Map(Object.entries(f.files[D]).map(([name,bytes])=>[name,file(name,bytes)]));
  selected.set("src/lib.rs",file("src/lib.rs","unreleased D product; never copied"));
  selected.set("new-product.rs",file("new-product.rs","unreleased new file; never copied"));
  const head=new Map(base);for(const name of proof.MAIN_CONTROLS)head.set(name,selected.get(name));
  const rows=(sha,map)=>({sha,truncated:false,tree:clone([...map.values()])});
  const routes=new Map([["git/commits/"+B,{sha:B,tree:{sha:baseTree},parents:[{sha:anchor}]}],
    ["git/commits/"+F,{sha:F,tree:{sha:headTree},parents:[{sha:B}]}],["git/commits/"+M,{sha:M,tree:{sha:headTree},parents:[{sha:B},{sha:F}]}],
    ["git/trees/"+baseTree+"?recursive=1",rows(baseTree,base)],["git/trees/"+f.accepted.tree.sha+"?recursive=1",rows(f.accepted.tree.sha,selected)],
    ["git/trees/"+headTree+"?recursive=1",rows(headTree,head)]]);
  f.main.protection={required_status_checks:{enforcement_level:"everyone",contexts:[proof.MAIN_JOB],checks:[{context:proof.MAIN_JOB,app_id:15368}]}};
  const locator={mode:"controller",sourceRevision:D,featurePr:f.input.featurePr,ciRun:f.input.ciRun};
  const pr={number:88,state:"open",merged:false,draft:false,mergeable:true,merge_commit_sha:M,
    body:"<!-- bamboo-main-locator/v1\n"+JSON.stringify(locator)+"\n-->",base:{ref:"main",sha:B,repo:clone(f.run.repository)},head:{ref:"controller/test",sha:F,repo:clone(f.run.repository)}};
  const original=f.reads,state={afterRead:null,counts:new Map(),logReads:[]},calls=[];
  const reads={...original,json:async route=>{
    calls.push(route);const n=(state.counts.get(route)||0)+1;state.counts.set(route,n);let value;
    if(route==="")value={id:1164779771,full_name:source.REPOSITORY,default_branch:"main"};
    else if(route==="pulls/88")value=pr;
    else if(routes.has(route))value=routes.get(route);
    else value=await original.json(route);
    value=clone(value);if(state.afterRead)state.afterRead(route,value,n);return value;
  },jobLog:async id=>{state.logReads.push(id);return original.jobLog(id)}};
  const input={sourceRevision:D,featurePr:f.input.featurePr,ciRun:f.input.ciRun,pullRequest:88,baseRevision:B,headRevision:F,snapshotRevision:M};
  return {f,D,B,F,M,baseTree,headTree,base,selected,head,routes,pr,locator,state,reads,calls,input,
    verify:()=>proof.verifyControllerMaintenance(input,reads,f.policy),updateSelected:()=>routes.set("git/trees/"+f.accepted.tree.sha+"?recursive=1",rows(f.accepted.tree.sha,selected))};
}
test("Controller2354 copies complete accepted D controls and preserves every B product/version/lock/gitlink/mode",async()=>{
  const f=controllerFixture(),result=await f.verify();
  assert.equal(result.kind,proof.CONTROLLER_KIND);assert.equal(result.publicationAuthorized,false);
  assert.equal(result.controllerSource.revision,f.D);assert.equal(result.controllerSource.featureSnapshotRevision,fixtures.identities.M);
  assert.equal(result.maintenance.baseProductTree,f.baseTree);assert.equal(result.maintenance.snapshotRevision,f.M);
  assert.notEqual(f.M,fixtures.identities.M);assert.ok(f.f.calls.includes("pulls/2333"));
  assert.equal(new Set(f.state.logReads).size,f.state.logReads.length,"Only immutable original logs may be reused across final fresh Source proof");
  assert.ok(!f.calls.some(route=>/^(releases|git\/ref\/tags|branches\/release)/.test(route)),"Maintenance needs no frozen ref, artifact or publication");
  for(const [name,entry]of f.base)if(!proof.MAIN_CONTROLS.includes(name))assert.deepEqual(f.head.get(name),entry);
  assert.equal(f.head.has("new-product.rs"),false);
});
for(const fault of ["product","version","lock","frontend-lock","frontend-bytes","gitlink","mode","new-product","partial-controls","missing-control","symlink-control",
  "B-control-missing","M-parents","M-tree","no-B-history","unknown-Main-entrypoint","unknown-recipe","unknown-helper","unmerged-D","failed-source-job","source-rerun",
  "head-race","locator-race","base-race","dev-rewrite","default-dev","required-app","caller-paths","release-locator"])
test("Controller2354 rejects "+fault,async()=>{
  const f=controllerFixture(),rows=f.routes.get("git/trees/"+f.headTree+"?recursive=1").tree;
  const change=name=>rows.find(row=>row.path===name).sha="1".repeat(40);
  if(fault==="product")change("src/lib.rs");if(fault==="version")change("Cargo.toml");if(fault==="lock")change("Cargo.lock");
  if(fault==="frontend-lock")change("scripts/frontend-package-lock.json");if(fault==="frontend-bytes")change("frontend/zip");if(fault==="gitlink")change("vendor/module");
  if(fault==="mode")rows.find(row=>row.path==="bin/launch").mode="100644";
  if(fault==="new-product")rows.push({path:"new-product.rs",mode:"100644",type:"blob",sha:"1".repeat(40)});
  if(fault==="partial-controls")change(proof.MAIN_CONTROLS[3]);
  if(fault==="missing-control"){const tree=f.routes.get("git/trees/"+f.f.accepted.tree.sha+"?recursive=1").tree;tree.splice(tree.findIndex(row=>row.path===proof.MAIN_CONTROLS[1]),1)}
  if(fault==="symlink-control")f.routes.get("git/trees/"+f.f.accepted.tree.sha+"?recursive=1").tree.find(row=>row.path===proof.MAIN_WORKFLOW).mode="120000";
  if(fault==="B-control-missing")f.routes.get("git/trees/"+f.baseTree+"?recursive=1").tree.shift();
  if(fault==="M-parents")f.routes.get("git/commits/"+f.M).parents.reverse();
  if(fault==="M-tree")f.routes.get("git/commits/"+f.M).tree.sha=f.baseTree;
  if(fault==="no-B-history")f.state.afterRead=(route,value)=>{if(route===`compare/${f.B}...${f.F}`)value.status="diverged"};
  if(fault==="unknown-Main-entrypoint")f.f.files[f.D][proof.MAIN_WORKFLOW]+="\n# unregistered entrypoint version";
  if(fault==="unknown-recipe")f.f.files[fixtures.identities.F][source.WORKFLOW]+="\n# unknown recipe";
  if(fault==="unknown-helper")f.f.files[fixtures.identities.M][source.REQUIRED_HELPERS[0]]+="drift";
  if(fault==="unmerged-D")f.f.pr.merged=false;
  if(fault==="failed-source-job")f.f.latest[2].conclusion="failure";
  if(fault==="source-rerun")f.state.afterRead=(route,value,n)=>{if(route==="actions/runs/123"&&n>2)value.run_attempt++};
  if(fault==="head-race")f.state.afterRead=(route,value,n)=>{if(route==="pulls/88"&&n>1)value.head.sha=f.D};
  if(fault==="locator-race")f.state.afterRead=(route,value,n)=>{if(route==="pulls/88"&&n>1)value.body=value.body.replace('"ciRun":123','"ciRun":124')};
  if(fault==="base-race")f.state.afterRead=(route,value,n)=>{if(route==="branches/main"&&n>1)value.commit.sha=f.D};
  if(fault==="dev-rewrite")f.state.afterRead=(route,value,n)=>{
    if(route==="branches/dev"&&n>=2)value.commit.sha=n===2?"7".repeat(40):"8".repeat(40);
    if(route===`compare/${"7".repeat(40)}...${"8".repeat(40)}`)value.status="diverged";
  };
  if(fault==="default-dev")f.state.afterRead=(route,value)=>{if(route==="")value.default_branch="dev"};
  if(fault==="required-app")f.f.main.protection.required_status_checks.checks[0].app_id=99;
  if(fault==="caller-paths")f.f.policy.controlPaths=["src/lib.rs"];
  if(fault==="release-locator")f.pr.body=f.pr.body.replace('"ciRun":123','"ciRun":123,"releaseRun":900');
  await assert.rejects(f.verify());
});
test("Controller2354 permits normal dev advancement while keeping original D and Source execution",async()=>{
  const f=controllerFixture();f.state.afterRead=(route,value,n)=>{if(route==="branches/dev"&&n>=2)value.commit.sha=n===2?"7".repeat(40):"8".repeat(40)};
  assert.equal((await f.verify()).controllerSource.revision,f.D);
});
test("Controller2354 default registry stays inactive and production readers receive no caller authority",async()=>{
  const f=controllerFixture();await assert.rejects(proof.verifyControllerMaintenance(f.input,f.reads),/inactive/);
  assert.deepEqual(source.ACCEPTED_PRODUCERS,{});assert.deepEqual(proof.ACCEPTED_RELEASE_PRODUCERS,{});
});
test("Controller2354 registration first permits known P0 controls then explicitly known P1 without unknown fallback",async()=>{
  const Module=require("node:module"),before=controllerFixture(),next=controllerFixture(),registryFile="scripts/frozen-source-admission.cjs";
  const text=fixtures.workflowText+"# explicitly reviewed P1\n",helper=source.REQUIRED_HELPERS[0],helperText=fixtures.helperText+"// reviewed P1 helper\n";
  const p0=hash(Buffer.from(fixtures.workflowText)),p1=hash(Buffer.from(text)),contract=clone(next.f.contract);contract.helpers[helper]=hash(Buffer.from(helperText));
  const registryText=producers=>{
    const original=readFileSync(path.join(__dirname,"frozen-source-admission.cjs"),"utf8"),needle="const ACCEPTED_PRODUCERS = Object.freeze({});";
    assert.equal(original.split(needle).length,2);return original.replace(needle,"const ACCEPTED_PRODUCERS = Object.freeze("+JSON.stringify(producers)+");");
  };
  // Simulate the trusted installed B module, not a caller policy argument. Only
  // known repository source + inert registration data is compiled in this unit.
  const installed=sourceText=>{
    const filename=path.join(__dirname,"frozen-source-admission.cjs"),s=new Module(filename,module);s.filename=filename;
    s.require=Module.createRequire(filename);s._compile(sourceText,filename);
    const target=path.join(__dirname,"published-source-admission.cjs"),m=new Module(target,module);m.filename=target;
    const normal=Module.createRequire(target);m.require=name=>name==="./frozen-source-admission.cjs"?s.exports:normal(name);
    m._compile(readFileSync(target,"utf8"),target);return m.exports;
  };
  const B0=installed(registryText({[p0]:clone(before.f.contract)}));
  const nextControls=registryText({[p0]:clone(before.f.contract),[p1]:contract});
  before.f.files[before.D][registryFile]=nextControls;
  const changed={path:registryFile,mode:"100644",type:"blob",sha:oid(before.f.files[before.D][registryFile])};
  for(const map of [before.selected,before.head])map.set(registryFile,changed);
  for(const tree of [before.f.accepted.tree.sha,before.headTree])before.routes.get("git/trees/"+tree+"?recursive=1").tree.find(row=>row.path===registryFile).sha=changed.sha;
  assert.equal((await B0.verifyControllerMaintenance(before.input,before.reads)).controllerSource.revision,before.D);
  for(const ref of [fixtures.identities.F,fixtures.identities.M,next.D]){next.f.files[ref][source.WORKFLOW]=text;next.f.files[ref][helper]=helperText}
  for(const [name,bytes]of[[source.WORKFLOW,text],[helper,helperText]]){
    const entry={path:name,mode:"100644",type:"blob",sha:oid(bytes)};
    next.selected.set(name,entry);if(proof.MAIN_CONTROLS.includes(name))next.routes.get("git/trees/"+next.headTree+"?recursive=1").tree.find(row=>row.path===name).sha=entry.sha;
  }
  next.updateSelected();await assert.rejects(B0.verifyControllerMaintenance(next.input,next.reads),/Unknown|incomplete/);
  const B1=installed(before.f.files[before.D][registryFile]); // Exact complete-D control copied to the simulated B1.
  assert.equal((await B1.verifyControllerMaintenance(next.input,next.reads)).controllerSource.revision,next.D);
  const pinned=controllerFixture();pinned.f.contract.helpers[registryFile]=hash(Buffer.from("old pinned validator"));
  pinned.f.files[fixtures.identities.M][registryFile]=pinned.f.files[pinned.D][registryFile];
  const pinnedB0=installed(registryText({[p0]:pinned.f.contract}));
  await assert.rejects(pinnedB0.verifyControllerMaintenance(pinned.input,pinned.reads),/helper/);
  assert.deepEqual(source.ACCEPTED_PRODUCERS,{}); // No live/global registry was installed.
});
test("Controller2354 distinct controller frame cannot satisfy publication or merged promotion proof",async()=>{
  const f=controllerFixture(),facts=await f.verify(),m=await mergedFixture();
  const record={...facts,repository:{id:1164779771,fullName:source.REPOSITORY},locatorSha256:hash(Buffer.from(core.canonicalJson(f.locator))),
    execution:{revision:f.B,metadataRevision:f.F,workflowPath:proof.MAIN_WORKFLOW,workflowId:701,runId:700,physicalAttempt:1,jobId:703,checkSuiteId:702,checkSuiteNodeId:"CS_inert_main"}};
  const line=clock(499)+" "+proof.controllerFrameLine(record);assert.deepEqual(proof.controllerLogFrame(line),record);
  const forbidden=value=>Object.entries(value).some(([key,value])=>["release","releaseRun","version","tag"].includes(key)||(value&&typeof value==="object"&&forbidden(value)));
  assert.equal(forbidden(record),false);assert.throws(()=>proof.logFrame(line));
  const before=m.f.calls.length;m.state.log=line;m.f.state.extraPr.body=f.pr.body;
  await assert.rejects(m.verify(),/frame/);assert.ok(!m.f.calls.slice(before).some(route=>route.startsWith("releases/")));
  await assert.rejects(proof.verifyPublishedSource(record,f.reads),/fields/);
  assert.throws(()=>core.sourceDispatchContract(record,"700"));
  for(const invalid of [{...record,release:{}},{...record,version:"2026.10.27"},{...record,publicationAuthorized:true},
    {...record,controllerSource:{...record.controllerSource,releaseRun:900}}])assert.throws(()=>proof.controllerFrameLine(invalid));
  assert.throws(()=>proof.controllerLogFrame(line+"\n"+line));
  assert.throws(()=>proof.controllerLogFrame(proof.controllerFrameLine(record)));
  assert.equal((await mergedFixture()).state.record.kind,proof.MAIN_KIND);
  assert.equal((await (await mergedFixture()).verify()).kind,"bamboo-merged-main-promotion/v1");
});
