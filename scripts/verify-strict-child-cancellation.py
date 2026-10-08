#!/usr/bin/env python3
"""Loopback fixture using the default native Host/Worker; cold reads disable background auto-Dream."""
import argparse, base64, hashlib, http.server, json, os, pathlib, socket, subprocess, threading, time, traceback, urllib.request, urllib.error

P = argparse.ArgumentParser(); P.add_argument('--binary', required=True); P.add_argument('--output', required=True); P.add_argument('--source-head', required=True)
A = P.parse_args(); out=pathlib.Path(A.output); out.mkdir(parents=True, exist_ok=False)
data=out/'data'; data.mkdir(); (data/'home').mkdir(); (data/'workspace').mkdir()
fixture=data/'workspace'/'evidence.txt'; fixture.write_text('WORKFLOW_READ_EVIDENCE\n')
lock=threading.Lock(); requests=[]; hold=threading.Event(); connected=threading.Event(); disconnected=threading.Event()
phase='success'
disconnected_times=[]
def mark_disconnected():
    disconnected_times.append(time.monotonic()); disconnected.set()
report={'version':1,'outcome':'completed','summary':'Read evidence file','reported_evidence':[], 'reported_verification':[], 'proposals':[], 'blockers':[], 'open_decisions':[]}
class Provider(http.server.BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self, *_): pass
    def do_GET(self):
        b=json.dumps({'object':'list','data':[{'id':'fixture-model','object':'model'}]}).encode(); self.send_response(200); self.send_header('Content-Length',str(len(b))); self.end_headers(); self.wfile.write(b)
    def do_POST(self):
        body=json.loads(self.rfile.read(int(self.headers.get('Content-Length','0'))))
        with lock:
            rows=[r for r in requests if r['phase']==phase]; index=len(rows); row={'phase':phase,'index':index,'path':self.path,'body':body}; requests.append(row)
        self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.send_header('Connection','close'); self.end_headers()
        if phase=='cancel' and index==1:
            connected.set()
            self.connection.settimeout(20)
            try:
                # No further provider chunk is delivered; cancellation must close the actual request.
                while not hold.is_set():
                    try:
                        if not self.connection.recv(1): mark_disconnected(); break
                    except socket.timeout: break
            except (ConnectionError,OSError): mark_disconnected()
            return
        if index==0:
            delta={'role':'assistant','tool_calls':[{'index':0,'id':'workflow-read-'+phase,'type':'function','function':{'name':'Read','arguments':json.dumps({'file_path':str(fixture)})}}]}; finish='tool_calls'; tokens=(11,7)
        else:
            delta={'role':'assistant','content':json.dumps(report)}; finish='stop'; tokens=(13,5)
        chunks=[{'id':'workflow-fixture','object':'chat.completion.chunk','choices':[{'index':0,'delta':delta,'finish_reason':finish}]}, {'id':'workflow-fixture','object':'chat.completion.chunk','choices':[],'usage':{'prompt_tokens':tokens[0],'completion_tokens':tokens[1],'total_tokens':sum(tokens)}}]
        try:
            for chunk in chunks: self.wfile.write(('data: '+json.dumps(chunk)+'\n\n').encode())
            self.wfile.write(b'data: [DONE]\n\n'); self.wfile.flush()
        except (ConnectionError,OSError): row['connection_closed']=True
provider=http.server.ThreadingHTTPServer(('127.0.0.1',0),Provider); provider.daemon_threads=True
threading.Thread(target=provider.serve_forever,daemon=True).start()
s=socket.socket(); s.bind(('127.0.0.1',0)); port=s.getsockname()[1]; s.close(); base=f'http://127.0.0.1:{port}/api/v1'
config={'provider':'openai','features':{'provider_model_ref':True},'providers':{'openai':{'api_key':'loopback-fixture-only','base_url':f'http://127.0.0.1:{provider.server_port}/v1','model':'fixture-model','runtime_models':['fixture-model']}},'defaults':{'chat':{'provider':'openai','model':'fixture-model'}},'subagents':{'runtime':'actor','executor':'bamboo_runtime','max_concurrent':2}}
(data/'config.json').write_text(json.dumps(config)); (data/'agents').mkdir(); (data/'agents/reviewer.md').write_text('---\nschema_version: 1\nname: reviewer\ndescription: Bounded reviewer\ntools:\n  allow: [Read]\n---\nPINNED_PRIVATE_WORKFLOW_PROFILE\n')
skill=data/'skills'/'agent-flow'; skill.mkdir(parents=True); (skill/'SKILL.md').write_text('---\nname: agent-flow\ndescription: Bounded workflow Agent proof\n---\nReview assigned file.\n')
(skill/'workflow.yaml').write_text('''workflow_schema: 1
id: agent-flow
revision: 1
invocation_policy: {explicit: true, automatic: false}
input_schema: {type: object, additionalProperties: true}
steps:
  - id: review
    type: agent
    agent: reviewer
    prompt: {from: literal, value: Read the assigned evidence file and return the required typed child report.}
    capabilities: [read]
plan: {type: step, step: review}
budgets:
  max_concurrency: 2
  max_agents: 2
  max_steps: 4
  max_retries: 0
  max_nesting_depth: 1
  wall_time_ms: 120000
  max_tokens: 1000
''')
env=os.environ.copy(); env.update({'HOME':str(data/'home'),'BAMBOO_JIANDU_DATA_DIR':str(data/'jiandu'),'RUST_LOG':'warn'})
log=open(out/'host.log','wb'); host=subprocess.Popen([A.binary,'serve','--bind','127.0.0.1','--port',str(port),'--data-dir',str(data)],cwd=data,env=env,stdout=log,stderr=log)
def api(method,path,body=None,timeout=30):
    b=None if body is None else json.dumps(body).encode(); req=urllib.request.Request(base+path,data=b,method=method,headers={'Content-Type':'application/json','Connection':'close'})
    try:
        with urllib.request.urlopen(req,timeout=timeout) as response: return response.status,json.load(response)
    except urllib.error.HTTPError as e: return e.code,json.load(e)
def save(name,value): (out/name).write_text(json.dumps(value,indent=2))
def wait(predicate,seconds=90):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        result=predicate()
        if result: return result
        if host.poll() is not None: raise AssertionError(f'Host exited {host.returncode}')
        time.sleep(.1)
    raise AssertionError('bounded fixture wait expired')
def children(parent):
    result=[]
    for path in (data/'sessions'/parent/'children').glob('*/session.json'):
        value=json.loads(path.read_text())
        if value.get('parent_session_id')==parent: result.append(value)
    return result
def completed(parent,run):
    status,snapshot=api('GET',f'/sessions/{parent}/workflow-runs/{run}')
    assert status==200,(status,snapshot)
    return snapshot if snapshot['status'] in ['succeeded','failed','cancelled','suspended'] else None
def verify_strict_receipt(kid, expected_status):
    metadata=kid['metadata']; source=json.loads(metadata['runtime.child_completion_source_v1'])
    usage=json.loads(metadata['workflow.agent_usage_observation.v1'])
    assert metadata['last_run_status']==source['status']==expected_status
    assert source['child_session_id']==usage['child_session_id']==kid['id']
    assert source['child_created_at']==usage['child_created_at']==kid['created_at']
    assert source['activation_run_id']==usage['activation_run_id']==metadata['workflow.agent_terminal_observation.v1']
    encode=lambda value, sort=False: json.dumps(value,ensure_ascii=False,separators=(',',':'),sort_keys=sort).encode()
    digest=lambda value: hashlib.sha256(encode(value)).hexdigest()
    assert source['input_sha256']==digest([row for row in kid['messages'] if row['role']=='user'])
    error=metadata.get('last_run_error')
    assert source['error_sha256']==(None if error is None else digest(error))
    assistant=next((row for row in reversed(kid['messages']) if row['role']=='assistant'),None)
    assert source['last_assistant']==(None if assistant is None else {'message_id':assistant['id'],'message_sha256':digest(assistant)})
    directory=data/'sessions'/kid['parent_session_id']/'children'/kid['id']
    ledger=json.loads((directory/'broker-terminal-receipts.v1.json').read_text())
    anchor=ledger['acknowledged_anchor']; proof=anchor['terminal_completeness']
    assert ledger['receipts']==[] and anchor['committed'] and anchor['terminal_status']==expected_status
    for record in [anchor,proof]:
        assert record['session_id']==kid['id'] and record['created_at']==kid['created_at']
        assert record['parent_session_id']==source['parent_session_id']==kid['parent_session_id']
        assert record['activation_run_id']==source['activation_run_id']
        assert record['message_count']==len(kid['messages'])
        assert record['messages_sha256']==base64.urlsafe_b64encode(hashlib.sha256(encode(kid['messages'],True)).digest()).decode().rstrip('=')
    assert proof['execution_epoch']==anchor['required_execution_epoch']>0
    assert proof['contiguous_applied_seq']>0
    assert anchor['message_ids'] and len(anchor['message_ids'])==len(set(anchor['message_ids']))
    assert not list((data/'broker'/'mailboxes'/('p-'+kid['id'])/'cur').glob('*.json'))
    save(expected_status+'-strict-receipt.json',ledger)
    return {'activation_run_id':source['activation_run_id'],'execution_epoch':proof['execution_epoch'],'contiguous_applied_seq':proof['contiguous_applied_seq'],'acknowledged_message_ids':anchor['message_ids'],'canonical_message_count':len(kid['messages'])}

result={'binary_sha256':hashlib.sha256(pathlib.Path(A.binary).read_bytes()).hexdigest(),'source_head':A.source_head,'fixture':'native serve/current_exe subagent-worker; loopback OpenAI HTTP/SSE','status':'running'}
try:
    def ready():
        try:
            req=urllib.request.Request(base+'/health',headers={'Connection':'close'})
            with urllib.request.urlopen(req,timeout=2) as response:
                response.read(); return response.status==200
        except (OSError,urllib.error.URLError): return False
    wait(ready,45)
    code,created=api('POST','/sessions',{'title':'Workflow production proof','model':'fixture-model','model_ref':{'provider':'openai','model':'fixture-model'},'workspace_path':str(data/'workspace')})
    save('create-session.json',{'http':code,'body':created}); assert code in [200,201],(code,created)
    parent=created.get('session',created)['id']; result['parent_id']=parent
    code,started=api('POST',f'/sessions/{parent}/workflow-runs',{'workflow_id':'agent-flow','revision':1,'args':{}}); save('success-start.json',{'http':code,'body':started}); assert code==202,(code,started)
    success=wait(lambda:completed(parent,started['run_id'])); save('success-status.json',success)
    assert success['status']=='succeeded',success
    assert success['usage']['tokens']==36 and success['usage']['cost_micros'] is None,success['usage']
    kids=children(parent); assert len(kids)==1,kids; kid=kids[0]; save('success-canonical-child.json',kid)
    usage=json.loads(kid['metadata']['workflow.agent_usage_observation.v1']); activation=json.loads(kid['metadata']['runtime.child_completion_source_v1'])['activation_run_id']
    assert usage['activation_run_id']==activation and usage['child_created_at']==kid['created_at'] and usage['child_session_id']==kid['id']; assert usage['prompt_tokens']==24 and usage['completion_tokens']==12,usage
    assert kid['metadata']['workflow.agent_terminal_observation.v1']==activation,kid['metadata']
    assert any(json.loads(m['content']).get('outcome')=='completed' for m in kid['messages'] if m['role']=='assistant' and m.get('content','').startswith('{'))
    with lock:
        success_requests=[r for r in requests if r['phase']=='success']; assert len(success_requests)==2
        assert 'WORKFLOW_READ_EVIDENCE' in json.dumps(success_requests[1]['body'])
        assert 'PINNED_PRIVATE_WORKFLOW_PROFILE' in json.dumps(success_requests[0]['body'])
        assert [t['function']['name'] for t in success_requests[0]['body']['tools']]==['Read']
    result['completed_strict_delivery']=verify_strict_receipt(kid,'completed')
    # A finite monetary cap is rejected before another Child/provider can be dispatched.
    budget=success['budget'].copy(); budget['max_cost_micros']=1
    code,money=api('POST',f'/sessions/{parent}/workflow-runs',{'workflow_id':'agent-flow','revision':1,'args':{},'budget':budget}); save('money-rejection.json',{'http':code,'body':money}); assert code==400 and money['code']=='workflow_monetary_budget_unsupported',(code,money)
    assert len(children(parent))==1 and len(requests)==2
    phase='cancel'
    code,started=api('POST',f'/sessions/{parent}/workflow-runs',{'workflow_id':'agent-flow','revision':1,'args':{}}); save('cancel-start.json',{'http':code,'body':started}); assert code==202,(code,started)
    assert connected.wait(45),'second actual provider request not entered'
    snapshot=api('GET','/subagents/snapshot'); save('cancel-before-subagents-snapshot.json',{'http':snapshot[0],'body':snapshot[1]})
    code,cancelled=api('POST',f'/sessions/{parent}/workflow-runs/{started["run_id"]}/cancel',{},timeout=25); cancel_receipt_time=time.monotonic(); save('cancel-response.json',{'http':code,'body':cancelled}); assert code==200,(code,cancelled)
    close_observed=disconnected.wait(5)
    save('cancel-provider-stop-timing.json',{'provider_closed':close_observed,'provider_disconnect_monotonic':disconnected_times[0] if disconnected_times else None,'cancel_http_response_monotonic':cancel_receipt_time})
    assert cancelled['status']=='cancelled' and cancelled['usage']['tokens']==18 and cancelled['usage']['cost_micros'] is None,cancelled
    assert close_observed,'actual provider connection not closed'
    assert disconnected_times[0]<=cancel_receipt_time,'provider close was not observed before cancel receipt'
    kids=children(parent); assert len(kids)==2
    kid=next(k for k in kids if k['id']!=result.get('success_child_id',json.loads((out/'success-canonical-child.json').read_text())['id'])); save('cancel-canonical-child.json',kid)
    activation=json.loads(kid['metadata']['runtime.child_completion_source_v1'])['activation_run_id']; usage=json.loads(kid['metadata']['workflow.agent_usage_observation.v1'])
    assert kid['metadata']['workflow.agent_terminal_observation.v1']==activation and usage['activation_run_id']==activation
    assert usage['child_session_id']==kid['id'] and usage['child_created_at']==kid['created_at']
    assert usage['prompt_tokens']==11 and usage['completion_tokens']==7,usage
    assert kid['metadata']['last_run_status']=='cancelled',kid['metadata'].get('last_run_error')
    assert len([row for row in kid['messages'] if row['role']=='tool' and row.get('tool_call_id')=='workflow-read-cancel' and 'WORKFLOW_READ_EVIDENCE' in row['content']])==1
    result['cancelled_strict_delivery']=verify_strict_receipt(kid,'cancelled')
    events=api('GET',f'/sessions/{parent}/workflow-runs/{started["run_id"]}/events?since=0'); save('cancel-events.json',{'http':events[0],'body':events[1]})
    snapshot=api('GET','/subagents/snapshot'); save('cancel-after-subagents-snapshot.json',{'http':snapshot[0],'body':snapshot[1]})
    result.update({'status':'pass','success_tokens':36,'cancelled_tokens':18,'provider_requests':len(requests),'provider_connection_closed_before_terminal_receipt':True,'cost_micros':None,'money_dispatch_rejected':True,'cancel_child_canonical_status':kid['metadata'].get('last_run_status'),'cancel_child_canonical_error':kid['metadata'].get('last_run_error'),'adjacent_strict_terminal_checkpoint_issue':1733,'provider_disconnect_monotonic':disconnected_times[0],'cancel_http_response_monotonic':cancel_receipt_time})
except BaseException as e:
    result.update({'status':'fail','error':repr(e),'traceback':traceback.format_exc()})
finally:
    hold.set(); host.terminate()
    try: host.wait(15)
    except subprocess.TimeoutExpired: host.kill(); host.wait()
    log.close()
    if result['status']=='pass':
        # Keep the four Child requests distinct from unrelated startup memory work.
        # The active Host/Worker above uses defaults; only this cold-read Host opts out.
        config['memory']={'auto_dream_enabled':False}
        (data/'config.json').write_text(json.dumps(config))
        result['cold_read_fixture_config']={'memory.auto_dream_enabled':False}
        cold_log=open(out/'cold-host.log','wb')
        s=socket.socket(); s.bind(('127.0.0.1',0)); port=s.getsockname()[1]; s.close(); base=f'http://127.0.0.1:{port}/api/v1'
        host=subprocess.Popen([A.binary,'serve','--bind','127.0.0.1','--port',str(port),'--data-dir',str(data)],cwd=data,env=env,stdout=cold_log,stderr=cold_log)
        try:
            wait(ready,45)
            for name,expected in [('success','completed'),('cancel','cancelled')]:
                canonical=json.loads((out/(name+'-canonical-child.json')).read_text())
                code,detail=api('GET',f'/sessions/{canonical["id"]}')
                save(name+'-cold-detail.json',{'http':code,'body':detail})
                assert code==200 and detail['session']['last_run_status']==expected
                assert detail['session']['created_at']==canonical['created_at'] and not detail['session']['is_running']
                code,history=api('GET',f'/sessions/{canonical["id"]}/history')
                save(name+'-cold-history.json',{'http':code,'body':history})
                assert code==200 and not history['truncated']
                assert [row['id'] for row in history['messages']]==[row['id'] for row in canonical['messages']]
                for cold,original in zip(history['messages'],canonical['messages']):
                    assert cold['role']==original['role'] and cold['content']==original['content']
                    assert cold.get('tool_call_id')==original.get('tool_call_id') and cold.get('tool_calls')==original.get('tool_calls')
            assert len(requests)==4
            result['fresh_host_storage_cold_read']='pass; both actual Storage detail/history routes, no execute'
        except BaseException as e:
            result.update({'status':'fail','cold_error':repr(e),'cold_traceback':traceback.format_exc()})
        finally:
            host.terminate()
            try: host.wait(15)
            except subprocess.TimeoutExpired: host.kill(); host.wait()
            cold_log.close()
    provider.shutdown(); save('provider-requests.json',requests); save('receipt.json',result)
print(json.dumps(result,indent=2)); raise SystemExit(0 if result['status']=='pass' else 1)
