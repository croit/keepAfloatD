#!/usr/bin/env python3
"""Whole-daemon regression; invoked only inside the private namespace wrapper."""
import subprocess as sp,tempfile,pathlib,json,time,re,signal,os,shutil,math,sys,select

PROC_ROOT = pathlib.Path('/proc')
NETNS_ROOT = pathlib.Path('/run/netns')

def require_pidfd_support():
 if not callable(getattr(os, 'pidfd_open', None)) or not callable(getattr(signal, 'pidfd_send_signal', None)):
  raise RuntimeError('proof cleanup requires Python pidfd APIs')
 handle = os.pidfd_open(os.getpid())
 try: signal.pidfd_send_signal(handle, 0)
 finally: os.close(handle)

def atomic_record(path, value):
 temporary = path.with_name(path.name + '.tmp')
 temporary.write_text(value)
 temporary.replace(path)

def process_identity(pid):
 if not isinstance(pid, int) or pid <= 1: raise ValueError('invalid owned pid')
 fields = (PROC_ROOT / str(pid) / 'stat').read_text().rsplit(') ', 1)[1].split()
 return dict(pid=pid, group=int(fields[2]), session=int(fields[3]), start=int(fields[19]))

def namespace_identity(path):
 metadata = path.stat()
 return [metadata.st_dev, metadata.st_ino]

def owned_namespace(work):
 return 'kafd-proof-' + work.name

def record_process(work, pid):
 identity = process_identity(pid)
 if identity['group'] != pid or identity['session'] != pid:
  raise RuntimeError('proof supervisor must own its session')
 atomic_record(work / 'process.json', json.dumps(identity))

def supervisor_alive(work):
 expected = json.loads((work / 'process.json').read_text())
 return process_identity(expected['pid']) == expected

def close_handles(handles):
 for handle in handles: os.close(handle)

def signal_handle(handle, signum):
 try: signal.pidfd_send_signal(handle, signum)
 except ProcessLookupError: pass

def wait_handles(handles, seconds):
 pending = set(handles)
 deadline = time.monotonic() + seconds
 while pending:
  remaining = max(0, deadline - time.monotonic())
  ready, _, _ = select.select(list(pending), [], [], remaining)
  pending.difference_update(ready)
  if not ready: break
 return pending

def terminate_supervisor(work):
 record = work / 'process.json'
 if not record.exists():
  if (work / 'pid').exists(): raise RuntimeError('missing supervisor generation')
  return
 expected = json.loads(record.read_text())
 if not supervisor_alive(work): raise RuntimeError('supervisor generation changed')
 handles = []
 try:
  for entry in PROC_ROOT.iterdir():
   if not entry.name.isdecimal() or int(entry.name) <= 1: continue
   try:
    identity = process_identity(int(entry.name))
    if identity['group'] != expected['pid'] or identity['session'] != expected['pid']: continue
    handle = os.pidfd_open(identity['pid'])
    try:
     if process_identity(identity['pid']) != identity or not supervisor_alive(work):
      raise RuntimeError('process generation changed while pinning cleanup')
    except BaseException:
     os.close(handle)
     raise
    handles.append(handle)
   except (FileNotFoundError, ProcessLookupError):
    continue
  # pidfds retain the selected processes even if their numeric PIDs are later reused.
  for handle in handles: signal_handle(handle, signal.SIGTERM)
  pending = wait_handles(handles, 5)
  for handle in pending: signal_handle(handle, signal.SIGKILL)
  if wait_handles(pending, 2): raise RuntimeError('proof supervisor did not stop')
 finally:
  close_handles(handles)

def create_namespace(work):
 require_pidfd_support()
 namespace = owned_namespace(work)
 path = NETNS_ROOT / namespace
 if path.exists() or (work / 'namespace.json').exists():
  raise RuntimeError('proof namespace already exists')
 sp.run(['ip', 'netns', 'add', namespace], check=True, timeout=5)
 identity = namespace_identity(path)
 atomic_record(work / 'namespace.json', json.dumps(dict(name=namespace, identity=identity)))
 return namespace

def cleanup_namespace(work):
 namespace = owned_namespace(work)
 path = NETNS_ROOT / namespace
 marker = work / 'namespace.json'
 if not marker.exists():
  if path.exists(): raise RuntimeError('namespace has no creation receipt')
  return
 record = json.loads(marker.read_text())
 if record['name'] != namespace or namespace_identity(path) != record['identity']:
  raise RuntimeError('namespace identity changed')
 namespace_fd = os.open(path, os.O_RDONLY)
 handles = []
 try:
  pinned = os.fstat(namespace_fd)
  if [pinned.st_dev, pinned.st_ino] != record['identity']:
   raise RuntimeError('namespace replaced while opening')
  result = sp.run(['ip', 'netns', 'pids', namespace], check=True,
                  capture_output=True, text=True, timeout=5)
  for value in result.stdout.split():
   if not value.isdecimal() or int(value) <= 1: raise RuntimeError('invalid namespace pid')
   try:
    identity = process_identity(int(value))
    handle = os.pidfd_open(int(value))
    try:
     if process_identity(int(value)) != identity or namespace_identity(PROC_ROOT / value / 'ns/net') != record['identity']:
      raise RuntimeError('process no longer belongs to owned namespace')
    except BaseException:
     os.close(handle)
     raise
    handles.append(handle)
   except (FileNotFoundError, ProcessLookupError):
    continue
  if namespace_identity(path) != record['identity']: raise RuntimeError('namespace replaced')
  for handle in handles: signal_handle(handle, signal.SIGKILL)
  if wait_handles(handles, 2): raise RuntimeError('namespace processes did not stop')
  if namespace_identity(path) != record['identity']: raise RuntimeError('namespace replaced')
  sp.run(['ip', 'netns', 'del', namespace], check=True, timeout=5)
  marker.unlink()
 finally:
  close_handles(handles)
  os.close(namespace_fd)

def proof_control(action, directory=None, *args):
 if action == 'preflight':
  require_pidfd_support()
  return
 work = pathlib.Path(directory)
 if not re.fullmatch(r'kafd-proof-(?:campaign|local)\.[A-Za-z0-9]+', work.name) or work.is_symlink():
  raise ValueError('invalid proof control directory')
 if action == 'record-process': record_process(work, int(args[0]))
 elif action == 'create-namespace': print(create_namespace(work))
 elif action == 'cleanup-namespace': cleanup_namespace(work)
 elif action == 'alive':
  if not supervisor_alive(work): raise RuntimeError('supervisor generation changed')
 elif action == 'cleanup':
  try:
   if not (work / 'status').exists(): terminate_supervisor(work)
  finally:
   cleanup_namespace(work)
  shutil.rmtree(work)
 else: raise ValueError('unknown proof control operation')

if len(sys.argv) > 1 and sys.argv[1] == '--proof-control':
 proof_control(*sys.argv[2:])
 sys.exit(0)

binary=os.environ['KEEP_AFLOATD_BIN']
work=pathlib.Path(tempfile.mkdtemp(prefix='kafd-isolated-health-'))
processes={}; logs={}
def interrupted(signum, frame):
 raise TimeoutError(f'isolated health proof interrupted by signal {signum}')
signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
signal.signal(signal.SIGALRM, interrupted)
signal.alarm(80)

def current_boot(text):
 return text[text.rfind('runtime admission timing'):] if 'runtime admission timing' in text else text

def safety_wait(text):
 lines=[line for line in text.splitlines() if 'runtime admission timing' in line]
 if not lines: raise ValueError('missing runtime admission timing')
 match=re.search(r'\bstartup_safety_wait_ms=(0|[1-9][0-9]{0,11})(?:\s|$)',lines[-1])
 if not match: raise ValueError('invalid runtime admission timing')
 return int(match[1])/1000

def assert_unique_states(current):
 for vip in set().union(*current.values()):
  owners=[i for i,addresses in current.items() if vip in addresses]
  assert len(owners)<=1, f'VIP {vip} overlaps on {owners}'

def observe_isolated_failure(processes, states, victim, victim_vip, clock=time):
 started=clock.monotonic()
 while clock.monotonic()-started<42:
  current=states()
  elapsed=clock.monotonic()-started
  assert all(p.poll() is None for i,p in processes.items() if i!=victim), 'fencing stopped a majority daemon'
  status=processes[victim].poll()
  assert status in (None,1), 'isolated daemon stopped with an unexpected status'
  if elapsed>=7 or status is not None:
   assert not current[victim], 'isolated daemon did not fence while its health probe was blocked'
  assert_unique_states(current)
  if elapsed>=7 and status==1 and sum(victim_vip in v for i,v in current.items() if i!=victim)==1: break
  clock.sleep(.05)
 current=states()
 assert not current[victim], 'isolated daemon did not fence while its health probe was blocked'
 assert sum(victim_vip in v for i,v in current.items() if i!=victim)==1, 'survivors did not take over'
 assert processes[victim].poll()==1, 'expired isolated daemon did not stop'
 assert all(p.poll() is None for i,p in processes.items() if i!=victim), 'fencing stopped a majority daemon'
 assert_unique_states(current)

def start_process(i):
 logs[i]=open(work/f'{i}.log','a')
 processes[i]=sp.Popen([binary,'-c',str(work/f'{i}.json')],stdout=logs[i],stderr=sp.STDOUT,start_new_session=True)

def startup_budget(nodes):
 until=time.monotonic()+5
 while time.monotonic()<until:
  try: return max(safety_wait((work/f'{i}.log').read_text()) for i in nodes)+12
  except ValueError: time.sleep(.05)
 raise AssertionError('daemon did not report its startup safety policy')
def states():
 out={}
 for i in processes:
  active=set()
  for line in (work/f'{i}.log').read_text().splitlines():
   m=re.search(r'dry-run: would (bind|unbind) (192\.0\.2\.\d+)/',line)
   if m:
    if m[1]=='bind': active.add(m[2])
    else: active.discard(m[2])
  out[i]=active
 return out
def leader():
 ids=[]
 for i in processes:
  m=re.findall(r'raft current leader is now (Some\(ReplicaId \{ physical_id: \d+, boot_nonce: \[[0-9, ]+\] \}\)|None)',current_boot((work/f'{i}.log').read_text()))
  if not m or m[-1]=='None': return None
  ids.append(m[-1])
 return int(re.search(r'physical_id: (\d+)',ids[0])[1]) if len(set(ids))==1 else None
try:
 for i in [1,2,3]:
  peers=[{'id':j,'raft_address':f'127.0.0.{10+j}:19210','client_submit_address':f'127.0.0.{10+j}:19211'} for j in [1,2,3]]
  cfg={'node_id':i,'raft_listen':peers[i-1]['raft_address'],'client_submit_listen':peers[i-1]['client_submit_address'],
       'cluster_secret':'local-isolated-regression-secret-012345','peers':peers,
       'vips':[{'address':f'192.0.2.{100+j}','interface':'lo'} for j in [1,2,3]],
       'health':{'command':['/bin/bash','-c',f'if test -f {work}/{i}.slow; then touch {work}/{i}.entered; sleep 4; fi'],'interval_ms':500,'timeout_ms':900,'stale_secs':1},
       'submit_timeout_ms':2000,'failback_delay_secs':0,'dry_run':True}
  if i==1:
   invalid=work/'invalid.json'
   for invalid_timeout in [1000,5000]:
    cfg['health']['timeout_ms']=invalid_timeout
    invalid.write_text(json.dumps(cfg))
    result=sp.run([binary,'-c',str(invalid)],capture_output=True,text=True,timeout=2)
    expected=f'health.timeout_ms ({invalid_timeout} ms) must be less than the effective stale window (1000 ms)'
    assert result.returncode==1 and expected in result.stderr, (result.returncode,result.stdout,result.stderr)
    assert 'dry-run:' not in result.stdout+result.stderr, 'invalid timing reached VIP startup effects'
   cfg['health']['timeout_ms']=900
   print('PASS: startup rejected equal and excessive health timeouts',flush=True)
  (work/f'{i}.json').write_text(json.dumps(cfg))
  start_process(i)
 baseline_budget=startup_budget(processes)
 if 'KAFD_PROOF_CONTROL_DIR' in os.environ:
  atomic_record(pathlib.Path(os.environ['KAFD_PROOF_CONTROL_DIR']) / 'policy',
                str(round((baseline_budget-12)*1000)) + '\n')
 # Retain the original work budget and add only two observed startup fences.
 signal.alarm(80+2*math.ceil(baseline_budget-12))
 until=time.monotonic()+baseline_budget
 while time.monotonic()<until:
  victim=leader()
  baseline=states()
  assert_unique_states(baseline)
  if victim and all(len(v)==1 for v in baseline.values()) and len(set.union(*baseline.values()))==3: break
  if any(p.poll() is not None for p in processes.values()): raise RuntimeError('daemon exited')
  time.sleep(.05)
 else: raise RuntimeError('baseline failed')
 print('baseline',states(),'leader',victim,flush=True)
 (work/f'{victim}.slow').touch()
 until=time.monotonic()+2
 while not (work/f'{victim}.entered').exists() and time.monotonic()<until: time.sleep(.01)
 assert (work/f'{victim}.entered').exists()
 address=f'127.0.0.{10+victim}'
 for selector in ['-s','-d']:
  sp.run(['iptables','-A','OUTPUT',selector,address,'-j','DROP'],check=True,timeout=2)
 victim_vip=next(iter(states()[victim]))
 observe_isolated_failure(processes,states,victim,victim_vip)
 victim_log=current_boot((work/f'{victim}.log').read_text())
 assert re.search(r'runtime admission.*(expired|sealed)',victim_log), 'missing terminal admission expiry evidence'
 print('PASS: isolated leader withdrew and stopped; survivors uniquely acquired its VIP',flush=True)
 (work/f'{victim}.slow').unlink()
 for selector in ['-s','-d']:
  sp.run(['iptables','-D','OUTPUT',selector,address,'-j','DROP'],check=True,timeout=2)
 logs[victim].close()
 start_process(victim)
 until=time.monotonic()+startup_budget([victim])
 while time.monotonic()<until:
  current=states()
  assert_unique_states(current)
  assert all(p.poll() is None for p in processes.values()), 'daemon stopped during recovery'
  if leader() and all(len(v)==1 for v in current.values()) and len(set.union(*current.values()))==3:
   break
  time.sleep(.05)
 else: raise AssertionError('cluster did not recover after the blocked probe and partition cleared')
 print('PASS: restarted member rejoined the live majority with unique balanced ownership',flush=True)
 print('final',states(),flush=True)
except BaseException:
 for i in processes:
  print(f'node {i} diagnostic tail:', flush=True)
  print('\n'.join((work/f'{i}.log').read_text().splitlines()[-30:]), flush=True)
 raise
finally:
 for p in processes.values():
  if p.poll() is None: os.killpg(p.pid,signal.SIGTERM)
 for p in processes.values():
  try: p.wait(timeout=4)
  except sp.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL); p.wait(timeout=2)
 for f in logs.values(): f.close()
 shutil.rmtree(work)
 signal.alarm(0)
