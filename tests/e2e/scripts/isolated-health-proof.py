#!/usr/bin/env python3
"""Whole-daemon regression; invoked only inside the private namespace wrapper."""
import subprocess as sp,tempfile,pathlib,json,time,re,signal,os,shutil
binary=os.environ['KEEP_AFLOATD_BIN']
work=pathlib.Path(tempfile.mkdtemp(prefix='kafd-isolated-health-'))
processes={}; logs={}; duplicate=False
def interrupted(signum, frame):
 raise TimeoutError(f'isolated health proof interrupted by signal {signum}')
signal.signal(signal.SIGTERM, interrupted)
signal.signal(signal.SIGINT, interrupted)
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
  m=re.findall(r'raft current leader is now (Some\(\d+\)|None)',(work/f'{i}.log').read_text())
  if not m or m[-1]=='None': return None
  ids.append(int(re.search(r'\d+',m[-1])[0]))
 return ids[0] if len(set(ids))==1 else None
try:
 for i in [1,2,3]:
  peers=[{'id':j,'raft_address':f'127.0.0.{10+j}:19210','client_submit_address':f'127.0.0.{10+j}:19211'} for j in [1,2,3]]
  cfg={'node_id':i,'raft_listen':peers[i-1]['raft_address'],'client_submit_listen':peers[i-1]['client_submit_address'],
       'cluster_secret':'local-isolated-regression-secret','peers':peers,
       'vips':[{'address':f'192.0.2.{100+j}','interface':'lo'} for j in [1,2,3]],
       'health':{'command':['/bin/bash','-c',f'if test -f {work}/{i}.slow; then touch {work}/{i}.entered; sleep 4; fi'],'interval_ms':500,'timeout_ms':5000,'stale_secs':1},
       'submit_timeout_ms':2000,'failback_delay_secs':0,'dry_run':True}
  (work/f'{i}.json').write_text(json.dumps(cfg))
  logs[i]=open(work/f'{i}.log','w')
  processes[i]=sp.Popen([binary,'-c',str(work/f'{i}.json')],stdout=logs[i],stderr=sp.STDOUT,start_new_session=True)
 until=time.monotonic()+12
 while time.monotonic()<until:
  victim=leader()
  baseline=states()
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
 started=time.monotonic(); duplicate=False
 while time.monotonic()-started<7:
  current=states()
  for vip in current[victim]:
   owners=[i for i,v in current.items() if vip in v]
   if len(owners)>1:
    print('DUPLICATE_DRY_RUN',round(time.monotonic()-started,2),vip,owners,flush=True); duplicate=True; break
  if duplicate: break
  time.sleep(.05)
 current=states()
 assert not current[victim], 'isolated daemon did not fence while its health probe was blocked'
 assert sum(victim_vip in v for i,v in current.items() if i != victim)==1, 'survivors did not take over'
 assert all(p.poll() is None for p in processes.values()), 'fencing stopped a daemon'
 print('PASS: isolated leader withdrew; survivors uniquely acquired its VIP',flush=True)
 (work/f'{victim}.slow').unlink()
 for selector in ['-s','-d']:
  sp.run(['iptables','-D','OUTPUT',selector,address,'-j','DROP'],check=True,timeout=2)
 until=time.monotonic()+12
 while time.monotonic()<until:
  current=states()
  if leader() and all(len(v)==1 for v in current.values()) and len(set.union(*current.values()))==3:
   break
  time.sleep(.05)
 else: raise AssertionError('cluster did not recover after the blocked probe and partition cleared')
 print('PASS: original daemons recovered with unique balanced ownership',flush=True)
 print('duplicate',duplicate,'final',states(),flush=True)
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
assert not duplicate, 'isolated leader retained a VIP after a survivor acquired it'
