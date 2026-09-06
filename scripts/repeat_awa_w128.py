"""Repeat W128 with reversed pair order using the original archived binaries."""
import argparse
import contextlib
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import sys
ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
sys.path.insert(0, str(ROOT))
from bench_harness.orchestrator import drive
from bench_harness.phases import parse_phase_spec
from bench_harness.versions import verify_awa_build

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', type=Path, required=True)
parser.add_argument('--candidate', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
root = args.output
root.mkdir(parents=True, exist_ok=False)
binaries = {'baseline': args.baseline.resolve(), 'candidate': args.candidate.resolve()}
for config in ('postgres.conf', 'docker-compose.yml', 'docker-compose.override.yml'):
 if (ROOT / config).exists(): shutil.copy2(ROOT / config, root / config)
run={'started_at':datetime.now(timezone.utc).isoformat(),'status':'running','builds':{k:verify_awa_build(v,match_inputs=False) for k,v in binaries.items()},'cells':[]}
def save(): (root/'campaign.json').write_text(json.dumps(run,indent=2)+'\n')
save()
try:
 for pair,order in [(2,('candidate','baseline')),(3,('baseline','candidate'))]:
  for label in order:
   name=f'pair{pair}-{label}'
   run['active_cell']=name;save();print(name,flush=True)
   os.environ['AWA_BENCH_EXECUTABLE']=str(binaries[label])
   specs=['warmup=warmup:60s','clean=clean:180s']
   cli=['env',f'AWA_BENCH_EXECUTABLE={binaries[label]}','bench','run','--systems','awa','--skip-build','--pg-image','postgres:18.3-alpine','--worker-count','128','--producer-rate','50000','--producer-mode','depth-target','--target-depth','4000']
   for spec in specs:cli.extend(['--phase',spec])
   with (root/f'{name}.log').open('w') as log,contextlib.redirect_stderr(log):
    result=drive(systems=['awa'],scenario=None,phases=[parse_phase_spec(s) for s in specs],pg_image='postgres:18.3-alpine',fast=False,skip_build=True,sample_every_s=5,producer_rate=50000,producer_mode='depth-target',target_depth=4000,worker_count=128,high_load_multiplier=1.5,awa_completion_batch_size=None,replicas=1,cli_args=cli)
   manifest=json.loads((result/'manifest.json').read_text())
   assert manifest['adapters']['awa']['revision']['runtime_storage']['ring_authority']=='ledger'
   shutil.move(str(result),str(root/name))
   run['cells'].append({'name':name,'path':name,'status':'complete'});save()
 run['status']='complete';run.pop('active_cell',None)
except BaseException as error:
 run['status']='failed';run['error']=repr(error);raise
finally:
 run['updated_at']=datetime.now(timezone.utc).isoformat();save()
