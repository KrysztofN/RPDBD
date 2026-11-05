"""Optional for live input: allow S2 `simulation` flag to be configured via env."""
from pathlib import Path
import sys
p=Path(sys.argv[1] if len(sys.argv)>1 else 's2_stream.py')
s=p.read_text()
if 'INCIDENTS_SIMULATION' in s: raise SystemExit('Already patched')
old='simulation=True))'
if old not in s: raise SystemExit('Original S2 simulation marker differs; no file changed')
s=s.replace('import time\n','import time\nimport os\n',1)
s=s.replace(old,"simulation=os.getenv('INCIDENTS_SIMULATION','1') == '1'))")
backup=p.with_suffix('.py.pre_incidents_backup')
if backup.exists(): raise SystemExit('Backup exists; refusing overwrite')
backup.write_text(p.read_text());p.write_text(s)
print('S2 updated. Add INCIDENTS_SIMULATION in compose.yaml x-common.environment')
