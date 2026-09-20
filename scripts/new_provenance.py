#!/usr/bin/env python3
from pathlib import Path
import re, sys, datetime
if len(sys.argv)<2: raise SystemExit('usage: new_provenance.py component-name')
name=re.sub(r'[^a-zA-Z0-9._-]+','-',sys.argv[1]).strip('-').lower()
root=Path(__file__).resolve().parents[1]
out=root/'third_party/provenance'/f'{name}.md'; out.parent.mkdir(parents=True,exist_ok=True)
if out.exists(): raise SystemExit(f'exists: {out}')
tpl=(root/'templates/provenance.md').read_text(encoding='utf-8')
out.write_text(tpl+f'\nCreated: {datetime.date.today().isoformat()}\n',encoding='utf-8')
print(out)
