#!/usr/bin/env python3
"""Explicit verified source preparation and offline native compilation."""
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import urllib.request
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
LOCK = json.loads((HERE / 'source-lock.json').read_text())
CACHE = ROOT / 'target' / 'newim-sqlite'
SOURCE = CACHE / LOCK['archive_sha256']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def command(args, **kwargs):
    print('+', ' '.join(map(str, args)), flush=True)
    return subprocess.run(args, check=True, timeout=180, **kwargs)


def prepare():
    CACHE.mkdir(parents=True, exist_ok=True)
    if SOURCE.exists():
        verify_source()
        print('verified cached SQLite source')
        return
    with urllib.request.urlopen(LOCK['url'], timeout=60) as response:
        archive = response.read(8 * 1024 * 1024 + 1)
    if hashlib.sha256(archive).hexdigest() != LOCK['archive_sha256']:
        raise RuntimeError('SQLite archive checksum mismatch')
    with tempfile.TemporaryDirectory(prefix='prepare-', dir=CACHE) as directory:
        tmp = Path(directory)
        with zipfile.ZipFile(io.BytesIO(archive)) as bundle:
            for name in ['sqlite3.c', 'sqlite3.h', 'sqlite3ext.h']:
                data = bundle.read('sqlite-amalgamation-3530400/' + name)
                (tmp / name).write_bytes(data)
        if hashlib.sha3_256((tmp / 'sqlite3.c').read_bytes()).hexdigest() != LOCK['sqlite3_c_sha3_256']:
            raise RuntimeError('SQLite source checksum mismatch')
        manifest = {name: digest(tmp / name) for name in ['sqlite3.c', 'sqlite3.h', 'sqlite3ext.h']}
        if manifest != LOCK['files_sha256']:
            raise RuntimeError('SQLite headers/source checksum mismatch')
        (tmp / 'files.json').write_text(json.dumps(manifest, sort_keys=True))
        # An exclusive destination avoids replacing another preparer's source.
        os.rename(tmp, SOURCE)
    verify_source()


def verify_source():
    if not SOURCE.is_dir():
        raise RuntimeError('SQLite source missing; run make store-prepare explicitly')
    for name, expected in LOCK['files_sha256'].items():
        if name not in {'sqlite3.c', 'sqlite3.h', 'sqlite3ext.h'} or digest(SOURCE / name) != expected:
            raise RuntimeError('SQLite source cache checksum mismatch')
    if hashlib.sha3_256((SOURCE / 'sqlite3.c').read_bytes()).hexdigest() != LOCK['sqlite3_c_sha3_256']:
        raise RuntimeError('SQLite source identity mismatch')


def configuration():
    if platform.system() not in {'Darwin', 'Linux'}:
        raise RuntimeError('Native toolchain not verified for this host')
    cc = Path('/usr/bin/cc')
    ar = Path('/usr/bin/ar')
    if not cc.exists() or not ar.exists():
        raise RuntimeError('C compiler and archiver required')
    compiler = subprocess.check_output([str(cc), '--version'], text=True, timeout=10)
    target = subprocess.check_output(['rustc', '-vV'], text=True, timeout=10).split('host: ', 1)[1].splitlines()[0]
    config = {'target': target, 'compiler': compiler, 'compiler_path': str(cc.resolve()), 'flags': LOCK['flags'], 'source': LOCK['archive_sha256']}
    key = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
    return CACHE / target / key, config, cc, ar


def build():
    verify_source()
    dest, config, cc, ar = configuration()
    if (dest / 'engine.json').exists():
        verify_engine(dest)
        return
    dest.mkdir(parents=True, exist_ok=True)
    command([str(cc), *LOCK['flags'], '-c', str(SOURCE / 'sqlite3.c'), '-o', str(dest / 'sqlite3.o')])
    command([str(ar), 'crs', str(dest / 'libsqlite3.a'), str(dest / 'sqlite3.o')])
    probe = dest / 'probe.c'
    probe.write_text('#include "sqlite3.h"\n#include <string.h>\n#include <stdio.h>\nint main(void) {\n'
        'if(sqlite3_libversion_number()!=3053004 || strcmp(sqlite3_sourceid(),'+json.dumps(LOCK['source_id'])+')) return 1;\n'
        'if(!sqlite3_compileoption_used("OMIT_LOAD_EXTENSION") || !sqlite3_compileoption_used("THREADSAFE=1") || !sqlite3_compileoption_used("DQS=0")) return 2;\n'
        'printf("SQLite %s %s\\n",sqlite3_libversion(),sqlite3_sourceid()); return 0; }\n')
    command([str(cc), '-O2', '-I', str(SOURCE), str(probe), str(dest / 'libsqlite3.a'), '-lm', '-lpthread', '-o', str(dest / 'probe')])
    command([str(dest / 'probe')])
    config['files'] = {name: digest(dest / name) for name in ['libsqlite3.a', 'probe']}
    (dest / 'engine.json').write_text(json.dumps(config, sort_keys=True, indent=2)+'\n')
    (dest / 'identity').write_text(LOCK['source_id']+'\n')


def verify_engine(dest):
    if not (dest / 'engine.json').is_file():
        raise RuntimeError('Native SQLite engine missing; run make store-engine explicitly')
    record = json.loads((dest / 'engine.json').read_text())
    for name in ['libsqlite3.a', 'probe']:
        if digest(dest / name) != record['files'][name]:
            raise RuntimeError('Native SQLite cache checksum mismatch')
    command([str(dest / 'probe')])


def run(args):
    verify_source()
    dest, _, _, _ = configuration()
    verify_engine(dest)
    if not args or args[0] != 'cargo' or '--locked' not in args or '--offline' not in args:
        raise RuntimeError('Native runner requires cargo --locked --offline')
    env = {key: value for key, value in os.environ.items() if not key.startswith(('SQLITE3_', 'SQLCIPHER_', 'LIBSQLITE3_'))}
    env.update(SQLITE3_LIB_DIR=str(dest), SQLITE3_INCLUDE_DIR=str(SOURCE), SQLITE3_STATIC='1', SQLITE3_NO_PKG_CONFIG='1', NEWIM_SQLITE_ENGINE=str(dest))
    # No shell interpolation and no network fetch in this path.
    command(args, cwd=ROOT, env=env)


if __name__ == '__main__':
    try:
        mode = sys.argv[1]
        if mode == 'prepare': prepare()
        elif mode == 'build': build()
        elif mode == 'run': run(sys.argv[2:])
        else: raise RuntimeError('expected prepare, build or run')
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print('SQLite build failed:', str(error), file=sys.stderr)
        sys.exit(1)
