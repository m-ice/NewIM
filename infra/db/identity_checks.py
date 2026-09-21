"""Negative identity fixtures complement real Docker integration. 身份负例不代替真实 daemon 验证。"""
from copy import deepcopy

from runtime import Failure, LOCK, locked_metadata, verify_inspected_image


def check_identity_contract():
    rejected = 0
    accepted = 0
    for target in LOCK['platforms']:
        locked = locked_metadata(target)
        classic = {'Id': locked['config'], 'Os': 'linux',
                   'Architecture': 'arm64' if target == 'linux/arm64/v8' else 'amd64',
                   'RootFS': {'Layers': locked['diff_ids']},
                   'RepoDigests': ['postgres@'+LOCK['index']]}
        if target == 'linux/arm64/v8':
            classic['Variant'] = 'v8'
        modern = dict(classic, Id=locked['manifest'], Descriptor={'digest': locked['manifest']})
        for actual in (classic, modern):
            verify_inspected_image(actual, target, locked)
            accepted += 1
        for alias in ('postgres', 'library/postgres', 'docker.io/library/postgres'):
            for digest in (LOCK['index'], locked['manifest']):
                verify_inspected_image(dict(classic, RepoDigests=[alias+'@'+digest]), target, locked)
                accepted += 1
        mutations = [
            {'Id': 'sha256:'+'0'*64}, {'Os': 'windows'}, {'Architecture': 'other'},
            {'RootFS': None}, {'RootFS': {'Layers': list(reversed(locked['diff_ids']))}},
            {'RootFS': {'Layers': locked['diff_ids'][:-1]}},
            {'Descriptor': None}, {'Descriptor': {}}, {'Descriptor': []},
            {'Descriptor': {'digest': LOCK['index']}},
            {'Descriptor': {'digest': 'sha256:'+'0'*64}},
        ]
        if target == 'linux/arm64/v8':
            mutations.append({'Variant': 'v7'})
        for source in (classic, modern):
            for mutation in mutations:
                bad = deepcopy(source)
                bad.update(mutation)
                try:
                    verify_inspected_image(bad, target, locked)
                except Failure:
                    rejected += 1
                else:
                    raise Failure('image identity negative fixture accepted')
        for mutation in [
            {'Id': locked['manifest']}, {'RepoDigests': []}, {'RepoDigests': None},
            {'RepoDigests': ['attacker.invalid/library/postgres@'+LOCK['index']]},
            {'RepoDigests': ['attacker/postgres@'+LOCK['index']]},
            {'RepoDigests': ['docker.io/library/postgres@sha256:'+'0'*64]},
            {'RepoDigests': [None]},
        ]:
            bad = dict(classic, **mutation)
            try:
                verify_inspected_image(bad, target, locked)
            except Failure:
                rejected += 1
            else:
                raise Failure('classic identity negative fixture accepted')
    print(f'PASS identity guards: {accepted} accepted / {rejected} rejected fixtures', flush=True)
