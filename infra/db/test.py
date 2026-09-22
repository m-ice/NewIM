"""Real PostgreSQL acceptance suites. 每个套件使用独立持久卷，不连接用户数据库。"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
import uuid

from migrate import migration_sql
from identity_checks import check_identity_contract
from runtime import Commands, Database, DB_DIR, Failure, MAXIMUM, ROOT, image_identity, locked_metadata, verify_inspected_image


def equal(actual, expected, label):
    if actual != expected:
        raise Failure(label + ': assertion failed')


def scalar(db, sql, expected, label):
    equal(db.sql(sql).strip(), str(expected), label)


BASE_TABLES = {'im_users', 'im_devices', 'im_sessions', 'im_conversations',
               'im_conversation_members', 'im_messages', 'im_read_states', 'im_blocks'}
DURABILITY_TABLES = {'im_outbox_events', 'im_webhook_deliveries', 'im_push_tokens'}
SYNC_TABLES = {'im_conversation_sync_accounts', 'im_conversation_sync_keys',
               'im_conversation_sync_changes'}
AUTH_TABLES = {'im_auth_tokens'}


def source_migrations():
    sources = sorted((DB_DIR/'migrations').glob('[0-9][0-9][0-9]_*.sql'))
    equal([int(p.name[:3]) for p in sources], list(range(1, len(sources)+1)),
          'source migration versions contiguous')
    return sources


def check_ledger(db, target=None):
    sources = source_migrations()
    target = len(sources) if target is None else target
    expected = [f'{i}|{hashlib.sha256(p.read_bytes()).hexdigest()}'
                for i, p in enumerate(sources[:target], 1)]
    equal(db.sql("SELECT version::text||'|'||sha256 FROM newim_meta.migrations ORDER BY version;").splitlines(),
          expected, 'complete ledger matches immutable source bytes')


def check_catalog(db, target=None):
    target = len(source_migrations()) if target is None else target
    expected = BASE_TABLES | (DURABILITY_TABLES if target >= 2 else set())
    expected |= SYNC_TABLES if target >= 3 else set()
    expected |= AUTH_TABLES if target >= 4 else set()
    equal(set(db.sql("SELECT tablename FROM pg_tables WHERE schemaname='newim';").splitlines()),
          expected, 'exact catalog table set')
    scalar(db, "SELECT count(*) FROM pg_constraint WHERE connamespace='newim'::regnamespace AND contype='f' AND confdeltype<>'a';",
           0, 'no cascading deletes')
    functions = db.sql("SELECT p.oid::regprocedure::text FROM pg_proc p WHERE pronamespace='newim'::regnamespace ORDER BY 1;").splitlines()
    expected_functions = ['newim.persist_message(newim.identifier,newim.identifier,newim.identifier,newim.identifier,newim.message_type,integer,bigint,bytea,newim.identifier)'] if target >= 2 else []
    equal(functions, expected_functions, 'exact function signatures')
    privileges = db.sql("SELECT has_function_privilege('public',p.oid,'EXECUTE') FROM pg_proc p WHERE pronamespace='newim'::regnamespace ORDER BY p.oid::regprocedure::text;").splitlines()
    equal(privileges, ['f']*len(expected_functions), 'every function denies public execute')
    scalar(db, "SELECT has_schema_privilege('public','newim','USAGE');", 'f', 'schema denies public usage')
    scalar(db, "SELECT count(*) FROM pg_trigger WHERE tgrelid IN (SELECT oid FROM pg_class WHERE relnamespace='newim'::regnamespace) AND NOT tgisinternal;",
           0, 'no application triggers')
    check_ledger(db, target)


def seed(db):
    db.sql("INSERT INTO newim.im_users(user_id) VALUES ('alice'),('bob'); "
           "INSERT INTO newim.im_conversations(conversation_id) VALUES ('room'),('other'); "
           "INSERT INTO newim.im_conversation_members VALUES ('room','alice'),('room','bob');")


def persist(client, *, room='room', server=None, event=None, sender='alice'):
    # Inputs are fixed test identifiers, never user/query text.
    server, event = server or 's_'+client, event or 'e_'+client
    return (f"SELECT conversation_seq FROM newim.persist_message('{sender}','{client}',"
            f"'{room}','{server}','text',1,123,convert_to('{{\"text\":\"test\"}}','UTF8'),'{event}');")


def snapshot(db):
    # Stable hashes cover ALL application/ledger rows without emitting message data.
    tables = db.sql("SELECT tablename FROM pg_tables WHERE schemaname IN ('newim','newim_meta') "
                    "ORDER BY schemaname,tablename;").splitlines()
    result = {}
    for table in tables:
        schema = 'newim_meta' if table == 'migrations' else 'newim'
        result[table] = db.sql(f"SELECT md5(COALESCE(string_agg(t::text,E'\\n' ORDER BY t::text),'')) "
                               f"FROM {schema}.{table} t;").strip()
    return result


def codec_roundtrips(db, commands):
    binary = commands.directory / 'codec-test'
    commands.run(['go', 'build', '-trimpath', '-o', str(binary), './infra/db/tests/codec'],
                 timeout=120, label='build-real-codec-bridge')
    fixtures = json.loads((ROOT/'core/protocol/fixtures/cases.json').read_text())
    wires = [f['wire'] for f in fixtures if not f.get('error')]
    # Escaped NUL is legal in known text; Python never parses opaque numeric payloads.
    nul = json.loads(wires[0])
    nul['payload'] = {'text': '\x00'}
    wires.append(json.dumps(nul, ensure_ascii=True))
    for wire in wires:
        raw = commands.run([str(binary), 'decode'], data=wire.encode(), label='codec-decode').stdout
        row = json.loads(raw)
        payload = base64.b64decode(row['payload_base64'])
        # Codec has already checked ASCII grammar; hex carries opaque raw bytes.
        db.sql("TRUNCATE newim.im_users CASCADE;")
        db.sql(f"INSERT INTO newim.im_users(user_id) VALUES ('{row['sender_id']}'); "
               f"INSERT INTO newim.im_conversations(conversation_id) VALUES ('{row['conversation_id']}'); "
               "INSERT INTO newim.im_messages VALUES "
               f"('{row['server_id']}','{row['sender_id']}','{row['client_id']}',"
               f"'{row['conversation_id']}',{row['seq']},{row['protocol_version']},{row['schema_version']},"
               f"'{row['type']}',{row['time']},decode('{payload.hex()}','hex'));")
        loaded = json.loads(db.sql("SELECT json_build_object('protocol_version',protocol_version,"
                                   "'schema_version',schema_version,'client_id',client_msg_id,"
                                   "'server_id',server_msg_id,'conversation_id',conversation_id,"
                                   "'seq',conversation_seq::text,'sender_id',sender_id,'type',message_type,"
                                   "'time',server_time_ms::text,'payload_base64',"
                                   "replace(encode(payload_bytes,'base64'),E'\\n','')) FROM newim.im_messages;"))
        equal(loaded, row, 'all relational envelope fields and raw bytes preserved')
        encoded = commands.run([str(binary), 'encode'], data=json.dumps(loaded).encode(), label='codec-encode').stdout
        decoded = json.loads(commands.run([str(binary), 'decode'], data=encoded, label='codec-redecode').stdout)
        equal(decoded, row, 'real codec roundtrip')
    print(f'PASS codec relational roundtrips: {len(wires)} accepted fixtures/boundaries', flush=True)


def sync_constraints(db):
    db.sql("INSERT INTO newim.im_conversation_sync_accounts(user_id) VALUES ('alice'); "
           "INSERT INTO newim.im_conversation_sync_keys VALUES ('alice','room',1),('alice','other',2); "
           "INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',1,'room','upsert',1,'s_one'),"
           "('alice',2,'other','upsert',0,NULL),('alice',3,'room','remove',NULL,NULL);")
    scalar(db, "SELECT epoch||'|'||last_change_seq||'|'||min_valid_seq FROM newim.im_conversation_sync_accounts WHERE user_id='alice';",
           '1|0|0', 'initial account defaults')
    columns = {
        'accounts': 'user_id,epoch,last_change_seq,min_valid_seq',
        'keys': 'user_id,conversation_id,first_change_seq',
        'changes': 'user_id,change_seq,conversation_id,kind,latest_seq,latest_server_msg_id',
    }
    for suffix, expected in columns.items():
        scalar(db, "SELECT string_agg(column_name,',' ORDER BY ordinal_position) FROM information_schema.columns "
               f"WHERE table_schema='newim' AND table_name='im_conversation_sync_{suffix}';",
               expected, 'exact sync columns '+suffix)
    scalar(db, "SELECT pg_get_indexdef('newim.im_conversation_sync_changes_version_idx'::regclass);",
           'CREATE INDEX im_conversation_sync_changes_version_idx ON newim.im_conversation_sync_changes USING btree (user_id, conversation_id, change_seq DESC)',
           'version lookup index')
    scalar(db, "SELECT coll.collname FROM pg_attribute a JOIN pg_collation coll ON coll.oid=a.attcollation "
           "WHERE a.attrelid='newim.im_conversation_sync_keys'::regclass AND a.attname='conversation_id';",
           'C', 'directory identifier collation')
    negatives = [
        ("INSERT INTO newim.im_conversation_sync_accounts(user_id) VALUES ('missing');", '23503'),
        ("UPDATE newim.im_conversation_sync_accounts SET epoch=0;", '23514'),
        ("UPDATE newim.im_conversation_sync_accounts SET epoch=NULL;", '23502'),
        ("UPDATE newim.im_conversation_sync_accounts SET last_change_seq=-1;", '23514'),
        ("UPDATE newim.im_conversation_sync_accounts SET min_valid_seq=-1;", '23514'),
        ("UPDATE newim.im_conversation_sync_accounts SET min_valid_seq=1;", '23514'),
        ("UPDATE newim.im_conversation_sync_keys SET first_change_seq=0;", '23514'),
        ("INSERT INTO newim.im_conversation_sync_keys VALUES ('bob','room',1);", '23503'),
        ("INSERT INTO newim.im_conversation_sync_keys VALUES ('alice','missing',1);", '23503'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('bob',1,'room','remove',NULL,NULL);", '23503'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',0,'room','upsert',0,NULL);", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'room','invalid',0,NULL);", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'room','upsert',NULL,NULL);", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'room','upsert',-1,NULL);", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'room','remove',0,NULL);", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'room','remove',NULL,'s_one');", '23514'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',4,'other','upsert',1,'s_one');", '23503'),
        ("INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',1,'other','upsert',0,NULL);", '23505'),
        ("DELETE FROM newim.im_conversation_sync_keys WHERE conversation_id='room';", '23503'),
        ("DELETE FROM newim.im_conversation_sync_accounts WHERE user_id='alice';", '23503'),
    ]
    before = snapshot(db)
    for sql, error in negatives:
        db.sql(sql, error=error)
    equal(snapshot(db), before, 'sync constraint failures preserve state')
    # BIGINT boundaries are valid independently of application-level allocation.
    db.sql(f"BEGIN; UPDATE newim.im_conversation_sync_accounts SET epoch={MAXIMUM},last_change_seq={MAXIMUM},min_valid_seq={MAXIMUM}; "
           f"INSERT INTO newim.im_conversation_sync_changes VALUES ('alice',{MAXIMUM},'room','upsert',{MAXIMUM},NULL); ROLLBACK;")
    equal(snapshot(db), before, 'boundary transaction rollback')
    print(f'PASS sync schema constraints: {len(negatives)} rejected writes', flush=True)


def schema(db, commands):
    check_identity_contract()
    previous = os.environ.get('NEWIM_DB_IMAGE')
    start = len(commands.records)
    try:
        os.environ['NEWIM_DB_IMAGE'] = 'newim-nonexistent-'+uuid.uuid4().hex
        try:
            image_identity(commands, prepare=True)
        except Failure:
            pass
        else:
            raise Failure('unavailable explicit image was substituted')
        if any(r['label'] == 'locked-image-pull' for r in commands.records[start:]):
            raise Failure('explicit selector unexpectedly triggered image pull')
    finally:
        if previous is None:
            os.environ.pop('NEWIM_DB_IMAGE', None)
        else:
            os.environ['NEWIM_DB_IMAGE'] = previous
    locked = locked_metadata(db.target)
    actual = json.loads(commands.run(['docker','image','inspect',db.image,'--format','{{json .}}'],label='identity-negative-test-source').stdout)
    for field, bad in [('Id','sha256:'+'0'*64),('Architecture','invalid'),('RootFS',{'Layers':[]}),('Descriptor',{'digest':'sha256:'+'0'*64})]:
        altered = dict(actual, **{field:bad})
        try:
            verify_inspected_image(altered,db.target,locked)
        except Failure:
            continue
        raise Failure('image identity negative assertion: '+field)
    db.sql(migration_sql())
    check_catalog(db)
    seed(db)
    db.sql(persist('one'))
    db.sql("INSERT INTO newim.im_devices VALUES ('alice','phone',default); "
           "INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES ('session','alice','phone'); "
           "INSERT INTO newim.im_push_tokens VALUES ('alice','phone','apns',decode('01','hex')); "
           "INSERT INTO newim.im_read_states VALUES ('room','alice',0); "
           "INSERT INTO newim.im_blocks VALUES ('alice','bob'); "
           "INSERT INTO newim.im_webhook_deliveries(delivery_id,event_id,destination_id) VALUES ('delivery','e_one','destination');")
    negatives = [
        ("INSERT INTO newim.im_users(user_id) VALUES ('bad id');", '23514'),
        ("INSERT INTO newim.im_users(user_id) VALUES (repeat('a',129));", '23514'),
        ("UPDATE newim.im_messages SET message_type='Bad';", '23514'),
        ("UPDATE newim.im_messages SET conversation_seq=-1;", '23514'),
        ("UPDATE newim.im_messages SET protocol_version=2;", '23514'),
        ("UPDATE newim.im_messages SET schema_version=0;", '23514'),
        ("UPDATE newim.im_messages SET server_time_ms=-1;", '23514'),
        ("UPDATE newim.im_messages SET payload_bytes=decode(repeat('00',65537),'hex');", '23514'),
        ("INSERT INTO newim.im_messages SELECT * FROM newim.im_messages;", '23505'),
        ("INSERT INTO newim.im_messages SELECT 'newserver',sender_id,client_msg_id,conversation_id,2,protocol_version,schema_version,message_type,server_time_ms,payload_bytes FROM newim.im_messages;", '23505'),
        ("INSERT INTO newim.im_messages SELECT 'newserver',sender_id,'newclient',conversation_id,conversation_seq,protocol_version,schema_version,message_type,server_time_ms,payload_bytes FROM newim.im_messages;", '23505'),
        ("UPDATE newim.im_conversations SET latest_server_msg_id='s_one' WHERE conversation_id='other';", '23503'),
        ("INSERT INTO newim.im_read_states VALUES ('other','alice',0);", '23503'),
        ("UPDATE newim.im_read_states SET read_seq=-1;", '23514'),
        ("INSERT INTO newim.im_blocks VALUES ('alice','alice');", '23514'),
        ("DELETE FROM newim.im_users WHERE user_id='alice';", '23503'),
        ("UPDATE newim.im_webhook_deliveries SET attempts=-1;", '23514'),
        ("INSERT INTO newim.im_push_tokens VALUES ('bob','missing','apns',decode('01','hex'));", '23503'),
    ]
    before = snapshot(db)
    for sql, error in negatives:
        db.sql(sql, error=error)
    equal(snapshot(db), before, 'negative writes preserve database')
    sync_constraints(db)
    auth_constraints(db)
    print(f'PASS schema catalogs and {len(negatives)} negative writes', flush=True)
    codec_roundtrips(db, commands)
    db.sql("TRUNCATE newim.im_users CASCADE;")
    seed(db)
    # Selective populated plans, with planner defaults and real indexes.
    db.sql("INSERT INTO newim.im_messages SELECT 's_'||g,'alice','c_'||g,'room',g,1,1,'text',g,convert_to('{}','UTF8') FROM generate_series(1,10000) g; "
           "INSERT INTO newim.im_conversations(conversation_id) SELECT 'r_'||g FROM generate_series(1,10000) g; "
           "INSERT INTO newim.im_conversation_members SELECT 'r_'||g,'alice' FROM generate_series(1,10000) g; "
           "INSERT INTO newim.im_outbox_events(event_id,server_msg_id,completed_at) SELECT 'e_'||g,'s_'||g,CASE WHEN g<=9900 THEN now() ELSE NULL END FROM generate_series(1,10000) g; "
           "ANALYZE newim.im_messages; ANALYZE newim.im_conversation_members; ANALYZE newim.im_outbox_events;")
    queries = [
        ("SELECT server_msg_id FROM newim.im_messages WHERE conversation_id='room' AND conversation_seq>9980 ORDER BY conversation_seq LIMIT 10", 'im_messages_conversation_seq_key'),
        ("SELECT server_msg_id FROM newim.im_messages WHERE sender_id='alice' AND client_msg_id='c_9999'", 'im_messages_sender_client_key'),
        ("SELECT conversation_id FROM newim.im_conversation_members WHERE user_id='alice' AND conversation_id>'r_9980' ORDER BY conversation_id LIMIT 10", 'im_conversation_members_user_idx'),
        ("SELECT event_id FROM newim.im_outbox_events WHERE completed_at IS NULL AND available_at<=now() ORDER BY available_at,event_id LIMIT 10", 'im_outbox_events_pending_idx'),
    ]
    plans = []
    for query, index in queries:
        plan = json.loads(db.sql('EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) '+query))
        if index not in json.dumps(plan):
            raise Failure('populated query did not use expected index: '+index)
        plans.append({'index':index,'plan':plan})
    (commands.directory/'query-plans.json').write_text(json.dumps(plans,indent=2)+'\n')
    scalar(db, "SELECT string_agg(conversation_seq::text,',' ORDER BY conversation_seq) FROM (SELECT conversation_seq FROM newim.im_messages WHERE conversation_id='room' AND conversation_seq>9990 ORDER BY conversation_seq LIMIT 5) p;", '9991,9992,9993,9994,9995', 'numeric keyset order/limit')
    print('PASS four populated index plans, numeric bounded history', flush=True)


def sync_migration_faults(db, commands):
    # Alter only owned copies; applied 001/002 checksums remain authoritative.
    marker = '-- SYNC_MIGRATION_FAULT_POINT'
    source = DB_DIR/'migrations/003_conversation_sync.sql'
    equal(source.read_text().count(marker), 1, 'unique sync fault injection marker')
    before = snapshot(db)
    with tempfile.TemporaryDirectory(prefix='migration-copy-', dir=commands.directory) as owned:
        directory = Path(owned)
        for path in source_migrations():
            (directory/path.name).write_bytes(path.read_bytes())
        candidate = directory/source.name
        candidate.write_text(source.read_text().replace(marker, 'SELECT 1/0;', 1))
        db.sql(migration_sql(3, directory=directory), error='22012')
        equal(snapshot(db), before, 'failed 003 leaves exact populated 002 state')
        check_catalog(db, 2)
        candidate.write_text(source.read_text().replace(marker, 'SELECT pg_sleep(30);', 1))
        held = db.session("SET application_name='newim_sync_migration_interrupt'; "+migration_sql(3, directory=directory))
        db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_sync_migration_interrupt' AND wait_event='PgSleep');")
        db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='newim_sync_migration_interrupt';")
        held.finish(error='57P01')
        equal(snapshot(db), before, 'terminated 003 leaves exact populated 002 state')
        check_catalog(db, 2)
    # Four first-time upgrades queue behind a real migration lock, then race.
    # 原始源码重试；锁屏障确保覆盖首次升级竞争，而非仅完成后的重放。
    gate = db.session("SET application_name='newim_upgrade_gate'; BEGIN; SELECT pg_advisory_xact_lock(1947620131);")
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_upgrade_gate' AND state='idle in transaction');")
    concurrent = [db.session("SET application_name='newim_upgrade_contender'; "+migration_sql(3)) for _ in range(4)]
    db.wait_sql("SELECT count(*)=4 FROM pg_stat_activity WHERE application_name='newim_upgrade_contender' AND wait_event_type='Lock';")
    gate.send('COMMIT;')
    gate.finish()
    for session in concurrent:
        session.finish()
    check_catalog(db, 3)
    after = snapshot(db)
    equal({k:after[k] for k in before if k!='migrations'},
          {k:v for k,v in before.items() if k!='migrations'}, '003 preserves all historical rows')
    for table in SYNC_TABLES:
        scalar(db, 'SELECT count(*) FROM newim.'+table+';', 0, 'migration performs no automatic backfill')
    print('PASS populated 003 failure/termination rollback, original-source restart and no implicit backfill', flush=True)


def auth_constraints(db):
    db.sql("INSERT INTO newim.im_devices VALUES ('alice','auth_phone',default) ON CONFLICT DO NOTHING; "
           "INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES ('auth_session','alice','auth_phone') ON CONFLICT DO NOTHING; "
           "INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) "
           "VALUES ('0123456789abcdef0123456789abcdef',decode(repeat('01',32),'hex'),'auth_session',"
           "'2026-01-01 00:00:00+00','2026-01-01 01:00:00+00');")
    scalar(db, "SELECT string_agg(column_name,',' ORDER BY ordinal_position) FROM information_schema.columns "
           "WHERE table_schema='newim' AND table_name='im_auth_tokens';",
           'token_id,token_digest,session_id,created_at,expires_at,revoked_at', 'exact auth token columns')
    scalar(db, "SELECT pg_get_indexdef('newim.im_auth_tokens_session_idx'::regclass);",
           'CREATE INDEX im_auth_tokens_session_idx ON newim.im_auth_tokens USING btree (session_id, token_id)',
           'auth token session index')
    scalar(db, "SELECT coll.collname FROM pg_attribute a JOIN pg_collation coll ON coll.oid=a.attcollation "
           "WHERE a.attrelid='newim.im_auth_tokens'::regclass AND a.attname='token_id';",
           'C', 'auth token identifier collation')
    negatives = [
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('0123456789abcdef0123456789abcdeF',decode(repeat('02',32),'hex'),'auth_session',now(),now()+interval '1 hour');", '23514'),
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('0123456789abcdef0123456789abcde',decode(repeat('02',32),'hex'),'auth_session',now(),now()+interval '1 hour');", '23514'),
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('fedcba9876543210fedcba9876543210',decode(repeat('02',31),'hex'),'auth_session',now(),now()+interval '1 hour');", '23514'),
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('0123456789abcdef0123456789abcdef',decode(repeat('03',32),'hex'),'auth_session',now(),now()+interval '1 hour');", '23505'),
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('fedcba9876543210fedcba9876543210',decode(repeat('01',32),'hex'),'auth_session',now(),now()+interval '1 hour');", '23505'),
        ("INSERT INTO newim.im_auth_tokens(token_id,token_digest,session_id,created_at,expires_at) VALUES ('fedcba9876543210fedcba9876543210',decode(repeat('02',32),'hex'),'missing',now(),now()+interval '1 hour');", '23503'),
        ("UPDATE newim.im_auth_tokens SET expires_at=created_at WHERE token_id='0123456789abcdef0123456789abcdef';", '23514'),
        ("UPDATE newim.im_auth_tokens SET revoked_at=created_at-interval '1 second' WHERE token_id='0123456789abcdef0123456789abcdef';", '23514'),
    ]
    before = snapshot(db)
    for sql, error in negatives:
        db.sql(sql, error=error)
    equal(snapshot(db), before, 'auth constraint failures preserve state')
    print(f'PASS auth schema constraints: {len(negatives)} rejected writes', flush=True)


def auth_migration_faults(db, commands):
    # Alter only an owned 004 copy; populated 001-003 bytes must survive every failure.
    marker = '-- AUTH_MIGRATION_FAULT_POINT'
    source = DB_DIR/'migrations/004_auth_sessions.sql'
    equal(source.read_text().count(marker), 1, 'unique auth migration fault injection marker')
    db.sql("INSERT INTO newim.im_devices VALUES ('alice','migration_auth_phone',default) ON CONFLICT DO NOTHING; "
           "INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES ('migration_auth_session','alice','migration_auth_phone') ON CONFLICT DO NOTHING;")
    before = snapshot(db)
    with tempfile.TemporaryDirectory(prefix='auth-migration-copy-', dir=commands.directory) as owned:
        directory = Path(owned)
        for path in source_migrations():
            (directory/path.name).write_bytes(path.read_bytes())
        candidate = directory/source.name
        candidate.write_text(source.read_text().replace(marker, 'SELECT 1/0;', 1))
        db.sql(migration_sql(directory=directory), error='22012')
        equal(snapshot(db), before, 'failed 004 leaves exact populated 003 state')
        check_catalog(db, 3)
        candidate.write_text(source.read_text().replace(marker, 'SELECT pg_sleep(30);', 1))
        held = db.session("SET application_name='newim_auth_migration_interrupt'; "+migration_sql(directory=directory))
        db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_auth_migration_interrupt' AND wait_event='PgSleep');")
        db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='newim_auth_migration_interrupt';")
        held.finish(error='57P01')
        equal(snapshot(db), before, 'terminated 004 leaves exact populated 003 state')
        check_catalog(db, 3)
    db.sql(migration_sql())
    check_catalog(db, 4)
    after = snapshot(db)
    equal({k:after[k] for k in before if k!='migrations'},
          {k:v for k,v in before.items() if k!='migrations'}, '004 preserves all historical rows')
    for table in AUTH_TABLES:
        scalar(db, 'SELECT count(*) FROM newim.'+table+';', 0, 'auth migration performs no implicit backfill')
    print('PASS populated 004 failure/termination rollback and original-source retry', flush=True)


def migrations(db, commands):
    head = len(source_migrations())
    db.sql(migration_sql(1))
    check_catalog(db, 1)
    seed(db)
    db.sql("INSERT INTO newim.im_messages VALUES ('legacy','alice','legacy','room',1,1,1,'text',10,convert_to('{}','UTF8')); "
           "UPDATE newim.im_conversations SET last_seq=1,latest_server_msg_id='legacy' WHERE conversation_id='room';")
    before = snapshot(db)
    db.sql(migration_sql(2))
    check_catalog(db, 2)
    after = snapshot(db)
    equal({k:after[k] for k in before if k!='migrations'}, {k:v for k,v in before.items() if k!='migrations'}, 'historical 001 to 002 preserves populated rows and payload')
    db.sql("UPDATE newim.im_messages SET conversation_seq=-1;", error='23514')
    db.sql("UPDATE newim.im_conversations SET latest_server_msg_id='legacy' WHERE conversation_id='other';", error='23503')
    db.sql(persist('migration_durable'))
    sync_migration_faults(db, commands)
    auth_migration_faults(db, commands)
    after = snapshot(db)
    db.sql(migration_sql())
    equal(snapshot(db), after, 'head replay stable')
    concurrent = [db.session(migration_sql()) for _ in range(4)]
    for session in concurrent:
        session.finish()
    equal(snapshot(db), after, 'competing head migration replay')
    check_catalog(db)
    # Check every applied checksum, including the latest migration.
    for version, path in enumerate(source_migrations(), 1):
        original = hashlib.sha256(path.read_bytes()).hexdigest()
        db.sql(f"UPDATE newim_meta.migrations SET sha256=repeat('0',64) WHERE version={version};")
        db.sql(migration_sql(), error='NM002')
        db.sql(f"UPDATE newim_meta.migrations SET sha256='{original}' WHERE version={version};")
    future = head+1
    db.sql(f"INSERT INTO newim_meta.migrations VALUES ({future},repeat('0',64),default);")
    db.sql(migration_sql(), error='NM001')
    db.sql(f'DELETE FROM newim_meta.migrations WHERE version={future};')
    db.sql('DELETE FROM newim_meta.migrations WHERE version=1;')
    db.sql(migration_sql(), error='NM003')
    original = hashlib.sha256(source_migrations()[0].read_bytes()).hexdigest()
    db.sql(f"INSERT INTO newim_meta.migrations VALUES (1,'{original}',now());")
    check_catalog(db)
    # Preserve 002 injection coverage on a fresh atomic install.
    db.sql('CREATE DATABASE migration_failure;')
    failure_marker = 'CREATE TABLE newim.im_outbox_events'
    equal(migration_sql().count(failure_marker), 1, 'unique durability failure injection marker')
    failing = migration_sql().replace(failure_marker, "SELECT 1/0;\n"+failure_marker, 1)
    db.sql(failing, database='migration_failure', error='22012')
    equal(db.sql("SELECT count(*) FROM pg_namespace WHERE nspname IN ('newim','newim_meta');", database='migration_failure').strip(), '0', 'atomic failed initial install')
    # Preserve historical 002 termination, then recover directly to newest head.
    db.sql('DROP SCHEMA newim CASCADE; DROP SCHEMA newim_meta CASCADE;')
    held = db.session("SET application_name='newim_migration_interrupt'; "+migration_sql(2).replace('COMMIT;', 'SELECT pg_sleep(30); COMMIT;'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_migration_interrupt' AND wait_event='PgSleep');")
    db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='newim_migration_interrupt';")
    held.finish(error='57P01')
    scalar(db, "SELECT count(*) FROM pg_namespace WHERE nspname IN ('newim','newim_meta');", 0, 'killed historical migration rollback')
    db.sql(migration_sql())
    check_catalog(db)
    scalar(db, 'SELECT count(*) FROM newim_meta.migrations;', head, 'restart completes newest migration')
    print('PASS empty/populated 1 to 2 to head migration, head replay/concurrency/checksum/future/gap, failure and termination recovery', flush=True)


def sequence(db, commands):
    db.sql(migration_sql())
    seed(db)
    sessions = [db.session(persist('parallel_'+str(i))) for i in range(16)]
    seqs = sorted(int(s.finish().strip()) for s in sessions)
    equal(seqs, list(range(1,17)), 'concurrent conversation allocation')
    sessions = [db.session(persist('duplicate', server='ds_'+str(i), event='de_'+str(i))) for i in range(12)]
    equal({s.finish().strip() for s in sessions}, {'17'}, 'same conversation duplicate race')
    # Force a cross-conversation race while the winning identity is uncommitted.
    held = db.session("SET application_name='newim_duplicate_winner'; BEGIN; "+persist('cross')+"SELECT pg_sleep(0.1);")
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_duplicate_winner' AND state='idle in transaction');")
    loser = db.session("SET application_name='newim_duplicate_loser'; "+persist('cross',room='other',server='cross_loser',event='cross_loser_event'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_duplicate_loser' AND wait_event_type='Lock');")
    held.send('COMMIT;')
    held.finish()
    equal(loser.finish().strip(), '18', 'cross conversation retry original')
    scalar(db, "SELECT last_seq FROM newim.im_conversations WHERE conversation_id='other';", 0, 'loser counter rolled back')
    # The primary key may win constraint selection when a retry reuses server ID.
    # The returned identity must not depend on PostgreSQL's unique-index check order.
    held = db.session("SET application_name='newim_same_server_winner'; BEGIN; "+persist('same_server',server='shared_server',event='shared_winner_event'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_same_server_winner' AND state='idle in transaction');")
    loser = db.session("SET application_name='newim_same_server_loser'; "+persist('same_server',room='other',server='shared_server',event='shared_loser_event'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_same_server_loser' AND wait_event_type='Lock');")
    held.send('COMMIT;')
    held.finish()
    equal(loser.finish().strip(), '19', 'same-server cross-conversation retry returns original')
    scalar(db, "SELECT last_seq FROM newim.im_conversations WHERE conversation_id='other';", 0, 'same-server loser allocation rolled back')
    scalar(db, "SELECT conversation_id||'|'||server_msg_id FROM newim.im_messages WHERE sender_id='alice' AND client_msg_id='same_server';", 'room|shared_server', 'original identity unchanged')
    scalar(db, "SELECT count(*) FROM newim.im_outbox_events WHERE event_id='shared_loser_event';", 0, 'same-server loser outbox rolled back')
    before = snapshot(db)
    db.sql('BEGIN; '+persist('rolled_back')+' ROLLBACK;')
    equal(snapshot(db), before, 'outer rollback atomicity')
    db.sql(persist('id_collision',server='s_parallel_0'), error='23505')
    db.sql(persist('event_collision',event='e_parallel_0'), error='23505')
    equal(snapshot(db), before, 'server/event collision counter and summary rollback')
    db.sql('BEGIN ISOLATION LEVEL REPEATABLE READ; '+persist('isolation'), error='NI003')
    db.sql(persist('missing',room='missing'), error='NI001')
    # Independent conversation progresses while one conversation is locked.
    lock = db.session("SET application_name='newim_room_lock'; BEGIN; SELECT 1 FROM newim.im_conversations WHERE conversation_id='room' FOR UPDATE;")
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_room_lock' AND state='idle in transaction');")
    scalar(db, persist('unrelated',room='other'), 1, 'unrelated conversation not blocked')
    lock.send('ROLLBACK;')
    lock.finish()
    db.sql(f"UPDATE newim.im_conversations SET last_seq={MAXIMUM-1} WHERE conversation_id='other';")
    scalar(db, persist('maximum',room='other'), MAXIMUM, 'last representable allocation')
    before = snapshot(db)
    db.sql(persist('overflow',room='other'), error='NI002')
    equal(snapshot(db), before, 'overflow atomicity')
    scalar(db, persist('maximum',room='other'), MAXIMUM, 'duplicate still retrievable at exhaustion')
    scalar(db, "SELECT count(*) FROM newim.im_messages m FULL JOIN newim.im_outbox_events o USING(server_msg_id) WHERE m.server_msg_id IS NULL OR o.server_msg_id IS NULL;", 0, 'message/outbox atomicity')
    scalar(db, "SELECT count(*) FROM newim.im_conversations c JOIN newim.im_messages m ON c.latest_server_msg_id=m.server_msg_id WHERE c.last_seq<>m.conversation_seq;", 0, 'summary matches latest')
    print('PASS 16 writers, 12 duplicate writers, different/same-server cross-conversation races, rollback/collisions/overflow, independent conversation progress', flush=True)


def repair(db, commands, image, target):
    db.sql(migration_sql())
    seed(db)
    for i in range(5):
        db.sql(persist('committed_'+str(i)))
    committed = snapshot(db)
    # Backend death and whole PostgreSQL process death must both abort uncommitted state.
    held = db.session("SET application_name='newim_repair_backend'; BEGIN; "+persist('uncommitted_backend'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_repair_backend' AND state='idle in transaction');")
    db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='newim_repair_backend';")
    held.send('COMMIT;')
    held.finish(error='57P01')
    equal(snapshot(db), committed, 'terminated backend preserves committed state')
    held = db.session("SET application_name='newim_repair_crash'; BEGIN; "+persist('uncommitted_crash'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_repair_crash' AND state='idle in transaction');")
    commands.run(['docker','kill','--signal','KILL',db.name], label='crash-owned-postgres')
    held.cancel()
    commands.run(['docker','start',db.name], label='restart-owned-volume')
    db.ready()
    equal(snapshot(db), committed, 'process crash recovery exact committed snapshot')
    scalar(db, persist('uncommitted_crash'), 6, 'retry after crash allocates once')
    # Runbook logical backup, new durable volume, restore and verified forward repair.
    dump = commands.run(['docker','exec',db.name,'pg_dump','-U','newim_test','-d','newim_test','--format=custom','--no-owner'], timeout=45,label='logical-backup').stdout
    (commands.directory/'backup-sha256.txt').write_text(hashlib.sha256(dump).hexdigest()+'\n')
    with Database(commands,image,target) as restored:
        commands.run(['docker','exec','-i',restored.name,'pg_restore','-U','newim_test','-d','newim_test','--exit-on-error','--single-transaction','--no-owner'],data=dump,timeout=45,label='restore-new-volume')
        equal(snapshot(restored),snapshot(db),'restored all tables/ledger exactly')
        check_catalog(restored)
        scalar(restored, "SELECT has_schema_privilege('public','newim','USAGE');", 'f', 'restored schema privilege')
        restored.sql(migration_sql())
        # Derived summary can be repaired under the same conversation lock; never renumber IDs.
        restored.sql("UPDATE newim.im_conversations SET latest_server_msg_id=NULL WHERE conversation_id='room';")
        restored.sql((DB_DIR/'repair-summary.sql').read_text())
        equal(snapshot(restored),snapshot(db),'forward summary repair exact state')
        scalar(restored,persist('after_restore'),7,'allocation resumes after restore')
        scalar(restored,"SELECT count(*) FROM newim.im_outbox_events;",7,'outbox preserved and resumed')
    print('PASS backend termination, SIGKILL/restart durable volume, dump/restore new volume, forward repair, resumed allocation',flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('suite', choices=['prepare','schema','migrations','sequence','repair'])
    args = parser.parse_args()
    run = ROOT/'build/db-runs'/(args.suite+'-'+uuid.uuid4().hex)
    commands = Commands(run)
    started = time.monotonic()
    outcome = 'fail'
    try:
        image,target = image_identity(commands,prepare=args.suite=='prepare')
        if args.suite=='prepare':
            commands.run(['docker','run','--rm','--pull','never','--platform',target,'--network','none',
                          '--read-only','--tmpfs','/var/lib/postgresql:rw,noexec,nosuid,size=1048576',
                          '--user','postgres','--entrypoint','postgres',image,'--version'], label='postgres-executable-version')
        else:
            with Database(commands,image,target) as db:
                if args.suite=='repair':
                    repair(db,commands,image,target)
                else:
                    globals()[args.suite](db,commands)
        outcome='pass'
    except (Failure,OSError,ValueError) as exc:
        print('FAIL '+str(exc),file=sys.stderr)
        return 1
    finally:
        (run/'result.json').write_text(json.dumps({'suite':args.suite,'result':outcome,'elapsed_seconds':round(time.monotonic()-started,3)},indent=2)+'\n')
        print('Evidence: '+str(run.relative_to(ROOT)),flush=True)
    print('PASS '+args.suite,flush=True)
    return 0


if __name__=='__main__':
    sys.exit(main())
