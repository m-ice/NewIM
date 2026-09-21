"""Real PostgreSQL acceptance suites. 每个套件使用独立持久卷，不连接用户数据库。"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid

from migrate import migration_sql
from runtime import Commands, Database, DB_DIR, Failure, MAXIMUM, ROOT, image_identity, locked_metadata, verify_inspected_image


def equal(actual, expected, label):
    if actual != expected:
        raise Failure(label + ': assertion failed')


def scalar(db, sql, expected, label):
    equal(db.sql(sql).strip(), str(expected), label)


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


def schema(db, commands):
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
    expected = {'im_users','im_devices','im_sessions','im_conversations','im_conversation_members',
                'im_messages','im_read_states','im_blocks','im_outbox_events','im_webhook_deliveries','im_push_tokens'}
    equal(set(db.sql("SELECT tablename FROM pg_tables WHERE schemaname='newim';").splitlines()), expected, 'catalog tables')
    scalar(db, "SELECT count(*) FROM pg_constraint WHERE connamespace='newim'::regnamespace AND contype='f' AND confdeltype<>'a';", 0, 'no cascading deletes')
    scalar(db, "SELECT has_function_privilege('public',p.oid,'EXECUTE') FROM pg_proc p WHERE pronamespace='newim'::regnamespace;", 'f', 'no public persist privilege')
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


def migrations(db, commands):
    db.sql(migration_sql(1))
    seed(db)
    db.sql("INSERT INTO newim.im_messages VALUES ('legacy','alice','legacy','room',1,1,1,'text',10,convert_to('{}','UTF8')); "
           "UPDATE newim.im_conversations SET last_seq=1,latest_server_msg_id='legacy' WHERE conversation_id='room';")
    before = snapshot(db)
    db.sql(migration_sql())
    after = snapshot(db)
    equal({k:after[k] for k in before if k!='migrations'}, {k:v for k,v in before.items() if k!='migrations'}, 'populated stage upgrade')
    db.sql(migration_sql())
    equal(snapshot(db), after, 'replay stable')
    concurrent = [db.session(migration_sql()) for _ in range(4)]
    for session in concurrent:
        session.finish()
    equal(snapshot(db), after, 'competing migration replay')
    original = db.sql('SELECT sha256 FROM newim_meta.migrations WHERE version=1;').strip()
    db.sql("UPDATE newim_meta.migrations SET sha256=repeat('0',64) WHERE version=1;")
    db.sql(migration_sql(), error='NM002')
    db.sql(f"UPDATE newim_meta.migrations SET sha256='{original}' WHERE version=1;")
    db.sql("INSERT INTO newim_meta.migrations VALUES (3,repeat('0',64),default);")
    db.sql(migration_sql(), error='NM001')
    db.sql('DELETE FROM newim_meta.migrations WHERE version=3; DELETE FROM newim_meta.migrations WHERE version=1;')
    db.sql(migration_sql(), error='NM003')
    db.sql(f"INSERT INTO newim_meta.migrations VALUES (1,'{original}',now());")
    # A new database proves atomic failed installation and terminated migration.
    db.sql('CREATE DATABASE migration_failure;')
    failing = migration_sql().replace('CREATE TABLE newim.im_outbox_events', "SELECT 1/0;\nCREATE TABLE newim.im_outbox_events")
    db.sql(failing, database='migration_failure', error='22012')
    equal(db.sql("SELECT count(*) FROM pg_namespace WHERE nspname IN ('newim','newim_meta');", database='migration_failure').strip(), '0', 'atomic failed initial install')
    # Hold the entire uncommitted DDL behind an observable backend barrier.
    db.sql('DROP SCHEMA newim CASCADE; DROP SCHEMA newim_meta CASCADE;')
    held = db.session("SET application_name='newim_migration_interrupt'; "+migration_sql().replace('COMMIT;', 'SELECT pg_sleep(10); COMMIT;'))
    db.wait_sql("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='newim_migration_interrupt' AND wait_event='PgSleep');")
    db.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='newim_migration_interrupt';")
    held.finish(error='57P01')
    scalar(db, "SELECT count(*) FROM pg_namespace WHERE nspname IN ('newim','newim_meta');", 0, 'killed migration rollback')
    db.sql(migration_sql())
    scalar(db, 'SELECT count(*) FROM newim_meta.migrations;', 2, 'restart completes migration')
    print('PASS empty/populated migration, replay, concurrent migrations, checksum/future/gap rejection, failure and backend termination recovery', flush=True)


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
        scalar(restored, "SELECT has_function_privilege('public',p.oid,'EXECUTE') FROM pg_proc p WHERE pronamespace='newim'::regnamespace;", 'f', 'restored public function privilege')
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
