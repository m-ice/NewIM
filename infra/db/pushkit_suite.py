"""Verify the Push Kit schema upgrade and registration against a real TLS gateway."""
import os
import uuid

from gateway_suite import build
from migrate import migration_sql
from runtime import Commands, Database, Failure, ROOT, image_identity
from sync_http_suite import run_test


def registration(device, channel, version):
    return f"""
    INSERT INTO newim.im_push_tokens
      (user_id, device_id, provider, token_ciphertext, session_id, registration_version, key_id, token_nonce, registered_at)
    VALUES ('owner', '{device}', '{channel}', decode(repeat('00',17),'hex'), '{device}_s',
      {version}, 'key', decode(repeat('00',12),'hex'), clock_timestamp());
    """


def delivery(device, channel, version):
    return f"""
    INSERT INTO newim.im_push_deliveries (event_id, user_id, device_id, session_id, provider, registration_version)
    VALUES ('event', 'owner', '{device}', '{device}_s', '{channel}', {version});
    """


def check_upgrade(db):
    db.sql(migration_sql(22), timeout=60)
    db.sql("""
    INSERT INTO newim.im_users(user_id) VALUES ('owner');
    INSERT INTO newim.im_devices(user_id, device_id, platform) VALUES
      ('owner','apple','ios'),('owner','android','android'),('owner','harmony','harmony');
    INSERT INTO newim.im_sessions(session_id,user_id,device_id) VALUES
      ('apple_s','owner','apple'),('android_s','owner','android'),('harmony_s','owner','harmony');
    INSERT INTO newim.im_conversations(conversation_id) VALUES ('conversation');
    INSERT INTO newim.im_messages
      (server_msg_id,sender_id,client_msg_id,conversation_id,conversation_seq,protocol_version,
       schema_version,message_type,server_time_ms,payload_bytes)
    VALUES ('message','owner','client','conversation',1,1,1,'text',0,convert_to('{}','UTF8'));
    INSERT INTO newim.im_outbox_events(event_id,server_msg_id) VALUES ('event','message');
    """)
    for device, channel, version in (("apple", "apns", 1), ("android", "fcm", 2)):
        db.sql(registration(device, channel, version) + delivery(device, channel, version))
    db.sql(registration("harmony", "pushkit", 3), error="23514")
    db.sql(delivery("harmony", "pushkit", 3), error="23514")
    baseline = db.sql("SELECT version,sha256 FROM newim_meta.migrations ORDER BY version;")
    db.sql(migration_sql(23), timeout=60)
    db.sql(migration_sql(23), timeout=60)
    if baseline != db.sql("SELECT version,sha256 FROM newim_meta.migrations WHERE version<=22 ORDER BY version;"):
        raise Failure("old migration hashes changed")
    if db.sql("SELECT max(version),count(*) FROM newim_meta.migrations;").strip() != "23|23":
        raise Failure("migration 023 did not preserve a contiguous ledger")
    db.sql(registration("harmony", "other", 3), error="23514")
    db.sql(delivery("harmony", "other", 3), error="23514")
    db.sql(registration("harmony", "pushkit", 3) + delivery("harmony", "pushkit", 3))
    db.sql(registration("harmony", "fcm", 4), error="23505")
    expected = ["apns|1", "fcm|2", "pushkit|3"]
    for table in ("im_push_tokens", "im_push_deliveries"):
        state = db.sql(f"SELECT provider,registration_version FROM newim.{table} ORDER BY provider;")
        if state.strip().splitlines() != expected:
            raise Failure("migration did not preserve existing rows and add the Push Kit row")
    print("PostgreSQL 22 -> 23: legacy rows and hashes preserved; Push Kit rows admitted; repeat upgrade safe")


def check_gateway(commands, image, platform):
    env = dict(os.environ, CGO_ENABLED="0", GOOS="linux", GOARCH=platform.split("/")[1], GOTOOLCHAIN="local")
    binary = "conversationsynchttp.test"
    servers = ("newim-gateway", "newim-server")
    build(commands, ["go", "test", "-c", "-tags=integration", "-o", str(commands.directory / binary),
                     "./tests/integration/conversationsynchttp"], env, "pushkit-test-build")
    for name in servers:
        build(commands, ["go", "build", "-o", str(commands.directory / name), "./server/cmd/" + name],
              env, "pushkit-" + name)
    with Database(commands, image, platform) as db:
        db.sql(migration_sql(23), timeout=60)
        for name in (binary, *servers):
            commands.run(["docker", "cp", str(commands.directory / name), db.name + ":/tmp/" + name],
                         label="copy-pushkit-" + name)
        commands.run(["docker", "exec", db.name, "chmod", "0755", *("/tmp/" + name for name in (binary, *servers))],
                     label="pushkit-permissions")
        run_test(db, "TestPushKitGateway")


def main():
    commands = Commands(ROOT / "build/pushkit" / uuid.uuid4().hex)
    try:
        image, platform = image_identity(commands)
        with Database(commands, image, platform) as db:
            check_upgrade(db)
        check_gateway(commands, image, platform)
    except (Failure, OSError) as error:
        print(f"Push Kit integration failed: {error}; command records: {commands.directory}")
        return 1
    print(f"Push Kit integration passed; command records: {commands.directory}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
