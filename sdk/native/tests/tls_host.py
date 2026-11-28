"""Drive the linked C host through the existing owned PostgreSQL/TLS fixture."""

from collections import deque
from contextlib import closing
from pathlib import Path
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile

from build_host import build_host

ACCOUNT = "sdk_bridge_alice"
ROOM = "sdk_bridge_group_00"


def require(ok, label):
    if not ok:
        raise AssertionError(label)


class Pipe:
    def __init__(self, argv):
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        encoding="utf-8")
        try:
            self.expect("READY")
        except (AssertionError, OSError):
            self.__exit__()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=10)
        self.process.stdin.close()
        self.process.stdout.close()

    def request(self, line):
        self.process.stdin.write(line + "\n")
        self.process.stdin.flush()
        return self.line()

    def line(self):
        reply = self.process.stdout.readline(1_048_600)
        require(reply.endswith("\n") and not reply.startswith("FAILED"), "fixture process response missing")
        return reply.rstrip("\n")

    def expect(self, expected):
        require(self.line() == expected, "unexpected fixture process response")

    def finish(self, expect_pass=False):
        require(self.request("QUIT") == "DONE", "fixture shutdown response")
        if expect_pass:
            self.expect("PASS")
        require(self.process.wait(timeout=10) == 0, "fixture exit")


def wire(client, text):
    return json.dumps({
        "protocolVersion": 1, "kind": "send",
        "body": {"clientMsgId": client, "conversationId": ROOM, "version": 1,
                 "type": "text", "payload": {"text": text}},
    }, separators=(",", ":"))


class Driver:
    def __init__(self, native, tls):
        self.native = native
        self.tls = tls
        self.clock = 100
        self.socket = None
        self.subscriptions = []

    def step(self, event, *, status=0):
        self.clock += 1
        envelope = json.dumps({"nowMs": self.clock, "random": 19, "event": event},
                              separators=(",", ":"))
        require(self.native.request("SUBMIT\t" + envelope) == f"SUBMIT\t{status}",
                f"C submit status for {event['type']}")
        effects = []
        while True:
            tag, code, output = self.native.request("NEXT").split("\t", 2)
            require(tag == "NEXT", "C output framing")
            if code == "4":
                return effects
            require(code == "0", "C output status")
            effects.append(json.loads(output))

    def http(self, role, path, body):
        tag, code, output = self.tls.request(
            f"HTTP\t{role}\tPOST\t{path}\t{body.encode().hex()}"
        ).split("\t", 2)
        require(tag == "HTTP", "missing independently verified TLS response")
        return int(code), bytes.fromhex(output).decode()

    def peer_message(self, client, text):
        code, _ = self.http("peer", "/api/v1/messages", wire(client, text))
        require(code == 200, "commit a peer message over TLS")

    def run(self, effects, *, hold_http=None):
        pending = deque(effects)
        observations = []
        for _ in range(128):
            if not pending:
                return observations
            effect = pending.popleft()
            kind = effect["type"]
            operation = effect.get("operation")
            match kind:
                case "credentials_required":
                    tag, account, ttl = self.tls.request("NATIVE_CREDENTIAL").split("\t", 2)
                    require(tag == "CREDENTIAL" and account == ACCOUNT and int(ttl) > 0,
                            "verified credential identity")
                    credentials = {"type": "credentials", "operation": operation,
                                   "account": account, "verified": True,
                                   "expiresAtMs": self.clock + int(ttl)}
                    require(not self.step({**credentials, "account": "another_account"}, status=1),
                            "wrong-account result changed native state")
                    require(not self.step({**credentials, "verified": False}, status=1),
                            "unverified credential result changed native state")
                    pending.extend(self.step(credentials))
                case "http" if effect["path"] == hold_http:
                    observations.append(effect)
                case "http":
                    code, body = self.http("phone", effect["path"], effect["body"])
                    pending.extend(self.step({"type": "http", "operation": operation,
                                              "status": code, "verified": True, "body": body}))
                case "open_socket":
                    require(self.socket is None, "open a second active fixture socket")
                    require(self.tls.request("SOCKET\tphone") == "SOCKET\t101\tnewim.v1",
                            "verified WSS upgrade")
                    self.socket = operation
                    pending.extend(self.step({"type": "socket_opened", "operation": operation,
                                              "verified": True, "subprotocol": "newim.v1"}))
                case "subscribe":
                    self.subscriptions.append(json.loads(effect["body"])["afterSeq"])
                    require(self.socket == operation, "subscribe to the current socket")
                    require(self.tls.request("SUBSCRIBE\t" + effect["body"].encode().hex()) == "SUBSCRIBED",
                            "WSS subscription")
                    pending.extend(self.step({"type": "socket_subscribed", "operation": operation}))
                case "cancel":
                    if operation == self.socket:
                        require(self.tls.request("CLOSE") == "CLOSE_OK", "close WSS after cancellation")
                        self.socket = None
                case _:
                    observations.append(effect)
        raise AssertionError("C bridge generated too many fixture effects")

    def event(self, kind, **fields):
        return self.run(self.step({"type": kind, **fields}))

    def frame_for(self, client):
        require(self.socket is not None, "active fixture socket")
        for _ in range(32):
            result = self.tls.request("POLL")
            if result == "EMPTY":
                continue
            tag, encoded = result.split("\t", 1)
            require(tag == "FRAME", "WSS frame did not arrive")
            body = bytes.fromhex(encoded).decode()
            self.event("frame", operation=self.socket, body=body)
            if json.loads(body)["body"].get("clientMsgId") == client:
                return body
        raise AssertionError("expected committed message did not arrive on WSS")

    def messages(self, expected):
        effects = self.event("list_messages", conversationId=ROOM, limit=32)
        require(len(effects) == 1 and effects[0]["type"] == "messages", "SQLite message view")
        items = effects[0]["items"]
        require([item["clientMsgId"] for item in items] == expected, "committed history convergence")
        require([item["conversationSeq"] for item in items] ==
                [str(seq) for seq in range(1, len(expected) + 1)], "stable persisted sequence")
        return items


def ready(effects, label):
    require(any(effect["type"] == "ready" for effect in effects), label)


def exercise(driver):
    driver.event("select_conversation", conversationId=ROOM)
    ready(driver.event("network_available"), "C bridge reached ready after SQLite bootstrap")
    messages = ["connect_001"]
    driver.messages(messages)

    outbound = "native_outbound"
    effects = driver.event("queue_send", body=wire(outbound, "written through the C SDK"))
    require([effect["type"] for effect in effects if effect["type"].startswith("send_")] ==
            ["send_queued", "send_ack"], "queue before authoritative receipt")
    messages.append(outbound)
    driver.messages(messages)

    realtime = "native_realtime"
    driver.peer_message(realtime, "real WSS receive")
    frame = driver.frame_for(realtime)
    driver.event("frame", operation=driver.socket, body=frame)
    messages.append(realtime)
    driver.messages(messages)

    stale = driver.socket
    driver.event("network_lost")
    require(not driver.step({"type": "frame", "operation": stale, "body": frame}, status=1),
            "stale socket callback ignored")
    offline = "native_while_offline"
    driver.peer_message(offline, "missed during the interruption")
    ready(driver.event("network_available"), "C bridge recovered after reconnect")
    require(driver.subscriptions[-1] == "3", "resume from the durable WSS sequence")
    messages.append(offline)
    driver.messages(messages)

    pushed = "native_after_notification"
    driver.peer_message(pushed, "new history on notification")
    ready(driver.event("notification", marker="sync"), "notification refreshed through normal recovery")
    require(driver.subscriptions[-1] == "4", "notification used the durable cursor")
    messages.append(pushed)
    require(json.loads(driver.messages(messages)[-1]["payload"])["text"] == "new history on notification",
            "notification history payload")

    replay_client = "native_after_reopen"
    effects = driver.step({"type": "queue_send", "body": wire(replay_client, "lost receipt before restart")})
    require([event["type"] for event in effects] == ["send_queued", "http"], "persist before dispatch")
    send = effects[1]
    code, body = driver.http("phone", send["path"], send["body"])
    require(code == 200, "initial authoritative response before restart")
    original_receipt = json.loads(body)["body"]
    pending = driver.event("list_pending", limit=16)[0]["items"]
    require(len(pending) == 1 and pending[0]["clientMsgId"] == replay_client and
            pending[0]["state"] == "in_flight", "pending attempt is durable before receipt")

    driver.event("network_lost")
    require(driver.native.request("REOPEN") == "READY", "reopen the same SQLite account")
    driver.clock += 300_001
    driver.event("select_conversation", conversationId=ROOM)
    effects = driver.event("network_available")
    ready(effects, "reopened local operation sequence")
    require(driver.subscriptions[-1] == "5", "reopened durable sequence")
    receipts = [event for event in effects if event["type"] == "send_ack"]
    require(len(receipts) == 1 and receipts[0]["serverMsgId"] == original_receipt["serverMsgId"] and
            receipts[0]["conversationSeq"] == original_receipt["conversationSeq"],
            "reopened send recovered the original authoritative receipt")
    require(not driver.step({"type": "http", "operation": send["operation"], "status": code,
                             "verified": True, "body": body}, status=1), "retired send completion rejected")
    require(not driver.event("list_pending", limit=16)[0]["items"], "recovered ack removed pending intent")
    messages.append(replay_client)
    driver.messages(messages)
    driver.event("logout")
    require(not driver.step({"type": "list_messages", "conversationId": ROOM, "limit": 32}, status=3),
            "logged-out bridge cannot expose cached messages")


def reject_send(driver, client):
    effects = driver.step({"type": "queue_send", "body": wire(client, "authentication recovery")})
    require([effect["type"] for effect in effects] == ["send_queued", "http"],
            "hold the current persisted send")
    send = effects[1]
    code, body = driver.http("unauthenticated", send["path"], send["body"])
    require(code == 401, "authoritative authentication rejection over TLS")
    return {"type": "http", "operation": send["operation"],
            "status": code, "verified": True, "body": body}


def exercise_authentication_recovery(driver, root):
    driver.event("resume")
    driver.event("network_lost")
    ready(driver.event("network_available"), "transport reconnected using the current credentials")

    client = "native_rejected_after_reconnect"
    rejection = reject_send(driver, client)
    effects = driver.step(rejection)
    require(any(effect["type"] == "send_failed" and effect["code"] == "AUTH_REQUIRED"
                for effect in effects), "record the rejected send")
    require(any(effect["type"] == "credentials_required" and effect["cause"] == "rejected"
                for effect in effects), "request credentials after the persisted auth fence changes")
    require(not driver.step({"type": "list_messages", "conversationId": ROOM, "limit": 32},
                            status=3), "block cached reads until authentication recovers")
    require(not driver.step(rejection, status=1), "retire the rejected send completion")
    ready(driver.run(effects), "re-authenticate, then load the current directory")
    driver.event("dismiss_send", clientId=client)

    rejection = reject_send(driver, "native_rejected_offline")
    driver.event("network_lost")
    with closing(sqlite3.connect(root / "active" / "store.db")) as store:
        generation, = store.execute("SELECT generation FROM store_metadata WHERE id=1").fetchone()
        store.execute("BEGIN IMMEDIATE")
        driver.step(rejection, status=3)
        require(not driver.step({"type": "list_messages", "conversationId": ROOM, "limit": 32},
                                status=6), "failed SQLite work does not restore local access")
        require(not driver.step({"type": "network_available"}, status=3),
                "record network changes while waiting for the store")
        require(not driver.step({"type": "logout"}, status=3),
                "logout overrides pending credential recovery")
        store.rollback()
        require(all(effect["type"] == "cancel" for effect in driver.event("tick")),
                "commit the fence without requesting credentials after logout")
        current, = store.execute("SELECT generation FROM store_metadata WHERE id=1").fetchone()
        require(current == generation + 1, "persist the auth fence once after SQLite recovers")
    require(not driver.event("network_available"), "network availability preserves logout")
    require(not driver.step(rejection, status=1), "reject completions after the auth fence changed")


def exercise_logout_during_history(driver, root):
    ready(driver.event("resume"), "explicit resume after logout")
    driver.event("network_lost")
    driver.peer_message("native_gap_during_logout", "history request before logout")
    path = "/api/v1/sync/messages"
    effects = driver.run(driver.step({"type": "network_available"}), hold_http=path)
    history = [effect for effect in effects if effect.get("path") == path]
    require(len(history) == 1 and driver.socket is not None, "hold active history recovery")
    with closing(sqlite3.connect(root / "active" / "store.db")) as store:
        generation, = store.execute("SELECT generation FROM store_metadata WHERE id=1").fetchone()
        store.execute("BEGIN IMMEDIATE")
        effects = driver.step({"type": "logout"}, status=3)
        require({effect["operation"] for effect in effects if effect["type"] == "cancel"} ==
                {driver.socket, history[0]["operation"]}, "cancel all work while SQLite is busy")
        require(not driver.run(effects), "logout has no visible data or new credential request")
        store.rollback()
        require(not driver.event("tick"), "retry the pending auth fence after cleanup fails")
        current, = store.execute("SELECT generation FROM store_metadata WHERE id=1").fetchone()
        require(current == generation + 1, "persist the logout fence after interrupted history")
    require(not driver.event("network_available"), "logout stays closed after the store recovers")


def main():
    require(len(sys.argv) == 2 and re.fullmatch(r"newim-db-[0-9a-f]{32}", sys.argv[1]),
            "run through the owned TLS fixture")
    go_fixture = ["docker", "exec", "-i",
                  "-e", "NEWIM_GATEWAY_BINARY=/tmp/newim-gateway",
                  "-e", "NEWIM_SERVER_BINARY=/tmp/newim-server",
                  "-e", "NEWIM_SYNC_BACKFILL_BINARY=/tmp/newim-sync-backfill",
                  sys.argv[1], "/tmp/conversationsynchttp.test", "-test.run",
                  "^TestSDKObservabilityBridge$", "-test.count=1", "-test.timeout=120s"]
    with tempfile.TemporaryDirectory(prefix="newim-native-tls-") as directory:
        parent = Path(directory).resolve()
        host = build_host(parent)
        root = parent / "account"
        root.mkdir(mode=0o700)
        with Pipe([str(host), "--relay", os.fspath(root), ACCOUNT]) as native, Pipe(go_fixture) as tls:
            driver = Driver(native, tls)
            exercise(driver)
            exercise_authentication_recovery(driver, root)
            exercise_logout_during_history(driver, root)
            native.finish()
            tls.finish(expect_pass=True)
    print("native C ABI, SQLite and TLS recovery passed")


if __name__ == "__main__":
    main()
