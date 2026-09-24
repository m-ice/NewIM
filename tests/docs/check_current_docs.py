#!/usr/bin/env python3
"""Check current architecture docs for stale claims and required boundaries."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DOCUMENTS = (
    ROOT / "docs" / "architecture.md",
    ROOT / "server" / "README.md",
)

STALE_CLAIMS = (
    (
        "standard-library-only",
        re.compile(r"\bstandard\s+librar(?:y|ies)\s+only\b", re.IGNORECASE),
    ),
    (
        "not-implemented-functionality",
        re.compile(
            r"\bnot\s+implemented\s+functionality\b",
            re.IGNORECASE,
        ),
    ),
    (
        "later-domain-protocol-data-modules",
        re.compile(
            r"\bbelong\s+to\s+later\s+domain,\s+protocol\s+and\s+data\s+modules\b",
            re.IGNORECASE,
        ),
    ),
    (
        "server-no-message-persistence",
        re.compile(
            r"\bdoes\s+not\s+include\s+a\s+network\s+server\s+or\s+message\s+persistence\b",
            re.IGNORECASE,
        ),
    ),
)

NEGATIVE_FIXTURES = (
    (
        "architecture-standard-library-only",
        "Runtime source dependencies are Go/Rust standard libraries only.",
        "standard-library-only",
    ),
    (
        "architecture-not-implemented",
        "These are architectural constraints, not implemented functionality.",
        "not-implemented-functionality",
    ),
    (
        "architecture-later-modules",
        "Durable side effects belong to later domain, protocol and data modules.",
        "later-domain-protocol-data-modules",
    ),
    (
        "server-no-persistence",
        "This foundation does not include a network server or message persistence.",
        "server-no-message-persistence",
    ),
)

REQUIRED_TEXT = {
    "docs/architecture.md": (
        ("Go pgx module", re.compile(r"\bpgx\b", re.IGNORECASE)),
        ("Rust serde_json dependency", re.compile(r"\bserde_json\b", re.IGNORECASE)),
        ("Rust rusqlite dependency", re.compile(r"\brusqlite\b", re.IGNORECASE)),
        (
            "Rust libsqlite3-sys dependency",
            re.compile(r"\blibsqlite3-sys\b", re.IGNORECASE),
        ),
        ("PostgreSQL persistence", re.compile(r"\bPostgreSQL\b", re.IGNORECASE)),
        ("message persistence", re.compile(r"\bmessages?\b", re.IGNORECASE)),
        ("transactional outbox", re.compile(r"\boutbox\b", re.IGNORECASE)),
        ("transaction boundary", re.compile(r"\btransactions?\b", re.IGNORECASE)),
        (
            "server conversation sequence",
            re.compile(r"\bconversationSeq\b|\bconversation\s+sequence\b", re.IGNORECASE),
        ),
        ("auth/session boundary", re.compile(r"\bauth/session\b", re.IGNORECASE)),
        ("conversation/message sync", re.compile(r"\bsync\b", re.IGNORECASE)),
        ("media metadata", re.compile(r"\bmedia\b", re.IGNORECASE)),
        ("SQLite adapter", re.compile(r"\bSQLite\b", re.IGNORECASE)),
        ("SDK outbox", re.compile(r"\bSDK\b", re.IGNORECASE)),
        ("future boundary", re.compile(r"\bfuture\b", re.IGNORECASE)),
        ("public network boundary", re.compile(r"public\s+network", re.IGNORECASE)),
        ("HTTP service foundation", re.compile(r"HTTP\s+service\s+foundation", re.IGNORECASE)),
        ("loopback listener", re.compile(r"loopback", re.IGNORECASE)),
        ("push boundary", re.compile(r"\bpush\b", re.IGNORECASE)),
        ("current token revocation", re.compile(r"current[- ]token\s+revocation", re.IGNORECASE)),
        ("DELETE token route", re.compile(r"DELETE\s+/api/v1/session/tokens/current", re.IGNORECASE)),
    ),
    "server/README.md": (
        ("server message package", re.compile(r"server/message", re.IGNORECASE)),
        ("server storage package", re.compile(r"server/storage", re.IGNORECASE)),
        (
            "message persistence statement",
            re.compile(r"message\s+persistence", re.IGNORECASE),
        ),
        ("PostgreSQL persistence", re.compile(r"\bPostgreSQL\b", re.IGNORECASE)),
        ("no public network server", re.compile(r"no\s+public\s+network\s+server", re.IGNORECASE)),
        ("UI boundary", re.compile(r"\bUI\b", re.IGNORECASE)),
        ("current token revocation", re.compile(r"current[- ]token\s+revocation", re.IGNORECASE)),
        ("DELETE token route", re.compile(r"DELETE\s+/api/v1/session/tokens/current", re.IGNORECASE)),
    ),
}

REQUIRED_LINKS = {
    "docs/architecture.md": (
        "adr/0002-language-toolchain.md",
        "adr/0007-internal-conversation-sync.md",
        "adr/0008-auth-session-core.md",
        "adr/0009-message-send-transaction.md",
        "adr/0011-media-credential-metadata.md",
        "adr/0013-http-api-service-foundation.md",
        "adr/0019-bearer-token-revocation.md",
        "../specs/http/service-foundation.md",
        "../specs/http/session-logout.md",
        "dependencies/protocol-v1.md",
        "dependencies/conversation-sync.md",
        "dependencies/local-store.md",
        "../THIRD_PARTY_NOTICES.md",
    ),
    "server/README.md": (
        "../docs/architecture.md",
        "../docs/adr/0013-http-api-service-foundation.md",
        "../docs/adr/0019-bearer-token-revocation.md",
        "../specs/http/service-foundation.md",
        "../specs/http/session-logout.md",
        "../THIRD_PARTY_NOTICES.md",
    ),
}

CONTRACTS = (
    (
        "docs/adr/0019-bearer-token-revocation.md",
        (
            "amends adr 0018",
            "delete /api/v1/session/tokens/current",
            "does not revoke its session",
            "session before token",
        ),
    ),
    (
        "specs/http/service-foundation.md",
        (
            "adr 0019",
            "allowedmethods",
            "get",
            "delete",
        ),
    ),
    (
        ".github/workflows/ci.yml",
        (
            "make auth-logout-check",
            "make auth-logout-security",
        ),
    ),
)

CONTRACT_NEGATIVE_FIXTURES = (
    ("missing-marker", "one marker only", ("one marker only", "second marker"), True),
    ("all-markers", "one marker only and second marker", ("one marker only", "second marker"), False),
)

MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")


def normalize(text: str) -> str:
    return " ".join(text.split())


def stale_claims(text: str) -> list[tuple[str, str]]:
    normalized = normalize(text)
    return [
        (claim_id, pattern.pattern)
        for claim_id, pattern in STALE_CLAIMS
        if pattern.search(normalized)
    ]


def check_negative_fixtures() -> list[str]:
    errors: list[str] = []
    for name, fixture, expected_claim in NEGATIVE_FIXTURES:
        claim_ids = {claim_id for claim_id, _ in stale_claims(fixture)}
        if expected_claim not in claim_ids:
            errors.append(
                f"negative fixture {name!r} was not rejected by {expected_claim!r}"
            )
    return errors


def link_targets(text: str) -> list[str]:
    targets: list[str] = []
    for match in MARKDOWN_LINK.finditer(text):
        target = match.group(1).strip()
        if target.startswith("<") and target.endswith(">"):
            target = target[1:-1].strip()
        target = target.split(maxsplit=1)[0]
        targets.append(target)
    return targets


def validate_document(path: Path) -> list[str]:
    relative = path.relative_to(ROOT).as_posix()
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"{relative}: cannot read document: {exc}"]

    errors: list[str] = []
    claims = stale_claims(text)
    for claim_id, _ in claims:
        errors.append(f"{relative}: contains stale claim {claim_id!r}")

    normalized = normalize(text)
    for label, pattern in REQUIRED_TEXT.get(relative, ()):
        if not pattern.search(normalized):
            errors.append(f"{relative}: missing required marker {label!r}")

    found_targets = set(link_targets(text))
    for required_target in REQUIRED_LINKS.get(relative, ()):
        if required_target not in found_targets:
            errors.append(f"{relative}: missing required link {required_target!r}")

    for target in sorted(found_targets):
        if target.startswith(("http://", "https://", "mailto:", "#")):
            continue
        path_part = target.split("#", 1)[0]
        if not path_part:
            continue
        resolved = (path.parent / path_part).resolve()
        if not resolved.is_file():
            errors.append(f"{relative}: link target does not exist: {target!r}")

    return errors


def contract_errors(text: str, markers: tuple[str, ...]) -> list[str]:
    normalized = normalize(text).lower()
    return [f"missing required contract marker {marker!r}" for marker in markers if marker.lower() not in normalized]


def check_contracts() -> list[str]:
    errors = []
    for relative, markers in CONTRACTS:
        path = ROOT / relative
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            errors.append(f"{relative}: cannot read contract: {exc}")
            continue
        errors.extend(f"{relative}: {error}" for error in contract_errors(text, markers))
    return errors


def check_contract_negative_fixtures() -> list[str]:
    errors = []
    for name, text, markers, want_error in CONTRACT_NEGATIVE_FIXTURES:
        found = contract_errors(text, markers)
        if bool(found) != want_error:
            errors.append(f"contract negative fixture {name!r} did not produce expected result")
    return errors


def run_document_check() -> list[str]:
    errors = check_negative_fixtures()
    if errors:
        return errors
    errors = check_contract_negative_fixtures()
    if errors:
        return errors
    errors = check_contracts()
    if errors:
        return errors
    for path in DOCUMENTS:
        errors.extend(validate_document(path))
    return errors


def report(errors: list[str]) -> int:
    if not errors:
        print("docs-check: OK")
        return 0
    print("docs-check: FAILED", file=sys.stderr)
    for error in errors:
        print(f"- {error}", file=sys.stderr)
    return 1


def check_stale_fixture(path: Path) -> int:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return report([f"{path}: cannot read stale fixture: {exc}"])
    claims = stale_claims(text)
    if not claims:
        return report([f"{path}: expected a stale claim, but none was detected"])
    print(f"docs-check: rejected fixture {path}: {', '.join(claim_id for claim_id, _ in claims)}")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="run only the negative stale-fixture self-test",
    )
    parser.add_argument(
        "--stale-fixture",
        type=Path,
        metavar="PATH",
        help="scan one fixture and return 1 when a stale claim is detected",
    )
    args = parser.parse_args()

    if args.self_test:
        errors = check_negative_fixtures() + check_contract_negative_fixtures()
        if errors:
            return report(errors)
        print(
            "docs-check: negative self-test rejected "
            f"{len(NEGATIVE_FIXTURES)}/{len(NEGATIVE_FIXTURES)} stale fixtures"
        )
        return 0

    if args.stale_fixture is not None:
        return check_stale_fixture(args.stale_fixture)

    fixture_errors = check_negative_fixtures() + check_contract_negative_fixtures()
    if fixture_errors:
        return report(fixture_errors)
    errors = run_document_check()
    if not errors:
        print(
            "docs-check: negative self-test rejected "
            f"{len(NEGATIVE_FIXTURES)}/{len(NEGATIVE_FIXTURES)} stale fixtures"
        )
    return report(errors)


if __name__ == "__main__":
    sys.exit(main())
