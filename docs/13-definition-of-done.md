# Definition of done

A task is done only when its acceptance criteria pass and its public behavior is documented. For reliability-sensitive code, “works in happy path” is not completion.

Every implementation task must answer: what happens on duplicate input, timeout, retry, process restart, stale cursor/token, schema mismatch, partial dependency outage and rolling upgrade? Not every task needs all cases, but relevant ones require explicit tests or documented non-applicability.
