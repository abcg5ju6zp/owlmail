# Delivery waits and test runs

These routes and cursor-aware MCP selectors are included in OwlMail **0.11.1**. They are absent from the original `v0.10.1` binary or image.

A delivery cursor closes the race between triggering application mail and
registering a waiter. Obtain it **before** triggering the application:

```sh
curl http://localhost:1080/api/v1/emails/cursor
# {"cursor":"<process-epoch>:<sequence>"}
```

Send mail with `X-OwlMail-Run-ID: checkout-42`, then wait:

```sh
curl -H 'Content-Type: application/json' \
  -d '{"after":"<cursor>","run_id":"checkout-42","recipient":"user@example.test","count":1,"timeout_seconds":30}' \
  http://localhost:1080/api/v1/emails/wait
```

`recipient` matches the complete SMTP envelope recipient **case-sensitively**,
not the visible To header. `run_id` matches the header exactly. Run IDs contain
1–128 ASCII letters, digits, `.`, `_`, `:`, or `-`; omission leaves mail ungrouped.
A duplicate or invalid run header rejects the SMTP transaction. Grouping survives
EML reloads without changing the sidecar format. It is a selector, not tenant
isolation or an authorization boundary.

The response contains `emails`, `cursor`, `timed_out`, and
`waited_milliseconds`. `count` defaults to 1 and is an **at-least** threshold
from 1 through 100. The first matching retained messages are returned; this is
not an exactly-one assertion. Timeout defaults to 30 seconds, maximum 120. A
normal timeout is HTTP 200 with `timed_out: true` and any partial matches. Reuse
the **original** cursor when retrying an incomplete count. A timeout does not
promise that mail will never arrive later.

At most 64 mailbox waits run concurrently; excess requests receive 429 and
`Retry-After: 1`. Shutdown cancels waits with 503. Invalid or previous-process
cursors receive 400. Cursors are process-scoped and do not survive a restart.

Deleted messages stay replayable for waiters that registered **before** the
deletion: a single delete moves the email into a small graveyard and wakes the
waiter, so cleanup never makes a blocked waiter skip an unreturned match.
Graveyard records are retained for 150 seconds (and never past 10,000 messages);
they are also reclaimed sooner once every registered waiter has a newer
cursor. When the cursor's replay window has been reclaimed — age-based
reclaim, global mailbox clear, or a read-only observer refresh that removed the
message — the next wait returns HTTP 400 with error code `CURSOR_EXPIRED`.
Re-synchronize with a fresh cursor from `GET /api/v1/emails/cursor`; do not keep
polling an expired cursor. Obtain the cursor before triggering the application
and do not delete mail a test still needs. A wait reads detached snapshots and
never changes read state. HTTP client disconnection is subject to the server
framework's cancellation behavior; the bounded timeout always applies.

Lists, previews, and exports accept `run_id` and `recipient`. Both selectors
compose with existing filters.

## Snapshot cleanup of a test run

Run-scoped cleanup is explicit and **snapshot-based**:

```sh
# First page: pins the currently visible set (at most 1000 per page)
curl -X DELETE http://localhost:1080/api/v1/test-runs/checkout-42/emails
# {"run_id":"checkout-42","deleted":["id1"],...,"failed":0,"failures":[],"more_possible":true,"next_cursor":"rc1...."}

# Continue exactly the same snapshot while more_possible is true
curl -X DELETE "http://localhost:1080/api/v1/test-runs/checkout-42/emails?cursor=rc1....&limit=1000"
```

`deleted` contains the IDs removed on **this page**. Each blocked member is
listed in `failures` with a stable `reason` and keeps all its source data:

- `relay_pending` — an accepted relay job still owns the source; retry later.
- `source_in_use` — a download, stream, or other reader has the source open;
  the round never blocks the whole page.
- `storage_unavailable` — a storage layer deletion failed; the tombstone lets
  it resume from the unfinished layer on the next attempt.

`failed` is the number of failures on this page. The pinned set is fixed for
the whole round: messages arriving after the pin belong to a later round and
are deleted only when you call the endpoint again without a cursor. The
`limit` query parameter is 1..1000 (default 1000). An unknown, stale, or
foreign `cursor` returns 400; so do invalid run IDs and limits. A round with no
matching email returns 200 with empty arrays and writes no snapshot. When the
server restarts mid-round it silently takes over the unfinished snapshot;
queued relay sources are preserved as `relay_pending`.

Never use global mailbox deletion to clean up a shared test run: it removes
messages from every run and expires older wait cursors with `CURSOR_EXPIRED`.

MCP `list_emails` and `search_emails` return a `cursor` and accept `run_id` and
`recipient`. Capture that cursor before triggering the application, then pass
`after`, `run_id`, and `recipient` to `wait_for_email`. With `after`, use exact
`recipient` rather than the legacy substring `to`. Omitting `after` preserves
the existing new-delivery-only contract. Existing MCP session/process quotas
still apply; cursor-based waits also share the mailbox wait quota. MCP stays
read-only and exposes no cleanup tool.

MCP `wait_for_email` accepts no `count` parameter and returns at most one summary
in `email`, with `matched` and `timed_out`. The `count` threshold and plural
`emails` response above belong to the REST endpoint.
