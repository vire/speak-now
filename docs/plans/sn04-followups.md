# SN-04 review follow-ups

TB-0005: a nonzero summary worker exit carrying private evidence in stdout or stderr must produce exactly one correlated error report with fixed exit diagnostics, without any raw output in message, stack or diagnostic logs. Preserve bounded drains, isolated worker configuration and cancellation semantics. Raw output is not needed in the thrown error either.

TB-0006: speech setup failures (invalid timeout and audio directory creation) and provider failures whose temporary-file cleanup also fails must produce one actionable report with source/job/trace context. Cleanup must not replace the primary provider error or skip reporting and terminal spans. Caller cancellation remains report-free. Exercise existing fetch and filesystem boundaries with synthetic fixtures, without provider calls or new test-only production seams.

TB-0007: maintained summary regression coverage must assert persisted reports for subprocess nonzero exit, malformed output, deadline and schema write setup failure. Each failure produces exactly one correlated report. A pre-aborted caller produces neither a worker nor a report, including before backend/config validation. Repaired reporter destinations regain available status on a subsequent successful write. Existing return-value and lifecycle tests do not independently guard persistence; extend the privacy fixture for distinct categories rather than duplicating harnesses.

TB-0008: remove write-only activeStartedAt state and obsolete timestamp-return bookkeeping. Reconcile active and archive files in one canonical maintenance pass per append; preserve caps, per-row retention, process lock and recovered destination status. This is a pure refactor, validated with existing persistence tests.

Tests protect public persisted output and owner failures, with credible regressions documented in the review tickets. They use existing summarize/synthesize/reporter seams and no test-only exports or filesystem injection wrappers. Synthetic workers run under temporary HOME/PATH in a child process so tests never invoke a real agent or mutate the test runner's environment.
