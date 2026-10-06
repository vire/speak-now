# Speak Now agent instructions

Speak Now uses Bun, TypeScript, and React to turn existing Herdr agent updates into short spoken summaries. Read `README.md` for local operation and capture configuration; `package.json` is the source of truth for commands.

## Mandatory feature worktrees

- Every new feature MUST use a dedicated branch and Git worktree under the repository's `.agents/worktrees/` directory. Create or select the worktree before editing feature code, and run all implementation, dependency installation, and verification commands inside that checkout.
- Resolve the main repository root with `git worktree list`; create worktrees beneath that root, rather than nesting them inside another worktree. For example, from the main checkout:

  ```sh
  git worktree add .agents/worktrees/sn-123 -b feature/sn-123
  cd .agents/worktrees/sn-123
  bun install
  ```

- Give concurrent features separate worktrees. Preserve unrelated changes in the main checkout and other worktrees. Commit only files belonging to the current task; integrate completed feature branches after their verification and review.
- Worktree directories are local, ignored artifacts. Keep `.agents/worktrees/` excluded from Git and Docker build context.

## Pull requests

- Open normal pull requests ready for review. Never open a draft PR unless the user explicitly instructs "Open Draft PR". Pending review or verification belongs in the PR description and does not authorize draft status.

- Agents must never merge a pull request unless it currently has the `merge-authorized` label. Check the live labels immediately before merging; if the label is absent or cannot be verified, leave the PR open and report that merge authorization is missing.
- Agents must not add `merge-authorized` themselves to authorize a merge.
- Direct pushes or force pushes that would cause an open PR to be marked merged require the same label check.

## Plans and local artifacts

- Never commit implementation plans, task plans, handoffs, or local review/evidence files. Keep them in ignored `tmp/` or the shared taskboard; put reviewer-facing scope and validation in the PR description.
- Before committing, inspect the staged file list and unstage any plan or local coordination artifact.

## Effect

- Use Effect for recoverable errors, resource acquisition and cleanup, concurrency, cancellation, timeouts, and retry policies in new or changed backend workflows. Represent expected failures with typed errors and handle them through Effect operators.
- Never fall back to handwritten error handling, error-message matching, manual Promise orchestration, or custom lifecycle machinery for concerns Effect supports. Keep Effect as a production dependency; performance concerns call for improving the Effect implementation, not replacing it with a native fallback.
- Wrap unavoidable native APIs in narrow Effect adapters. Lift failures with `Effect.try` or `Effect.tryPromise`, own resources with `Effect.acquireRelease` and `Effect.scoped`, and keep native cancellation and process reaping explicit inside those adapters.
- Run Effects at application boundaries and preserve existing public contracts, including Promise interfaces. Keep one terminal reporting owner, safe typed diagnostics, and the existing retry budget; cancellation must not become a reported failure.

## Behavior changes

- **Spec -> test -> code, in that order. Always.** Every behavior change follows the triad:

  1. **Spec** - the intended behavior is written down before any test or code exists. The task as stated,
     a Linear issue, or an ignored local spec in `tmp/` all count; if none covers the change, write the spec yourself
     (a paragraph is enough) where the reviewer will read it: the top of the PR description, or the
     hand-back when the change stays in the working tree. An agent-written spec counts, and nobody's
     approval gates the next step. What makes it a spec is that it comes first and is concrete: the
     behavior, the inputs and outcomes a test can check, and what is out of scope, taken from the task,
     never from the code. Ask only when the task reads two ways that would lead to different tests.
  2. **Test** - a test written from the spec (unit test, `eve eval`, or policy test), run on its own and
     seen to fail for the right reason. Where no test can express the spec (compose, Dockerfiles, `infra/`,
     `seed.json`, teach HTML, docs), the gate is the CI job step that covers the file.
  3. **Implementation** - code is written only to make those tests pass.

  Never write a new unit test after the implementation it covers: a test written against finished code
  asserts what the code does, not what the spec requires. If the behavior you are changing has no test
  yet, go back to step 1 - state the intended behavior from the issue, the docs or the task, write it
  down, then write the test from that statement, not from the code. A bug fix is the same triad: the
  issue is the spec, the regression test reproduces it and fails on `main`, the fix makes it pass. Pure
  refactors keep the existing tests green and need no new spec.

## Setup and verification

- Use Bun 1.3 or newer. Run `bun install` in each new checkout, then `bun run dev` for local development at `http://localhost:3000`.
- The dev command builds the browser bundle once and watches the server. After client edits, run `bun run build:client` to refresh the bundle.
- Run `bun run typecheck` and `bun run build` for code changes, plus relevant tests through `bun test ./src` when present. Verify within the task's worktree.
- The collector runs on the host that owns Herdr. Follow the README for configuration and exclude implementation and review panes through `OBSERVATION_EXCLUDE_PANES` when exercising capture.
- Keep `.env`, credentials, captured transcripts, generated audio, and temporary runtime files out of commits. Configure each worktree locally without printing secrets.

## Capture boundaries

- Treat observed sessions as read-only. The collector reads Herdr state and exact transcript records; it must never resume, prompt, attach to, or control an observed session.
- Match sessions using the exact agent session identity advertised by Herdr. A missing identity must remain explicit rather than guessed from labels or paths.
- Keep summary workers isolated from project state and observed sessions. Preserve bounded structured output and the existing worker sandbox/config isolation.

## Code discovery

- Prefer codebase-memory-mcp for code discovery. If the repository is not indexed, run `index_repository` first.
- Use `search_graph` for symbols, `trace_path` for callers and callees, `get_code_snippet` for source, `query_graph` for complex relationships, and `get_architecture` for the project overview.
- Fall back to `rg` for literals, error messages, configuration, non-code files, or graph coverage gaps. Use `rg --files` to list files; use `rg` instead of `grep` or `find`.

## Herdr and writing conventions

- Use the Herdr skill when asked to inspect or control Herdr. Verify `HERDR_ENV=1` before any control command, use explicit pane IDs or `--current`, and preserve user focus with `--no-focus` for background work.
- When creating a Herdr tab, use the Linear ticket ID as its name, such as `SN-123`. If there is no Linear ticket ID, use the GitHub pull request number, such as `PR-123`.
- Use regular dashes with spaces in prose. No em dashes in code, comments, or content.
