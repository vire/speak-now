export interface Migration {
  id: number;
  statements: readonly string[];
}

export const migrations: readonly Migration[] = [
  {
    id: 1,
    statements: [
      `CREATE TABLE IF NOT EXISTS sources (
        source_id TEXT PRIMARY KEY,
        epoch TEXT NOT NULL,
        topology_sequence INTEGER NOT NULL DEFAULT 0,
        topology_digest TEXT NOT NULL DEFAULT '',
        observed_at TEXT NOT NULL,
        stale INTEGER NOT NULL DEFAULT 0,
        listening_generation INTEGER NOT NULL DEFAULT 0,
        scope_json TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS workspaces (
        workspace_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        label TEXT NOT NULL,
        ordering INTEGER NOT NULL,
        live INTEGER NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS tabs (
        tab_id TEXT PRIMARY KEY,
        workspace_id TEXT NOT NULL REFERENCES workspaces(workspace_id),
        label TEXT NOT NULL,
        ordering INTEGER NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS panes (
        pane_id TEXT PRIMARY KEY,
        tab_id TEXT NOT NULL REFERENCES tabs(tab_id),
        terminal_id TEXT NOT NULL,
        cwd TEXT,
        label TEXT
      )`,
      `CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        pane_id TEXT NOT NULL REFERENCES panes(pane_id),
        value_json TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS cursors (
        participant_id TEXT PRIMARY KEY REFERENCES participants(participant_id),
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        cursor_value TEXT NOT NULL,
        baseline_generation INTEGER
      )`,
      `CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        digest TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        source_cursor TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        evidence_json TEXT,
        expires_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS batches (
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        batch_id TEXT NOT NULL,
        digest TEXT NOT NULL,
        receipt_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (source_id, batch_id)
      )`,
      `CREATE TABLE IF NOT EXISTS jobs (
        job_id TEXT PRIMARY KEY,
        input_key TEXT NOT NULL UNIQUE,
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        participant_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        evidence_json TEXT,
        status TEXT NOT NULL,
        attempts INTEGER NOT NULL DEFAULT 0,
        lease_token TEXT,
        lease_expires_at TEXT,
        result_key TEXT,
        result_digest TEXT,
        receipt_json TEXT,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS announcements (
        announcement_id TEXT PRIMARY KEY,
        job_id TEXT NOT NULL UNIQUE REFERENCES jobs(job_id),
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        participant_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        summary_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        expires_at TEXT NOT NULL
      )`,
      `CREATE TABLE IF NOT EXISTS durable_events (
        sequence INTEGER PRIMARY KEY AUTOINCREMENT,
        kind TEXT NOT NULL,
        value_json TEXT NOT NULL,
        created_at TEXT NOT NULL
      )`,
      "CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs(status, lease_expires_at, created_at)",
      "CREATE INDEX IF NOT EXISTS events_expiry ON events(expires_at)",
      "CREATE INDEX IF NOT EXISTS announcements_expiry ON announcements(expires_at)",
    ],
  },
  {
    id: 2,
    statements: [
      "ALTER TABLE workspaces ADD COLUMN present INTEGER NOT NULL DEFAULT 1",
      "ALTER TABLE tabs ADD COLUMN present INTEGER NOT NULL DEFAULT 1",
      "ALTER TABLE panes ADD COLUMN present INTEGER NOT NULL DEFAULT 1",
      "ALTER TABLE participants ADD COLUMN present INTEGER NOT NULL DEFAULT 1",
      "CREATE INDEX IF NOT EXISTS workspaces_current ON workspaces(source_id, present)",
      "CREATE INDEX IF NOT EXISTS participants_current ON participants(source_id, present)",
    ],
  },
  {
    id: 3,
    statements: [
      "ALTER TABLE jobs ADD COLUMN completion_lease_token TEXT",
      "CREATE INDEX IF NOT EXISTS durable_events_retention ON durable_events(created_at)",
    ],
  },
  {
    id: 4,
    statements: [
      "ALTER TABLE jobs ADD COLUMN result_json TEXT",
      "ALTER TABLE jobs ADD COLUMN capture_json TEXT",
      "CREATE TABLE IF NOT EXISTS storage_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    ],
  },
  {
    id: 5,
    statements: [
      "ALTER TABLE jobs ADD COLUMN terminal_expires_at TEXT",
      "CREATE INDEX IF NOT EXISTS jobs_terminal_expiry ON jobs(status, terminal_expires_at)",
    ],
  },
  {
    id: 6,
    statements: [
      "ALTER TABLE jobs ADD COLUMN terminal_outcome TEXT",
      "CREATE INDEX IF NOT EXISTS jobs_participant_claim ON jobs(source_id, participant_id, status, lease_expires_at)",
    ],
  },
  {
    id: 7,
    statements: [
      "ALTER TABLE events ADD COLUMN capture_json TEXT",
      "ALTER TABLE jobs ADD COLUMN catch_up_request_id TEXT",
      `CREATE TABLE IF NOT EXISTS catch_up_requests (
        source_id TEXT NOT NULL REFERENCES sources(source_id),
        generation INTEGER NOT NULL,
        request_id TEXT NOT NULL,
        scope_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (source_id, generation, request_id)
      )`,
      `CREATE TABLE IF NOT EXISTS catch_up_entries (
        source_id TEXT NOT NULL,
        generation INTEGER NOT NULL,
        request_id TEXT NOT NULL,
        participant_id TEXT NOT NULL,
        job_id TEXT,
        capture_json TEXT NOT NULL,
        evidence_refs_json TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        reason TEXT,
        PRIMARY KEY (source_id, generation, request_id, participant_id),
        FOREIGN KEY (source_id, generation, request_id) REFERENCES catch_up_requests(source_id, generation, request_id)
      )`,
    ],
  },
];
