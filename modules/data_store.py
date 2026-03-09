"""
data_store.py — SQLite persistence layer for DALA Analytics.

Schema:
  reports        — one row per month's generation run
  brand_kpis     — one row per brand per report
  daily_sales    — daily revenue/qty per brand per report
  alerts         — auto-generated alerts per report
  brand_tokens   — brand partner portal tokens

Usage:
  from modules.data_store import DataStore
  ds = DataStore()
  report_id = ds.save_report(...)
"""

import json
import os
import sqlite3
import uuid
from datetime import datetime
from difflib import SequenceMatcher

from .brand_names import canonicalize_brand_name, normalize_brand_compare_key, normalize_name_key

# Allow overriding via env var so a Railway Volume can be used for persistence.
# On Railway: set DATABASE_PATH=/data/dala_data.db and mount a Volume at /data
_default_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dala_data.db')
DB_PATH = os.environ.get('DATABASE_PATH', _default_db)

# If DATABASE_PATH points to a Volume path that has no data, seed it from the
# bundled dala_data.db. Handles two cases: file missing (fresh volume) and
# file present but empty (schema-only from a previous crashed deploy).
def _seed_volume_db_if_needed():
    if DB_PATH == _default_db:
        return  # not using a volume path, nothing to do
    if not os.path.isfile(_default_db):
        return  # no bundled seed to copy from

    needs_seed = not os.path.isfile(DB_PATH)
    if not needs_seed:
        try:
            _c = sqlite3.connect(DB_PATH)
            count = _c.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
            _c.close()
            needs_seed = (count == 0)
        except Exception:
            needs_seed = True

    if needs_seed:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        src = sqlite3.connect(_default_db)
        dst = sqlite3.connect(DB_PATH)
        src.backup(dst)
        dst.close()
        src.close()

_seed_volume_db_if_needed()


class DataStore:
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self._init_db()
        try:
            self.sync_catalog_from_history()
        except Exception:
            pass

    @staticmethod
    def _slugify(value):
        value = normalize_name_key(value)
        return value.replace(' ', '-')

    @staticmethod
    def _ordered_pair(left_key, right_key):
        return tuple(sorted((left_key, right_key)))

    @staticmethod
    def _brand_similarity_score(left_name, right_name):
        left_norm = normalize_name_key(left_name)
        right_norm = normalize_name_key(right_name)
        if not left_norm or not right_norm:
            return 0.0
        if left_norm == right_norm:
            return 1.0

        left_cmp = normalize_brand_compare_key(left_name)
        right_cmp = normalize_brand_compare_key(right_name)
        if left_cmp and left_cmp == right_cmp:
            return 0.99

        seq_norm = SequenceMatcher(None, left_norm, right_norm).ratio()
        seq_cmp = SequenceMatcher(None, left_cmp, right_cmp).ratio() if left_cmp and right_cmp else seq_norm

        left_tokens = set(left_cmp.split() or left_norm.split())
        right_tokens = set(right_cmp.split() or right_norm.split())
        overlap = (len(left_tokens & right_tokens) / len(left_tokens | right_tokens)) if (left_tokens or right_tokens) else 0.0
        contains = (
            (left_cmp and right_cmp and (left_cmp in right_cmp or right_cmp in left_cmp)) or
            (left_norm in right_norm or right_norm in left_norm)
        )

        score = max(seq_cmp, (seq_norm * 0.45) + (seq_cmp * 0.3) + (overlap * 0.25))
        if contains and overlap >= 0.5:
            score = max(score, 0.92)
        if left_tokens and right_tokens and (left_tokens.issubset(right_tokens) or right_tokens.issubset(left_tokens)):
            score = max(score, 0.9)
        return round(score, 4)

    @staticmethod
    def _sku_similarity_score(left_name, right_name):
        left_norm = normalize_name_key(left_name)
        right_norm = normalize_name_key(right_name)
        if not left_norm or not right_norm:
            return 0.0
        if left_norm == right_norm:
            return 1.0

        seq = SequenceMatcher(None, left_norm, right_norm).ratio()
        left_tokens = set(left_norm.split())
        right_tokens = set(right_norm.split())
        overlap = (len(left_tokens & right_tokens) / len(left_tokens | right_tokens)) if (left_tokens or right_tokens) else 0.0
        if (left_norm in right_norm or right_norm in left_norm) and overlap >= 0.5:
            return round(max(seq, 0.9), 4)
        return round(max(seq, (seq * 0.7) + (overlap * 0.3)), 4)

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        with self._connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS reports (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    month_label     TEXT NOT NULL,
                    start_date      TEXT NOT NULL,
                    end_date        TEXT NOT NULL,
                    xls_filename    TEXT,
                    report_type     TEXT DEFAULT 'monthly',
                    total_revenue   REAL DEFAULT 0,
                    total_qty       REAL DEFAULT 0,
                    total_stores    INTEGER DEFAULT 0,
                    brand_count     INTEGER DEFAULT 0,
                    generated_at    TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS brand_kpis (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id            INTEGER NOT NULL,
                    brand_name           TEXT NOT NULL,
                    total_revenue        REAL DEFAULT 0,
                    total_qty            REAL DEFAULT 0,
                    num_stores           INTEGER DEFAULT 0,
                    unique_skus          INTEGER DEFAULT 0,
                    trading_days         INTEGER DEFAULT 0,
                    repeat_stores        INTEGER DEFAULT 0,
                    single_stores        INTEGER DEFAULT 0,
                    repeat_pct           REAL DEFAULT 0,
                    avg_revenue_per_store REAL DEFAULT 0,
                    closing_stock_total  REAL DEFAULT 0,
                    stock_days_cover     REAL DEFAULT 0,
                    inv_health_status    TEXT,
                    perf_grade           TEXT,
                    perf_score           INTEGER DEFAULT 0,
                    perf_revenue_score   INTEGER DEFAULT 0,
                    perf_loyalty_score   INTEGER DEFAULT 0,
                    perf_reach_score     INTEGER DEFAULT 0,
                    perf_activity_score  INTEGER DEFAULT 0,
                    portfolio_share_pct  REAL DEFAULT 0,
                    wow_rev_change       REAL DEFAULT 0,
                    wow_qty_change       REAL DEFAULT 0,
                    peak_date            TEXT,
                    peak_revenue         REAL DEFAULT 0,
                    top_store_name       TEXT,
                    top_store_revenue    REAL DEFAULT 0,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS daily_sales (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id   INTEGER NOT NULL,
                    brand_name  TEXT NOT NULL,
                    date        TEXT NOT NULL,
                    revenue     REAL DEFAULT 0,
                    qty         REAL DEFAULT 0,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS alerts (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id    INTEGER NOT NULL,
                    brand_name   TEXT,
                    alert_type   TEXT NOT NULL,
                    severity     TEXT NOT NULL,
                    message      TEXT NOT NULL,
                    created_at   TEXT NOT NULL,
                    acknowledged INTEGER DEFAULT 0,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS brand_tokens (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_name  TEXT UNIQUE NOT NULL,
                    token       TEXT UNIQUE NOT NULL,
                    email       TEXT,
                    whatsapp    TEXT,
                    active      INTEGER DEFAULT 1,
                    created_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS brand_targets (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_name   TEXT NOT NULL,
                    month_label  TEXT NOT NULL,
                    target_revenue REAL DEFAULT 0,
                    target_stores  INTEGER DEFAULT 0,
                    target_repeat_pct REAL DEFAULT 0,
                    set_by       TEXT DEFAULT 'manual',
                    created_at   TEXT NOT NULL,
                    UNIQUE(brand_name, month_label)
                );

                CREATE TABLE IF NOT EXISTS brands_master (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_name        TEXT NOT NULL,
                    canonical_name    TEXT NOT NULL,
                    canonical_key     TEXT NOT NULL UNIQUE,
                    slug              TEXT NOT NULL UNIQUE,
                    status            TEXT DEFAULT 'active',
                    category          TEXT,
                    start_date        TEXT,
                    default_email     TEXT,
                    default_whatsapp  TEXT,
                    notes             TEXT,
                    created_at        TEXT NOT NULL,
                    updated_at        TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS brand_aliases (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id     INTEGER NOT NULL,
                    alias_name   TEXT NOT NULL,
                    alias_key    TEXT NOT NULL UNIQUE,
                    created_at   TEXT NOT NULL,
                    FOREIGN KEY (brand_id) REFERENCES brands_master(id)
                );

                CREATE TABLE IF NOT EXISTS skus_master (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id            INTEGER NOT NULL,
                    sku_name            TEXT NOT NULL,
                    canonical_sku_name  TEXT NOT NULL,
                    canonical_key       TEXT NOT NULL,
                    sku_code            TEXT,
                    pack_size           TEXT,
                    unit_type           TEXT,
                    status              TEXT DEFAULT 'active',
                    launch_date         TEXT,
                    notes               TEXT,
                    created_at          TEXT NOT NULL,
                    updated_at          TEXT NOT NULL,
                    UNIQUE(brand_id, canonical_key),
                    FOREIGN KEY (brand_id) REFERENCES brands_master(id)
                );

                CREATE TABLE IF NOT EXISTS sku_aliases (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    sku_id       INTEGER NOT NULL,
                    brand_id     INTEGER NOT NULL,
                    alias_name   TEXT NOT NULL,
                    alias_key    TEXT NOT NULL,
                    created_at   TEXT NOT NULL,
                    UNIQUE(brand_id, alias_key),
                    FOREIGN KEY (sku_id) REFERENCES skus_master(id),
                    FOREIGN KEY (brand_id) REFERENCES brands_master(id)
                );

                CREATE TABLE IF NOT EXISTS catalog_review_queue (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type         TEXT NOT NULL,
                    raw_name            TEXT NOT NULL,
                    normalized_name     TEXT NOT NULL,
                    canonical_candidate TEXT,
                    brand_candidate     TEXT,
                    suggested_match_name TEXT,
                    similarity_score    REAL DEFAULT 0,
                    source_report_id    INTEGER,
                    source_filename     TEXT,
                    reason              TEXT DEFAULT 'new_detected',
                    status              TEXT DEFAULT 'pending',
                    review_note         TEXT,
                    created_at          TEXT NOT NULL,
                    reviewed_at         TEXT,
                    FOREIGN KEY (source_report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS catalog_distinct_rules (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type      TEXT NOT NULL,
                    left_key         TEXT NOT NULL,
                    right_key        TEXT NOT NULL,
                    brand_scope_key  TEXT DEFAULT '',
                    note             TEXT,
                    created_at       TEXT NOT NULL,
                    UNIQUE(entity_type, left_key, right_key, brand_scope_key)
                );

                CREATE TABLE IF NOT EXISTS alert_rules (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_name    TEXT NOT NULL,
                    brand_filter TEXT DEFAULT 'all',
                    metric       TEXT NOT NULL,
                    operator     TEXT NOT NULL,
                    threshold    REAL NOT NULL,
                    severity     TEXT DEFAULT 'medium',
                    active       INTEGER DEFAULT 1,
                    created_at   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS activity_log (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    action       TEXT NOT NULL,
                    detail       TEXT,
                    brand_name   TEXT,
                    report_id    INTEGER,
                    created_at   TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scheduled_reports (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    label        TEXT NOT NULL,
                    cron_expr    TEXT NOT NULL,
                    file_path    TEXT,
                    start_offset INTEGER DEFAULT 1,
                    end_offset   INTEGER DEFAULT 0,
                    active       INTEGER DEFAULT 1,
                    last_run     TEXT,
                    created_at   TEXT NOT NULL,
                    job_type     TEXT DEFAULT 'report',
                    target       TEXT,
                    payload_json TEXT DEFAULT '{}',
                    cadence      TEXT,
                    next_run     TEXT,
                    last_result  TEXT DEFAULT '{}',
                    status       TEXT DEFAULT 'active',
                    updated_at   TEXT,
                    connector    TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_bkpis_report ON brand_kpis(report_id);
                CREATE INDEX IF NOT EXISTS idx_daily_report ON daily_sales(report_id);
                CREATE INDEX IF NOT EXISTS idx_daily_brand  ON daily_sales(brand_name);
                CREATE INDEX IF NOT EXISTS idx_alerts_report ON alerts(report_id);
                CREATE INDEX IF NOT EXISTS idx_activity_log ON activity_log(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_targets_brand ON brand_targets(brand_name);
                CREATE INDEX IF NOT EXISTS idx_brands_master_status ON brands_master(status);
                CREATE INDEX IF NOT EXISTS idx_skus_master_brand ON skus_master(brand_id);
                CREATE INDEX IF NOT EXISTS idx_queue_status ON catalog_review_queue(status, entity_type, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_catalog_distinct_rules ON catalog_distinct_rules(entity_type, brand_scope_key);

                CREATE TABLE IF NOT EXISTS ai_narratives (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id   INTEGER NOT NULL,
                    brand_name  TEXT NOT NULL,
                    narrative   TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
                    UNIQUE(report_id, brand_name),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS brand_detail_json (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id           INTEGER NOT NULL,
                    brand_name          TEXT NOT NULL,
                    top_stores_json     TEXT DEFAULT '[]',
                    product_value_json  TEXT DEFAULT '[]',
                    product_qty_json    TEXT DEFAULT '[]',
                    closing_stock_json  TEXT DEFAULT '[]',
                    pickup_json         TEXT DEFAULT '[]',
                    supply_json         TEXT DEFAULT '[]',
                    reorder_json        TEXT DEFAULT '[]',
                    heatmap_json        TEXT DEFAULT '[]',
                    UNIQUE(report_id, brand_name),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS generation_jobs (
                    id          TEXT PRIMARY KEY,
                    status      TEXT DEFAULT 'running',
                    progress    INTEGER DEFAULT 0,
                    total       INTEGER DEFAULT 0,
                    current_brand TEXT,
                    report_id   INTEGER,
                    portfolio_file TEXT,
                    brands_done TEXT DEFAULT '[]',
                    errors      TEXT DEFAULT '[]',
                    result_json TEXT DEFAULT '{}',
                    error_msg   TEXT,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS brand_forecast_history (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id         INTEGER NOT NULL,
                    brand_name        TEXT NOT NULL,
                    predicted_revenue REAL DEFAULT 0,
                    actual_revenue    REAL,
                    growth_label      TEXT,
                    confidence        REAL DEFAULT 0,
                    accuracy_pct      REAL,
                    created_at        TEXT NOT NULL,
                    UNIQUE(report_id, brand_name),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS tool_execution_log (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    tool_name       TEXT NOT NULL,
                    arguments_json  TEXT DEFAULT '{}',
                    result_json     TEXT DEFAULT '{}',
                    status          TEXT DEFAULT 'success',
                    created_at      TEXT NOT NULL,
                    updated_at      TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS store_churn (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id     INTEGER NOT NULL,
                    brand_name    TEXT NOT NULL,
                    store_name    TEXT NOT NULL,
                    churn_type    TEXT NOT NULL,
                    prev_revenue  REAL DEFAULT 0,
                    created_at    TEXT NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS activity_batches (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_filename TEXT NOT NULL,
                    source_type    TEXT,
                    report_id      INTEGER,
                    start_date     TEXT,
                    end_date       TEXT,
                    row_count      INTEGER DEFAULT 0,
                    import_status  TEXT DEFAULT 'imported',
                    summary_json   TEXT DEFAULT '{}',
                    created_at     TEXT NOT NULL,
                    updated_at     TEXT NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS activity_events (
                    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id             INTEGER NOT NULL,
                    report_id            INTEGER,
                    activity_date        TEXT,
                    salesman_name        TEXT,
                    salesman_code        TEXT,
                    salesman_designation TEXT,
                    reporting_person_name TEXT,
                    survey_code          TEXT,
                    survey_name          TEXT,
                    survey_start_date    TEXT,
                    survey_end_date      TEXT,
                    retailer_code        TEXT,
                    retailer_name        TEXT,
                    retailer_type        TEXT,
                    retailer_state       TEXT,
                    retailer_district    TEXT,
                    retailer_city        TEXT,
                    question             TEXT,
                    answer_type          TEXT,
                    label                TEXT,
                    answer               TEXT,
                    created_at           TEXT NOT NULL,
                    FOREIGN KEY (batch_id) REFERENCES activity_batches(id),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS activity_visits (
                    id                INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id          INTEGER NOT NULL,
                    report_id         INTEGER,
                    visit_key         TEXT NOT NULL,
                    activity_date     TEXT,
                    salesman_name     TEXT,
                    survey_name       TEXT,
                    retailer_code     TEXT,
                    retailer_name     TEXT,
                    retailer_state    TEXT,
                    retailer_city     TEXT,
                    event_count       INTEGER DEFAULT 0,
                    issue_count       INTEGER DEFAULT 0,
                    opportunity_count INTEGER DEFAULT 0,
                    photo_count       INTEGER DEFAULT 0,
                    created_at        TEXT NOT NULL,
                    UNIQUE(batch_id, visit_key),
                    FOREIGN KEY (batch_id) REFERENCES activity_batches(id),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS activity_issues (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id      INTEGER NOT NULL,
                    report_id     INTEGER,
                    activity_date TEXT,
                    retailer_code TEXT,
                    retailer_name TEXT,
                    salesman_name TEXT,
                    brand_name    TEXT,
                    sku_name      TEXT,
                    issue_type    TEXT NOT NULL,
                    severity      TEXT DEFAULT 'medium',
                    question      TEXT,
                    label         TEXT,
                    answer        TEXT,
                    status        TEXT DEFAULT 'open',
                    created_at    TEXT NOT NULL,
                    FOREIGN KEY (batch_id) REFERENCES activity_batches(id),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS activity_brand_mentions (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id      INTEGER NOT NULL,
                    report_id     INTEGER,
                    brand_name    TEXT,
                    sku_name      TEXT,
                    retailer_code TEXT,
                    retailer_name TEXT,
                    activity_date TEXT,
                    source_kind   TEXT,
                    source_value  TEXT,
                    created_at    TEXT NOT NULL,
                    FOREIGN KEY (batch_id) REFERENCES activity_batches(id),
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS agent_actions (
                    id               INTEGER PRIMARY KEY AUTOINCREMENT,
                    agent_type       TEXT NOT NULL,
                    subject_type     TEXT NOT NULL,
                    subject_key      TEXT NOT NULL,
                    report_id        INTEGER,
                    priority         TEXT DEFAULT 'medium',
                    title            TEXT NOT NULL,
                    reason           TEXT,
                    proposed_payload TEXT DEFAULT '{}',
                    action_signature TEXT,
                    status           TEXT DEFAULT 'pending',
                    approved_by      TEXT,
                    approved_at      TEXT,
                    applied_at       TEXT,
                    created_at       TEXT NOT NULL,
                    updated_at       TEXT NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE TABLE IF NOT EXISTS agent_memories (
                    id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope_type    TEXT NOT NULL,
                    scope_key     TEXT NOT NULL,
                    memory_kind   TEXT DEFAULT 'note',
                    memory_text   TEXT NOT NULL,
                    confidence    REAL DEFAULT 0.5,
                    source        TEXT,
                    created_at    TEXT NOT NULL,
                    updated_at    TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS agent_feedback (
                    id             INTEGER PRIMARY KEY AUTOINCREMENT,
                    action_id      INTEGER NOT NULL,
                    feedback_type  TEXT NOT NULL,
                    original_status TEXT,
                    actor          TEXT,
                    note           TEXT,
                    created_at     TEXT NOT NULL,
                    FOREIGN KEY (action_id) REFERENCES agent_actions(id)
                );

                CREATE TABLE IF NOT EXISTS recommendation_outcomes (
                    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_name         TEXT NOT NULL,
                    report_id          INTEGER,
                    recommendation_key TEXT NOT NULL,
                    outcome_type       TEXT NOT NULL,
                    outcome_value      REAL,
                    note               TEXT,
                    created_at         TEXT NOT NULL,
                    FOREIGN KEY (report_id) REFERENCES reports(id)
                );

                CREATE INDEX IF NOT EXISTS idx_forecast_brand ON brand_forecast_history(brand_name);
                CREATE INDEX IF NOT EXISTS idx_churn_report ON store_churn(report_id, brand_name);
                CREATE INDEX IF NOT EXISTS idx_activity_batch_created ON activity_batches(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_activity_events_batch ON activity_events(batch_id, activity_date);
                CREATE INDEX IF NOT EXISTS idx_activity_events_report ON activity_events(report_id, activity_date);
                CREATE INDEX IF NOT EXISTS idx_activity_visits_batch ON activity_visits(batch_id, activity_date);
                CREATE INDEX IF NOT EXISTS idx_activity_issues_brand ON activity_issues(brand_name, issue_type, activity_date);
                CREATE INDEX IF NOT EXISTS idx_activity_store_code ON activity_events(retailer_code, activity_date);
                CREATE INDEX IF NOT EXISTS idx_agent_actions_status ON agent_actions(status, priority, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_agent_subject ON agent_actions(subject_type, subject_key);
                CREATE INDEX IF NOT EXISTS idx_agent_memories_scope ON agent_memories(scope_type, scope_key, updated_at DESC);
            """)
            # ── Schema migration: add report_type to existing databases ──────
            existing = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
            if 'report_type' not in existing:
                conn.execute("ALTER TABLE reports ADD COLUMN report_type TEXT DEFAULT 'monthly'")
            job_cols = [r[1] for r in conn.execute("PRAGMA table_info(generation_jobs)").fetchall()]
            if 'result_json' not in job_cols:
                conn.execute("ALTER TABLE generation_jobs ADD COLUMN result_json TEXT DEFAULT '{}'")
            memory_cols = [r[1] for r in conn.execute("PRAGMA table_info(agent_memories)").fetchall()]
            if 'memory_layer' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN memory_layer TEXT DEFAULT 'session'")
            if 'subject_type' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN subject_type TEXT")
            if 'subject_key' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN subject_key TEXT")
            if 'recency' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN recency REAL DEFAULT 0")
            if 'tags' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN tags TEXT DEFAULT '[]'")
            if 'related_report_id' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN related_report_id INTEGER")
            if 'related_brand' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN related_brand TEXT")
            if 'pinned' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN pinned INTEGER DEFAULT 0")
            if 'metadata_json' not in memory_cols:
                conn.execute("ALTER TABLE agent_memories ADD COLUMN metadata_json TEXT DEFAULT '{}'")
            conn.execute(
                """UPDATE agent_memories
                   SET subject_type=COALESCE(subject_type, scope_type),
                       subject_key=COALESCE(subject_key, scope_key),
                       memory_layer=COALESCE(memory_layer, 'session'),
                       recency=CASE
                           WHEN recency IS NULL OR recency=0 THEN strftime('%s', COALESCE(updated_at, created_at))
                           ELSE recency
                       END,
                       tags=COALESCE(tags, '[]'),
                       metadata_json=COALESCE(metadata_json, '{}'),
                       pinned=COALESCE(pinned, 0)"""
            )
            schedule_cols = [r[1] for r in conn.execute("PRAGMA table_info(scheduled_reports)").fetchall()]
            if 'job_type' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN job_type TEXT DEFAULT 'report'")
            if 'target' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN target TEXT")
            if 'payload_json' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN payload_json TEXT DEFAULT '{}'")
            if 'cadence' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN cadence TEXT")
            if 'next_run' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN next_run TEXT")
            if 'last_result' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN last_result TEXT DEFAULT '{}'")
            if 'status' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN status TEXT DEFAULT 'active'")
            if 'updated_at' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN updated_at TEXT")
            if 'connector' not in schedule_cols:
                conn.execute("ALTER TABLE scheduled_reports ADD COLUMN connector TEXT")
            conn.execute(
                """UPDATE scheduled_reports
                   SET cadence=COALESCE(cadence, cron_expr),
                       status=COALESCE(status, CASE WHEN active=1 THEN 'active' ELSE 'paused' END),
                       updated_at=COALESCE(updated_at, created_at),
                       payload_json=COALESCE(payload_json, '{}'),
                       last_result=COALESCE(last_result, '{}')"""
            )
            queue_cols = [r[1] for r in conn.execute("PRAGMA table_info(catalog_review_queue)").fetchall()]
            if 'suggested_match_name' not in queue_cols:
                conn.execute("ALTER TABLE catalog_review_queue ADD COLUMN suggested_match_name TEXT")
            if 'similarity_score' not in queue_cols:
                conn.execute("ALTER TABLE catalog_review_queue ADD COLUMN similarity_score REAL DEFAULT 0")
            try:
                conn.execute(
                    """CREATE VIRTUAL TABLE IF NOT EXISTS agent_memories_fts
                       USING fts5(scope_type, scope_key, memory_text, content='')"""
                )
            except sqlite3.OperationalError:
                pass

    # ── Report type helpers ───────────────────────────────────────────────────

    @staticmethod
    def _infer_report_type(start_date, end_date, override=None):
        """Auto-detect report period type from date range, or use explicit override."""
        if override and override in ('weekly', 'biweekly', 'monthly', 'quarterly', 'yearly'):
            return override
        from datetime import date as _date
        try:
            s = datetime.strptime(start_date, '%Y-%m-%d').date()
            e = datetime.strptime(end_date,   '%Y-%m-%d').date()
            days = (e - s).days + 1
            from datetime import timedelta as _td
            is_full_month = s.day == 1 and (e + _td(days=1)).day == 1
            is_full_year = s.month == 1 and s.day == 1 and e.month == 12 and e.day == 31
            is_quarter = (
                s.day == 1 and
                s.month in (1, 4, 7, 10) and
                e.month == s.month + 2 and
                ((e.month in (3, 12) and e.day == 31) or (e.month == 6 and e.day == 30) or (e.month == 9 and e.day == 30))
            )
            if is_full_year or days in (365, 366):
                return 'yearly'
            if is_quarter or 85 <= days <= 95:
                return 'quarterly'
            if is_full_month or 28 <= days <= 31:
                return 'monthly'
            if days <= 7:
                return 'weekly'
            if days <= 14:
                return 'biweekly'
            return 'custom'
        except Exception:
            return 'custom'

    @staticmethod
    def _build_month_label(start_date, end_date, report_type):
        """Build a human-readable label appropriate to the report period."""
        try:
            s = datetime.strptime(start_date, '%Y-%m-%d')
            e = datetime.strptime(end_date,   '%Y-%m-%d')
            if report_type == 'weekly':
                return f"Week of {s.strftime('%d %b %Y')}"
            if report_type == 'biweekly':
                return f"{s.strftime('%d %b')} – {e.strftime('%d %b %Y')}"
            if report_type == 'yearly':
                return f"{s.year}"
            if report_type == 'quarterly':
                return f"Q{((s.month - 1) // 3) + 1} {s.year}"
            if report_type == 'monthly':
                return s.strftime('%b %Y')
            return f"{s.strftime('%d %b %Y')} – {e.strftime('%d %b %Y')}"
        except Exception:
            return start_date

    # ── Report operations ─────────────────────────────────────────────────────

    def save_report(self, start_date, end_date, xls_filename,
                    total_revenue, total_qty, total_stores, brand_count,
                    report_type=None):
        """Create a report record. Returns the new report_id."""
        rt          = self._infer_report_type(start_date, end_date, report_type)
        month_label = self._build_month_label(start_date, end_date, rt)
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO reports
                   (month_label, start_date, end_date, xls_filename, report_type,
                    total_revenue, total_qty, total_stores, brand_count, generated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (month_label, start_date, end_date, xls_filename, rt,
                 total_revenue, total_qty, total_stores, brand_count, now)
            )
            return cur.lastrowid

    def get_all_reports(self):
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM reports ORDER BY start_date DESC"
            ).fetchall()]

    def get_report(self, report_id):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone()
            return dict(row) if row else None

    def get_latest_report(self):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM reports ORDER BY start_date DESC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def get_report_by_date_range(self, start_date: str, end_date: str):
        """Check if a report already exists for this date range."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM reports
                   WHERE start_date = ? AND end_date = ?
                   ORDER BY generated_at DESC LIMIT 1""",
                (start_date, end_date)
            ).fetchone()
            return dict(row) if row else None

    def clear_report_data(self, report_id):
        """Delete all brand_kpis, daily_sales, alerts, and ai_narratives for a report.
        Used when re-generating the same date range to avoid duplicates."""
        with self._connect() as conn:
            conn.execute("DELETE FROM alerts WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM brand_kpis WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM daily_sales WHERE report_id=?", (report_id,))
            conn.execute("DELETE FROM brand_detail_json WHERE report_id=?", (report_id,))
            try:
                conn.execute("DELETE FROM brand_forecast_history WHERE report_id=?", (report_id,))
            except Exception:
                pass
            try:
                conn.execute("DELETE FROM store_churn WHERE report_id=?", (report_id,))
            except Exception:
                pass
            try:
                conn.execute("DELETE FROM ai_narratives WHERE report_id=?", (report_id,))
            except Exception:
                pass  # table may not exist on older DBs

    def update_report(self, report_id, xls_filename, total_revenue, total_qty,
                      total_stores, brand_count, report_type=None, start_date=None, end_date=None):
        """Update an existing report row's stats after re-generation."""
        now = datetime.now().isoformat(timespec='seconds')
        rt = self._infer_report_type(start_date, end_date, report_type) if start_date and end_date else report_type
        month_label = self._build_month_label(start_date, end_date, rt) if start_date and end_date and rt else None
        with self._connect() as conn:
            if rt and month_label:
                conn.execute(
                    """UPDATE reports SET
                       xls_filename=?, total_revenue=?, total_qty=?, total_stores=?,
                       brand_count=?, generated_at=?, report_type=?, month_label=?
                       WHERE id=?""",
                    (xls_filename, total_revenue, total_qty, total_stores,
                     brand_count, now, rt, month_label, report_id)
                )
            else:
                conn.execute(
                    """UPDATE reports SET
                       xls_filename=?, total_revenue=?, total_qty=?, total_stores=?,
                       brand_count=?, generated_at=?
                       WHERE id=?""",
                    (xls_filename, total_revenue, total_qty, total_stores,
                     brand_count, now, report_id)
                )

    # ── Brand KPI operations ──────────────────────────────────────────────────

    def save_brand_kpis(self, report_id, brand_name, kpis,
                        perf_score_dict=None, portfolio_share_pct=0):
        ps = perf_score_dict or {}
        peak_date_str = None
        if kpis.get('peak_date') is not None:
            try:
                peak_date_str = str(kpis['peak_date'])[:10]
            except Exception:
                pass

        with self._connect() as conn:
            conn.execute(
                """INSERT INTO brand_kpis
                   (report_id, brand_name, total_revenue, total_qty, num_stores,
                    unique_skus, trading_days, repeat_stores, single_stores, repeat_pct,
                    avg_revenue_per_store, closing_stock_total, stock_days_cover,
                    inv_health_status, perf_grade, perf_score, perf_revenue_score,
                    perf_loyalty_score, perf_reach_score, perf_activity_score,
                    portfolio_share_pct, wow_rev_change, wow_qty_change,
                    peak_date, peak_revenue, top_store_name, top_store_revenue)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    report_id, brand_name,
                    kpis.get('total_revenue', 0), kpis.get('total_qty', 0),
                    kpis.get('num_stores', 0), kpis.get('unique_skus', 0),
                    kpis.get('trading_days', 0), kpis.get('repeat_stores', 0),
                    kpis.get('single_stores', 0), kpis.get('repeat_pct', 0),
                    kpis.get('avg_revenue_per_store', 0),
                    kpis.get('total_closing_stock', 0), kpis.get('stock_days_cover', 0),
                    kpis.get('inv_health_status', ''), ps.get('grade', ''),
                    ps.get('total', 0), ps.get('revenue_score', 0),
                    ps.get('loyalty_score', 0), ps.get('reach_score', 0),
                    ps.get('activity_score', 0), portfolio_share_pct,
                    kpis.get('wow_rev_change', 0), kpis.get('wow_qty_change', 0),
                    peak_date_str, kpis.get('peak_revenue', 0),
                    kpis.get('top_store_name', ''), kpis.get('top_store_revenue', 0),
                )
            )

    def save_daily_sales(self, report_id, brand_name, daily_sales_df):
        """Save daily_sales DataFrame rows for a brand."""
        rows = []
        for _, row in daily_sales_df.iterrows():
            rows.append((
                report_id, brand_name,
                str(row['Date'])[:10],
                float(row['Revenue']),
                float(row['Quantity']),
            ))
        if rows:
            with self._connect() as conn:
                conn.executemany(
                    "INSERT INTO daily_sales (report_id, brand_name, date, revenue, qty) VALUES (?,?,?,?,?)",
                    rows
                )

    def save_brand_detail_json(self, report_id, brand_name, kpis):
        """Persist the detailed DataFrames (stores, SKUs, inventory) as JSON."""
        def _df_json(df):
            if df is None or (hasattr(df, 'empty') and df.empty):
                return '[]'
            try:
                return df.to_json(orient='records')
            except Exception:
                return '[]'

        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO brand_detail_json
                   (report_id, brand_name, top_stores_json, product_value_json,
                    product_qty_json, closing_stock_json, pickup_json, supply_json,
                    reorder_json, heatmap_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    report_id, brand_name,
                    _df_json(kpis.get('top_stores')),
                    _df_json(kpis.get('product_value')),
                    _df_json(kpis.get('product_qty')),
                    _df_json(kpis.get('closing_stock')),
                    _df_json(kpis.get('pickup_summary')),
                    _df_json(kpis.get('supply_summary')),
                    _df_json(kpis.get('reorder_analysis')),
                    _df_json(kpis.get('store_heatmap_df')),
                )
            )

    def canonical_brand_name(self, brand_name):
        brand = self.resolve_brand_master(brand_name)
        if brand:
            return brand['canonical_name']
        return canonicalize_brand_name(brand_name)

    def analytics_brand_name(self, brand_name):
        canonical = self.canonical_brand_name(brand_name)
        compare_key = normalize_brand_compare_key(canonical)
        if not compare_key:
            return canonical

        candidates = []
        for brand in self.get_all_brand_master(status='all'):
            candidate_name = brand.get('canonical_name', '')
            if normalize_brand_compare_key(candidate_name) != compare_key:
                continue
            if self.is_catalog_distinct('brand', canonical, candidate_name):
                continue
            candidates.append(candidate_name)

        if not candidates:
            return canonical
        return min(set(candidates), key=lambda name: (len(normalize_name_key(name)), name.lower()))

    def _get_brand_family_names(self, brand_name):
        names = {
            str(brand_name or '').strip(),
            canonicalize_brand_name(brand_name),
        }
        target_canonical = canonicalize_brand_name(brand_name)
        target_compare = normalize_brand_compare_key(self.analytics_brand_name(brand_name))
        brand = self.resolve_brand_master(brand_name)
        if brand:
            names.update({
                brand.get('brand_name', ''),
                brand.get('canonical_name', ''),
            })
            for alias in self.get_brand_aliases(brand['id']):
                names.add(alias.get('alias_name', ''))
            target_canonical = brand.get('canonical_name') or target_canonical

        with self._connect() as conn:
            rows = conn.execute(
                """SELECT brand_name FROM brand_kpis
                   UNION
                   SELECT brand_name FROM daily_sales
                   UNION
                   SELECT brand_name FROM brand_detail_json"""
            ).fetchall()
        for row in rows:
            candidate = row['brand_name']
            if not candidate:
                continue
            candidate_canonical = canonicalize_brand_name(candidate)
            same_canonical = candidate_canonical == target_canonical
            same_compare = normalize_brand_compare_key(self.analytics_brand_name(candidate)) == target_compare
            if same_canonical or same_compare:
                names.add(candidate)
        return sorted(name for name in names if name)

    def _merge_brand_kpi_rows(self, rows, order_desc=False):
        merged = {}
        for row in rows or []:
            canonical = self.analytics_brand_name(row.get('brand_name'))
            report_key = row.get('report_id', row.get('start_date', ''))
            key = (canonical, report_key)
            entry = merged.setdefault(key, {
                **row,
                'brand_name': canonical,
                'total_revenue': 0.0,
                'total_qty': 0.0,
                'num_stores': 0,
                'repeat_stores': 0,
                'single_stores': 0,
                'closing_stock_total': 0.0,
                'portfolio_share_pct': 0.0,
                'top_store_revenue': 0.0,
                'peak_revenue': 0.0,
                'wow_rev_change_weight': 0.0,
                'wow_qty_change_weight': 0.0,
            })

            revenue = float(row.get('total_revenue', 0) or 0)
            qty = float(row.get('total_qty', 0) or 0)
            entry['total_revenue'] += revenue
            entry['total_qty'] += qty
            entry['num_stores'] += int(row.get('num_stores', 0) or 0)
            entry['repeat_stores'] += int(row.get('repeat_stores', 0) or 0)
            entry['single_stores'] += int(row.get('single_stores', 0) or 0)
            entry['closing_stock_total'] += float(row.get('closing_stock_total', 0) or 0)
            entry['portfolio_share_pct'] += float(row.get('portfolio_share_pct', 0) or 0)

            entry['unique_skus'] = max(int(entry.get('unique_skus', 0) or 0), int(row.get('unique_skus', 0) or 0))
            entry['trading_days'] = max(int(entry.get('trading_days', 0) or 0), int(row.get('trading_days', 0) or 0))
            current_cover = entry.get('stock_days_cover', 0) or 0
            row_cover = row.get('stock_days_cover', 0) or 0
            if current_cover == 0:
                entry['stock_days_cover'] = row_cover
            elif row_cover:
                entry['stock_days_cover'] = min(current_cover, row_cover)

            perf_score = int(row.get('perf_score', 0) or 0)
            if perf_score >= int(entry.get('perf_score', 0) or 0):
                for field in (
                    'perf_grade', 'perf_score', 'perf_revenue_score',
                    'perf_loyalty_score', 'perf_reach_score',
                    'perf_activity_score', 'inv_health_status',
                ):
                    entry[field] = row.get(field)

            if float(row.get('top_store_revenue', 0) or 0) >= float(entry.get('top_store_revenue', 0) or 0):
                entry['top_store_name'] = row.get('top_store_name')
                entry['top_store_revenue'] = float(row.get('top_store_revenue', 0) or 0)

            if float(row.get('peak_revenue', 0) or 0) >= float(entry.get('peak_revenue', 0) or 0):
                entry['peak_date'] = row.get('peak_date')
                entry['peak_revenue'] = float(row.get('peak_revenue', 0) or 0)

            entry['wow_rev_change_weight'] += float(row.get('wow_rev_change', 0) or 0) * max(revenue, 1)
            entry['wow_qty_change_weight'] += float(row.get('wow_qty_change', 0) or 0) * max(qty, 1)

        results = []
        for entry in merged.values():
            stores = int(entry.get('num_stores', 0) or 0)
            qty = float(entry.get('total_qty', 0) or 0)
            revenue = float(entry.get('total_revenue', 0) or 0)
            entry['avg_revenue_per_store'] = (revenue / stores) if stores else 0.0
            entry['repeat_pct'] = ((float(entry.get('repeat_stores', 0) or 0) / stores) * 100) if stores else 0.0
            entry['wow_rev_change'] = (
                entry.pop('wow_rev_change_weight', 0.0) / max(revenue, 1)
                if revenue else 0.0
            )
            entry['wow_qty_change'] = (
                entry.pop('wow_qty_change_weight', 0.0) / max(qty, 1)
                if qty else 0.0
            )
            results.append(entry)

        results.sort(key=lambda row: (row.get('start_date') or '', row.get('brand_name') or ''), reverse=order_desc)
        return results

    def get_brand_detail_json(self, report_id, brand_name):
        """Return merged detail JSON for a canonical brand, or None."""
        brand_names = self._get_brand_family_names(brand_name)
        placeholders = ','.join('?' for _ in brand_names)
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT * FROM brand_detail_json
                    WHERE report_id=? AND brand_name IN ({placeholders})""",
                [report_id, *brand_names]
            ).fetchall()

        if not rows:
            return None
        if len(rows) == 1:
            return dict(rows[0])

        json_fields = (
            'top_stores_json', 'product_value_json', 'product_qty_json',
            'closing_stock_json', 'pickup_json', 'supply_json',
            'reorder_json', 'heatmap_json',
        )
        merged = {
            'report_id': report_id,
            'brand_name': self.analytics_brand_name(brand_name),
        }
        for field in json_fields:
            records = []
            for row in rows:
                try:
                    records.extend(json.loads(row[field] or '[]'))
                except Exception:
                    continue
            merged[field] = json.dumps(records)
        return merged

    def get_brand_history(self, brand_name, limit=12, report_type=None):
        """Return canonical brand KPIs across reports, newest first."""
        brand_names = self._get_brand_family_names(brand_name)
        placeholders = ','.join('?' for _ in brand_names)
        with self._connect() as conn:
            params = list(brand_names)
            query = f"""SELECT bk.*, r.month_label, r.start_date, r.end_date, r.report_type
                        FROM brand_kpis bk
                        JOIN reports r ON bk.report_id = r.id
                        WHERE bk.brand_name IN ({placeholders})"""
            if report_type:
                query += " AND r.report_type=?"
                params.append(report_type)
            query += " ORDER BY r.start_date DESC"
            rows = conn.execute(query, params).fetchall()
        merged = self._merge_brand_kpi_rows([dict(r) for r in rows], order_desc=True)
        return merged[:limit]

    def get_all_brand_kpis(self, report_id):
        """Return canonical brand KPIs for a given report."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM brand_kpis WHERE report_id=? ORDER BY total_revenue DESC",
                (report_id,)
            ).fetchall()
        return self._merge_brand_kpi_rows([dict(r) for r in rows], order_desc=True)

    def get_brand_kpis_single(self, report_id, brand_name):
        target_name = self.analytics_brand_name(brand_name)
        for row in self.get_all_brand_kpis(report_id):
            if row.get('brand_name') == target_name:
                return row
        return None

    def get_daily_sales(self, report_id, brand_name=None):
        with self._connect() as conn:
            if brand_name:
                brand_names = self._get_brand_family_names(brand_name)
                placeholders = ','.join('?' for _ in brand_names)
                rows = conn.execute(
                    f"""SELECT * FROM daily_sales
                        WHERE report_id=? AND brand_name IN ({placeholders})
                        ORDER BY date""",
                    [report_id, *brand_names]
                ).fetchall()
                merged = {}
                for row in rows:
                    record = dict(row)
                    entry = merged.setdefault(record['date'], {
                        'report_id': report_id,
                        'brand_name': self.analytics_brand_name(brand_name),
                        'date': record['date'],
                        'revenue': 0.0,
                        'qty': 0.0,
                    })
                    entry['revenue'] += float(record.get('revenue', 0) or 0)
                    entry['qty'] += float(record.get('qty', 0) or 0)
                return [merged[key] for key in sorted(merged)]
            else:
                rows = conn.execute(
                    "SELECT * FROM daily_sales WHERE report_id=? ORDER BY date",
                    (report_id,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_all_brands_in_db(self):
        """Return sorted canonical list of all brand names ever stored."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT brand_name FROM brand_kpis ORDER BY brand_name"
            ).fetchall()
            brands = {self.analytics_brand_name(r['brand_name']) for r in rows if r['brand_name']}
            return sorted(brands)

    def get_report_by_month(self, year: int, month: int, report_type: str = None):
        """Return report row whose start_date falls in the given year+month, or None."""
        prefix = f"{year:04d}-{month:02d}"
        with self._connect() as conn:
            if report_type:
                row = conn.execute(
                    """SELECT * FROM reports
                       WHERE start_date LIKE ? AND report_type=?
                       ORDER BY start_date DESC LIMIT 1""",
                    (prefix + '%', report_type)
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT * FROM reports WHERE start_date LIKE ? ORDER BY start_date DESC LIMIT 1",
                    (prefix + '%',)
                ).fetchone()
            return dict(row) if row else None

    def find_report_covering_range(self, start_date: str, end_date: str):
        """Return the nearest report overlapping the given activity date range, or None."""
        with self._connect() as conn:
            row = conn.execute(
                """SELECT *,
                          CASE
                            WHEN start_date <= ? AND end_date >= ? THEN 0
                            ELSE 1
                          END AS exact_cover
                   FROM reports
                   WHERE start_date <= ? AND end_date >= ?
                   ORDER BY exact_cover ASC, start_date DESC
                   LIMIT 1""",
                (start_date, end_date, end_date, start_date)
            ).fetchone()
            return dict(row) if row else None

    def get_yoy_kpis(self, report_id: int):
        """
        Return a dict {brand_name: brand_kpis_row} for the same calendar month
        one year prior to the given report_id.
        Returns {} if no matching report OR if report types differ (e.g. weekly vs monthly).
        """
        report = self.get_report(report_id)
        if not report:
            return {}
        try:
            from datetime import date as _date
            d = _date.fromisoformat(report['start_date'])
            prev_year = d.year - 1
            prev_report = self.get_report_by_month(prev_year, d.month, report.get('report_type'))
        except Exception:
            return {}
        if not prev_report:
            return {}
        # Only compare reports of the same type to avoid week vs month distortion
        if prev_report.get('report_type') != report.get('report_type'):
            return {}
        rows = self.get_all_brand_kpis(prev_report['id'])
        return {r['brand_name']: r for r in rows}

    def get_portfolio_yoy(self, report_id: int):
        """
        Return (prev_report, prev_total_revenue, prev_total_qty, prev_total_stores)
        for the same calendar month last year.  Returns (None, 0, 0, 0) if unavailable
        or if report types differ (e.g. weekly vs monthly).
        """
        report = self.get_report(report_id)
        if not report:
            return None, 0, 0, 0
        try:
            from datetime import date as _date
            d = _date.fromisoformat(report['start_date'])
            prev_report = self.get_report_by_month(d.year - 1, d.month, report.get('report_type'))
        except Exception:
            return None, 0, 0, 0
        if not prev_report:
            return None, 0, 0, 0
        # Only compare same report type to avoid week vs month distortion
        if prev_report.get('report_type') != report.get('report_type'):
            return None, 0, 0, 0
        return (
            prev_report,
            prev_report.get('total_revenue', 0),
            prev_report.get('total_qty', 0),
            prev_report.get('total_stores', 0),
        )

    def get_data_quality_score(self, report_id: int):
        """Return stored data quality score for a report, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT detail FROM activity_log WHERE action='quality_score' AND report_id=? ORDER BY created_at DESC LIMIT 1",
                (report_id,)
            ).fetchone()
            if row:
                try:
                    return float(row['detail'])
                except Exception:
                    return None
            return None

    def save_data_quality_score(self, report_id: int, score: float):
        """Persist data quality score in activity_log."""
        self.log_activity('quality_score', detail=str(round(score, 1)), report_id=report_id)

    def save_forecast_result(self, report_id: int, brand_name: str, predicted_revenue: float,
                              growth_label: str, confidence: float):
        """Store a forecast prediction for later accuracy comparison."""
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO brand_forecast_history
                   (report_id, brand_name, predicted_revenue, growth_label, confidence, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (report_id, brand_name, predicted_revenue, growth_label, confidence, now)
            )

    def get_forecast_accuracy(self, brand_name: str):
        """
        Compare stored predictions against actuals.
        Returns list of {month_label, predicted, actual, error_pct}.
        """
        with self._connect() as conn:
            # For each forecast, look up the actual from the NEXT month's brand_kpis
            rows = conn.execute(
                """SELECT bfh.*, r.month_label, r.start_date
                   FROM brand_forecast_history bfh
                   JOIN reports r ON bfh.report_id = r.id
                   WHERE bfh.brand_name=? ORDER BY r.start_date DESC LIMIT 12""",
                (brand_name,)
            ).fetchall()
        results = []
        for row in rows:
            row = dict(row)
            try:
                from datetime import date as _date, timedelta
                d = _date.fromisoformat(row['start_date'])
                # The "next" month's actual is the report one month later
                next_month = d.month % 12 + 1
                next_year = d.year + (1 if d.month == 12 else 0)
                next_report = self.get_report_by_month(next_year, next_month)
                if next_report:
                    actual_row = self.get_brand_kpis_single(next_report['id'], brand_name)
                    if actual_row:
                        actual = actual_row['total_revenue']
                        pred = row['predicted_revenue']
                        if pred and pred > 0:
                            error = round(abs(actual - pred) / pred * 100, 1)
                            accuracy = round(100 - error, 1)
                        else:
                            accuracy = None
                        results.append({
                            'month_label': row['month_label'],
                            'predicted': pred,
                            'actual': actual,
                            'accuracy_pct': accuracy,
                        })
            except Exception:
                pass
        return results

    # ── Store churn operations ────────────────────────────────────────────────

    def save_store_churn(self, report_id: int, brand_name: str, churn_data: dict):
        """Persist churn data for a brand into the store_churn table."""
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute("DELETE FROM store_churn WHERE report_id=? AND brand_name=?", (report_id, brand_name))
            for entry in churn_data.get('churned_stores', []):
                conn.execute(
                    "INSERT INTO store_churn (report_id, brand_name, store_name, churn_type, prev_revenue, created_at) VALUES (?,?,?,?,?,?)",
                    (report_id, brand_name, entry['store'], 'churned', entry.get('prev_revenue', 0), now)
                )
            for entry in churn_data.get('new_stores', []):
                conn.execute(
                    "INSERT INTO store_churn (report_id, brand_name, store_name, churn_type, prev_revenue, created_at) VALUES (?,?,?,?,?,?)",
                    (report_id, brand_name, entry['store'], 'new', entry.get('curr_revenue', 0), now)
                )

    def get_store_churn(self, report_id: int, brand_name: str = None):
        """Return churn rows for a report, optionally filtered by brand."""
        with self._connect() as conn:
            if brand_name:
                rows = conn.execute(
                    "SELECT * FROM store_churn WHERE report_id=? AND brand_name=? ORDER BY churn_type, store_name",
                    (report_id, brand_name)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM store_churn WHERE report_id=? ORDER BY brand_name, churn_type, store_name",
                    (report_id,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_churn_summary(self, report_id: int):
        """Return aggregated churn count per brand: {brand_name: {churned, new}}."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT brand_name, churn_type, COUNT(*) as cnt FROM store_churn WHERE report_id=? GROUP BY brand_name, churn_type",
                (report_id,)
            ).fetchall()
        result = {}
        for r in rows:
            bn = r['brand_name']
            if bn not in result:
                result[bn] = {'churned': 0, 'new': 0}
            result[bn][r['churn_type']] = r['cnt']
        return result

    # ── Alert operations ──────────────────────────────────────────────────────

    def save_alert(self, report_id, brand_name, alert_type, severity, message):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO alerts (report_id, brand_name, alert_type, severity, message, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (report_id, brand_name, alert_type, severity, message, now)
            )

    def get_alerts(self, report_id=None, unacknowledged_only=False):
        query = "SELECT a.*, r.month_label FROM alerts a JOIN reports r ON a.report_id=r.id"
        params = []
        where = []
        if report_id:
            where.append("a.report_id=?")
            params.append(report_id)
        if unacknowledged_only:
            where.append("a.acknowledged=0")
        if where:
            query += " WHERE " + " AND ".join(where)
        query += " ORDER BY a.created_at DESC"
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(query, params).fetchall()]

    def acknowledge_alert(self, alert_id):
        with self._connect() as conn:
            conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (alert_id,))

    def get_unacknowledged_count(self):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE acknowledged=0"
            ).fetchone()
            return row['cnt'] if row else 0

    # ── Brand token operations ────────────────────────────────────────────────

    def get_or_create_token(self, brand_name):
        """Return existing token or generate a new one for the brand."""
        self.ensure_brand_master(brand_name)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT token FROM brand_tokens WHERE brand_name=?", (brand_name,)
            ).fetchone()
            if row:
                return row['token']
            token = uuid.uuid4().hex
            now = datetime.now().isoformat(timespec='seconds')
            conn.execute(
                "INSERT INTO brand_tokens (brand_name, token, active, created_at) VALUES (?,?,1,?)",
                (brand_name, token, now)
            )
            return token

    def get_brand_by_token(self, token):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM brand_tokens WHERE token=? AND active=1", (token,)
            ).fetchone()
            return dict(row) if row else None

    def get_all_tokens(self):
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM brand_tokens ORDER BY brand_name"
            ).fetchall()]

    def update_brand_contact(self, brand_name, email=None, whatsapp=None):
        with self._connect() as conn:
            conn.execute(
                "UPDATE brand_tokens SET email=?, whatsapp=? WHERE brand_name=?",
                (email, whatsapp, brand_name)
            )
            brand = conn.execute(
                "SELECT id FROM brands_master WHERE canonical_key=?",
                (normalize_name_key(canonicalize_brand_name(brand_name)),)
            ).fetchone()
            if brand:
                conn.execute(
                    """UPDATE brands_master
                       SET default_email=?, default_whatsapp=?, updated_at=?
                       WHERE id=?""",
                    (email, whatsapp, datetime.now().isoformat(timespec='seconds'), brand['id'])
                )

    def revoke_token(self, brand_name):
        with self._connect() as conn:
            conn.execute(
                "UPDATE brand_tokens SET active=0 WHERE brand_name=?", (brand_name,)
            )

    def regenerate_token(self, brand_name):
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute(
                "UPDATE brand_tokens SET token=?, active=1 WHERE brand_name=?",
                (token, brand_name)
            )
        return token

    # ── Catalog master data ──────────────────────────────────────────────────

    def ensure_brand_master(self, brand_name, status='active', category=None,
                            start_date=None, email=None, whatsapp=None, notes=None):
        canonical_name = canonicalize_brand_name(brand_name)
        canonical_key = normalize_name_key(canonical_name)
        if not canonical_key:
            return None

        now = datetime.now().isoformat(timespec='seconds')
        slug_base = self._slugify(canonical_name)

        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM brands_master WHERE canonical_key=?",
                (canonical_key,)
            ).fetchone()
            if row:
                updates = []
                params = []
                if email is not None:
                    updates.append("default_email=?")
                    params.append(email)
                if whatsapp is not None:
                    updates.append("default_whatsapp=?")
                    params.append(whatsapp)
                if category:
                    updates.append("category=COALESCE(category, ?)")
                    params.append(category)
                if start_date:
                    updates.append("start_date=COALESCE(start_date, ?)")
                    params.append(start_date)
                if notes:
                    updates.append("notes=COALESCE(notes, ?)")
                    params.append(notes)
                if updates:
                    params.extend([now, row['id']])
                    conn.execute(
                        f"UPDATE brands_master SET {', '.join(updates)}, updated_at=? WHERE id=?",
                        params
                    )
                    row = conn.execute("SELECT * FROM brands_master WHERE id=?", (row['id'],)).fetchone()
                return dict(row)

            slug = slug_base or f"brand-{uuid.uuid4().hex[:8]}"
            suffix = 2
            while conn.execute("SELECT 1 FROM brands_master WHERE slug=?", (slug,)).fetchone():
                slug = f"{slug_base}-{suffix}"
                suffix += 1

            cur = conn.execute(
                """INSERT INTO brands_master
                   (brand_name, canonical_name, canonical_key, slug, status, category,
                    start_date, default_email, default_whatsapp, notes, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (canonical_name, canonical_name, canonical_key, slug, status, category,
                 start_date, email, whatsapp, notes, now, now)
            )
            brand_id = cur.lastrowid
            conn.execute(
                """INSERT OR IGNORE INTO brand_aliases
                   (brand_id, alias_name, alias_key, created_at)
                   VALUES (?,?,?,?)""",
                (brand_id, canonical_name, canonical_key, now)
            )
            return dict(conn.execute("SELECT * FROM brands_master WHERE id=?", (brand_id,)).fetchone())

    def add_brand_alias(self, brand_id, alias_name):
        alias_name = str(alias_name or '').strip()
        alias_key = normalize_name_key(alias_name)
        if not alias_key:
            return
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO brand_aliases
                   (brand_id, alias_name, alias_key, created_at)
                   VALUES (?,?,?,?)""",
                (brand_id, alias_name, alias_key, now)
            )

    def resolve_brand_master(self, brand_name):
        alias_key = normalize_name_key(canonicalize_brand_name(brand_name))
        if not alias_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """SELECT bm.*
                   FROM brands_master bm
                   LEFT JOIN brand_aliases ba ON ba.brand_id = bm.id
                   WHERE bm.canonical_key=? OR ba.alias_key=?
                   ORDER BY bm.id
                   LIMIT 1""",
                (alias_key, alias_key)
            ).fetchone()
            return dict(row) if row else None

    def get_brand_master(self, brand_id):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM brands_master WHERE id=?", (brand_id,)).fetchone()
            return dict(row) if row else None

    def get_brand_master_by_slug(self, slug):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM brands_master WHERE slug=?", (slug,)).fetchone()
            return dict(row) if row else None

    def get_brand_aliases(self, brand_id):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM brand_aliases WHERE brand_id=? ORDER BY alias_name",
                (brand_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def update_brand_master(self, brand_id, **fields):
        allowed = {
            'brand_name', 'canonical_name', 'status', 'category',
            'start_date', 'default_email', 'default_whatsapp', 'notes'
        }
        updates = {k: v for k, v in fields.items() if k in allowed}
        if not updates:
            return self.get_brand_master(brand_id)

        if 'brand_name' in updates and 'canonical_name' not in updates:
            updates['canonical_name'] = canonicalize_brand_name(updates['brand_name'])
        if 'canonical_name' in updates:
            updates['canonical_name'] = canonicalize_brand_name(updates['canonical_name'])
            updates['canonical_key'] = normalize_name_key(updates['canonical_name'])
            updates['slug'] = self._slugify(updates['canonical_name'])

        updates['updated_at'] = datetime.now().isoformat(timespec='seconds')
        cols = ', '.join(f"{k}=?" for k in updates)
        params = list(updates.values()) + [brand_id]

        with self._connect() as conn:
            conn.execute(f"UPDATE brands_master SET {cols} WHERE id=?", params)
        return self.get_brand_master(brand_id)

    def get_all_brand_master(self, status=None):
        query = "SELECT * FROM brands_master"
        params = []
        if status and status != 'all':
            query += " WHERE status=?"
            params.append(status)
        query += " ORDER BY canonical_name"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def ensure_sku_master(self, brand_id, sku_name, sku_code=None, pack_size=None,
                          unit_type=None, status='active', launch_date=None, notes=None):
        display_name = str(sku_name or '').strip()
        canonical_key = normalize_name_key(display_name)
        if not brand_id or not canonical_key:
            return None

        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM skus_master WHERE brand_id=? AND canonical_key=?",
                (brand_id, canonical_key)
            ).fetchone()
            if row:
                return dict(row)

            cur = conn.execute(
                """INSERT INTO skus_master
                   (brand_id, sku_name, canonical_sku_name, canonical_key, sku_code,
                    pack_size, unit_type, status, launch_date, notes, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (brand_id, display_name, display_name, canonical_key, sku_code,
                 pack_size, unit_type, status, launch_date, notes, now, now)
            )
            sku_id = cur.lastrowid
            conn.execute(
                """INSERT OR IGNORE INTO sku_aliases
                   (sku_id, brand_id, alias_name, alias_key, created_at)
                   VALUES (?,?,?,?,?)""",
                (sku_id, brand_id, display_name, canonical_key, now)
            )
            return dict(conn.execute("SELECT * FROM skus_master WHERE id=?", (sku_id,)).fetchone())

    def add_sku_alias(self, sku_id, brand_id, alias_name):
        alias_name = str(alias_name or '').strip()
        alias_key = normalize_name_key(alias_name)
        if not alias_key:
            return
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO sku_aliases
                   (sku_id, brand_id, alias_name, alias_key, created_at)
                   VALUES (?,?,?,?,?)""",
                (sku_id, brand_id, alias_name, alias_key, now)
            )

    def resolve_sku_master(self, brand_id, sku_name):
        alias_key = normalize_name_key(sku_name)
        if not brand_id or not alias_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """SELECT sm.*
                   FROM skus_master sm
                   LEFT JOIN sku_aliases sa ON sa.sku_id = sm.id
                   WHERE sm.brand_id=?
                     AND (sm.canonical_key=? OR sa.alias_key=?)
                   LIMIT 1""",
                (brand_id, alias_key, alias_key)
            ).fetchone()
            return dict(row) if row else None

    def get_brand_skus(self, brand_id, status=None):
        query = "SELECT * FROM skus_master WHERE brand_id=?"
        params = [brand_id]
        if status and status != 'all':
            query += " AND status=?"
            params.append(status)
        query += " ORDER BY sku_name"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_sku_master(self, sku_id):
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM skus_master WHERE id=?", (sku_id,)).fetchone()
            return dict(row) if row else None

    def get_sku_aliases(self, brand_id):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM sku_aliases WHERE brand_id=? ORDER BY alias_name",
                (brand_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_catalog_distinct(self, entity_type, left_name, right_name, brand_scope=None, note=None):
        left_key = normalize_name_key(left_name)
        right_key = normalize_name_key(right_name)
        if entity_type not in ('brand', 'sku') or not left_key or not right_key:
            return
        left_key, right_key = self._ordered_pair(left_key, right_key)
        brand_scope_key = normalize_name_key(brand_scope) if entity_type == 'sku' and brand_scope else ''
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO catalog_distinct_rules
                   (entity_type, left_key, right_key, brand_scope_key, note, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (entity_type, left_key, right_key, brand_scope_key, note, now)
            )

    def is_catalog_distinct(self, entity_type, left_name, right_name, brand_scope=None):
        left_key = normalize_name_key(left_name)
        right_key = normalize_name_key(right_name)
        if entity_type not in ('brand', 'sku') or not left_key or not right_key:
            return False
        left_key, right_key = self._ordered_pair(left_key, right_key)
        brand_scope_key = normalize_name_key(brand_scope) if entity_type == 'sku' and brand_scope else ''
        with self._connect() as conn:
            row = conn.execute(
                """SELECT 1 FROM catalog_distinct_rules
                   WHERE entity_type=? AND left_key=? AND right_key=? AND brand_scope_key=?""",
                (entity_type, left_key, right_key, brand_scope_key)
            ).fetchone()
            return bool(row)

    def find_brand_duplicate_candidate(self, brand_name, threshold=0.82):
        best = None
        for brand in self.get_all_brand_master(status='all'):
            if self.is_catalog_distinct('brand', brand_name, brand['canonical_name']):
                continue
            score = self._brand_similarity_score(brand_name, brand['canonical_name'])
            if score >= threshold and (best is None or score > best['score']):
                best = {'brand': brand, 'score': score}
        return best

    def find_sku_duplicate_candidate(self, brand_id, sku_name, threshold=0.84):
        brand = self.get_brand_master(brand_id)
        if not brand:
            return None
        brand_scope = brand['canonical_name']
        best = None
        for sku in self.get_brand_skus(brand_id, status='all'):
            if self.is_catalog_distinct('sku', sku_name, sku['sku_name'], brand_scope=brand_scope):
                continue
            score = self._sku_similarity_score(sku_name, sku['sku_name'])
            if score >= threshold and (best is None or score > best['score']):
                best = {'sku': sku, 'score': score, 'brand': brand}
        return best

    def queue_catalog_candidate(self, entity_type, raw_name, canonical_candidate=None,
                                brand_candidate=None, source_report_id=None,
                                source_filename=None, reason='new_detected',
                                suggested_match_name=None, similarity_score=0.0):
        raw_name = str(raw_name or '').strip()
        normalized_name = normalize_name_key(raw_name)
        if entity_type not in ('brand', 'sku') or not normalized_name:
            return None

        canonical_candidate = canonical_candidate or raw_name
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            existing = conn.execute(
                """SELECT id FROM catalog_review_queue
                   WHERE entity_type=? AND normalized_name=?
                     AND COALESCE(brand_candidate, '')=COALESCE(?, '')
                     AND status='pending'
                   ORDER BY id DESC LIMIT 1""",
                (entity_type, normalized_name, brand_candidate)
            ).fetchone()
            if existing:
                return None

            cur = conn.execute(
                """INSERT INTO catalog_review_queue
                   (entity_type, raw_name, normalized_name, canonical_candidate, brand_candidate,
                    suggested_match_name, similarity_score, source_report_id, source_filename,
                    reason, status, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (entity_type, raw_name, normalized_name, canonical_candidate, brand_candidate,
                 suggested_match_name, similarity_score, source_report_id, source_filename,
                 reason, 'pending', now)
            )
            return cur.lastrowid

    def get_catalog_review_queue(self, status='pending', limit=200):
        query = "SELECT * FROM catalog_review_queue"
        params = []
        if status and status != 'all':
            query += " WHERE status=?"
            params.append(status)
        query += """ ORDER BY
                     CASE status WHEN 'pending' THEN 0 ELSE 1 END,
                     CASE reason WHEN 'possible_duplicate' THEN 0 ELSE 1 END,
                     similarity_score DESC,
                     id DESC
                     LIMIT ?"""
        params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def get_catalog_review_item(self, item_id):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM catalog_review_queue WHERE id=?",
                (item_id,)
            ).fetchone()
            return dict(row) if row else None

    def update_catalog_review_status(self, item_id, status, review_note=None):
        reviewed_at = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """UPDATE catalog_review_queue
                   SET status=?, review_note=?, reviewed_at=?
                   WHERE id=?""",
                (status, review_note, reviewed_at, item_id)
            )

    def get_catalog_summary(self):
        with self._connect() as conn:
            summary = {
                'brands_total': conn.execute("SELECT COUNT(*) FROM brands_master").fetchone()[0],
                'brands_active': conn.execute("SELECT COUNT(*) FROM brands_master WHERE status='active'").fetchone()[0],
                'skus_total': conn.execute("SELECT COUNT(*) FROM skus_master").fetchone()[0],
                'pending_reviews': conn.execute("SELECT COUNT(*) FROM catalog_review_queue WHERE status='pending'").fetchone()[0],
                'pending_brand_reviews': conn.execute("SELECT COUNT(*) FROM catalog_review_queue WHERE status='pending' AND entity_type='brand'").fetchone()[0],
                'pending_sku_reviews': conn.execute("SELECT COUNT(*) FROM catalog_review_queue WHERE status='pending' AND entity_type='sku'").fetchone()[0],
                'pending_duplicate_reviews': conn.execute("SELECT COUNT(*) FROM catalog_review_queue WHERE status='pending' AND reason='possible_duplicate'").fetchone()[0],
            }
        return summary

    def sync_catalog_from_history(self):
        """Backfill catalog tables from existing reports, detail JSON, and portal contacts."""
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            brand_rows = conn.execute(
                """SELECT DISTINCT bk.brand_name, bt.email, bt.whatsapp
                   FROM brand_kpis bk
                   LEFT JOIN brand_tokens bt ON bt.brand_name = bk.brand_name
                   ORDER BY bk.brand_name"""
            ).fetchall()
            for row in brand_rows:
                brand = self.ensure_brand_master(
                    row['brand_name'],
                    email=row['email'],
                    whatsapp=row['whatsapp'],
                )
                if not brand:
                    continue

            detail_rows = conn.execute(
                "SELECT brand_name, product_value_json, product_qty_json FROM brand_detail_json"
            ).fetchall()

            for row in detail_rows:
                brand = self.resolve_brand_master(row['brand_name'])
                if not brand:
                    continue
                sku_names = set()
                for payload in (row['product_value_json'], row['product_qty_json']):
                    try:
                        records = json.loads(payload or '[]')
                    except Exception:
                        records = []
                    for record in records:
                        sku = (
                            record.get('SKU') or record.get('sku') or
                            record.get('Product') or record.get('name') or ''
                        )
                        sku = str(sku).strip()
                        if sku:
                            sku_names.add(sku)
                for sku_name in sku_names:
                    self.ensure_sku_master(brand['id'], sku_name)

            # Keep brand master contacts aligned with the token table.
            token_rows = conn.execute("SELECT brand_name, email, whatsapp FROM brand_tokens").fetchall()
            for row in token_rows:
                brand = self.resolve_brand_master(row['brand_name'])
                if brand:
                    conn.execute(
                        """UPDATE brands_master
                           SET default_email=COALESCE(default_email, ?),
                               default_whatsapp=COALESCE(default_whatsapp, ?),
                               updated_at=?
                           WHERE id=?""",
                        (row['email'], row['whatsapp'], now, brand['id'])
                    )

    def register_catalog_candidates(self, df, source_filename=None, source_report_id=None):
        """Queue unknown brands and SKUs discovered during import or generation."""
        if df is None or getattr(df, 'empty', True):
            return {'queued_brands': 0, 'queued_skus': 0}

        if 'Brand Partner' not in df.columns or 'SKUs' not in df.columns:
            return {'queued_brands': 0, 'queued_skus': 0}

        sales_df = df
        try:
            sales_df = df[df['Vch Type'] == 'Sales'].copy()
        except Exception:
            pass

        queued_brands = 0
        queued_skus = 0
        seen_brand_keys = set()
        seen_sku_keys = set()

        brand_series = sales_df['Brand Partner'].dropna().astype(str).str.strip()
        for brand_name in sorted(brand_series.unique().tolist()):
            canonical_brand = canonicalize_brand_name(brand_name)
            brand_key = normalize_name_key(canonical_brand)
            if not brand_key or brand_key in seen_brand_keys:
                continue
            seen_brand_keys.add(brand_key)

            brand = self.resolve_brand_master(brand_name)
            if not brand:
                suggested_brand = self.find_brand_duplicate_candidate(canonical_brand)
                reason = 'possible_duplicate' if suggested_brand else 'new_detected'
                queue_id = self.queue_catalog_candidate(
                    entity_type='brand',
                    raw_name=brand_name,
                    canonical_candidate=canonical_brand,
                    brand_candidate=suggested_brand['brand']['canonical_name'] if suggested_brand else None,
                    source_report_id=source_report_id,
                    source_filename=source_filename,
                    reason=reason,
                    suggested_match_name=suggested_brand['brand']['canonical_name'] if suggested_brand else None,
                    similarity_score=suggested_brand['score'] if suggested_brand else 0.0,
                )
                if queue_id:
                    queued_brands += 1

            sku_series = sales_df.loc[sales_df['Brand Partner'] == brand_name, 'SKUs']
            for sku_name in sorted(sku_series.dropna().astype(str).str.strip().unique().tolist()):
                sku_norm = normalize_name_key(sku_name)
                sku_scope = str(brand['id']) if brand else canonical_brand
                sku_key = f"{sku_scope}::{sku_norm}"
                if not sku_norm or sku_key in seen_sku_keys:
                    continue
                seen_sku_keys.add(sku_key)
                if brand and self.resolve_sku_master(brand['id'], sku_name):
                    continue
                suggested_sku = self.find_sku_duplicate_candidate(brand['id'], sku_name) if brand else None
                reason = 'possible_duplicate' if suggested_sku else 'new_detected'
                queue_id = self.queue_catalog_candidate(
                    entity_type='sku',
                    raw_name=sku_name,
                    canonical_candidate=sku_name,
                    brand_candidate=brand['canonical_name'] if brand else canonical_brand,
                    source_report_id=source_report_id,
                    source_filename=source_filename,
                    reason=reason,
                    suggested_match_name=suggested_sku['sku']['sku_name'] if suggested_sku else None,
                    similarity_score=suggested_sku['score'] if suggested_sku else 0.0,
                )
                if queue_id:
                    queued_skus += 1

        return {'queued_brands': queued_brands, 'queued_skus': queued_skus}

    # ── Analytics / cross-report queries ─────────────────────────────────────

    def get_brand_revenue_trend(self, brand_name, limit=12):
        """Returns list of {month_label, total_revenue, perf_grade} newest first."""
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                """SELECT r.month_label, r.start_date, bk.total_revenue, bk.perf_grade, bk.perf_score
                   FROM brand_kpis bk JOIN reports r ON bk.report_id=r.id
                   WHERE bk.brand_name=? ORDER BY r.start_date DESC LIMIT ?""",
                (brand_name, limit)
            ).fetchall()]

    def get_all_brands_revenue_trends(self, limit=6):
        """Batch version — returns {brand_name: [trend_rows]} for ALL brands in ONE query.
        Replaces calling get_brand_revenue_trend() in a per-brand loop (N+1 problem)."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT bk.brand_name, r.month_label, r.start_date,
                          bk.total_revenue, bk.perf_grade, bk.perf_score,
                          ROW_NUMBER() OVER (
                              PARTITION BY bk.brand_name ORDER BY r.start_date DESC
                          ) AS rn
                   FROM brand_kpis bk JOIN reports r ON bk.report_id=r.id
                   ORDER BY bk.brand_name, r.start_date DESC"""
            ).fetchall()
        result = {}
        for r in rows:
            r = dict(r)
            if r['rn'] > limit:
                continue
            result.setdefault(r['brand_name'], []).append(r)
        return result

    def get_portfolio_monthly_trend(self, limit=12):
        """Returns monthly portfolio totals."""
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                """SELECT month_label, start_date, total_revenue, total_qty,
                          total_stores, brand_count
                   FROM reports ORDER BY start_date DESC LIMIT ?""",
                (limit,)
            ).fetchall()]

    def compare_brands(self, brand_a, brand_b, report_id):
        """Returns KPIs for both brands in same report for comparison."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT bk.*, r.month_label FROM brand_kpis bk JOIN reports r ON bk.report_id=r.id
                   WHERE bk.report_id=? AND bk.brand_name IN (?,?)""",
                (report_id, brand_a, brand_b)
            ).fetchall()
            return {r['brand_name']: dict(r) for r in rows}

    # ── Target operations ─────────────────────────────────────────────────────

    def set_target(self, brand_name, month_label, target_revenue=0,
                   target_stores=0, target_repeat_pct=0, set_by='manual'):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO brand_targets
                   (brand_name, month_label, target_revenue, target_stores,
                    target_repeat_pct, set_by, created_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(brand_name, month_label) DO UPDATE SET
                     target_revenue=excluded.target_revenue,
                     target_stores=excluded.target_stores,
                     target_repeat_pct=excluded.target_repeat_pct,
                     set_by=excluded.set_by""",
                (brand_name, month_label, target_revenue, target_stores,
                 target_repeat_pct, set_by, now)
            )

    def get_target(self, brand_name, month_label):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM brand_targets WHERE brand_name=? AND month_label=?",
                (brand_name, month_label)
            ).fetchone()
            return dict(row) if row else None

    def get_all_targets(self, month_label=None):
        with self._connect() as conn:
            if month_label:
                rows = conn.execute(
                    "SELECT * FROM brand_targets WHERE month_label=? ORDER BY brand_name",
                    (month_label,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM brand_targets ORDER BY month_label DESC, brand_name"
                ).fetchall()
            return [dict(r) for r in rows]

    # ── Alert rule operations ─────────────────────────────────────────────────

    def save_alert_rule(self, rule_name, brand_filter, metric, operator,
                        threshold, severity='medium'):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO alert_rules
                   (rule_name, brand_filter, metric, operator, threshold, severity, active, created_at)
                   VALUES (?,?,?,?,?,?,1,?)""",
                (rule_name, brand_filter, metric, operator, threshold, severity, now)
            )
            return cur.lastrowid

    def get_alert_rules(self, active_only=True):
        with self._connect() as conn:
            if active_only:
                rows = conn.execute(
                    "SELECT * FROM alert_rules WHERE active=1 ORDER BY created_at DESC"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM alert_rules ORDER BY created_at DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    def toggle_alert_rule(self, rule_id, active):
        with self._connect() as conn:
            conn.execute("UPDATE alert_rules SET active=? WHERE id=?", (1 if active else 0, rule_id))

    def delete_alert_rule(self, rule_id):
        with self._connect() as conn:
            conn.execute("DELETE FROM alert_rules WHERE id=?", (rule_id,))

    # ── Activity log operations ───────────────────────────────────────────────

    def log_activity(self, action, detail=None, brand_name=None, report_id=None):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO activity_log (action, detail, brand_name, report_id, created_at) VALUES (?,?,?,?,?)",
                (action, detail, brand_name, report_id, now)
            )

    def get_activity_log(self, limit=50, brand_name=None):
        with self._connect() as conn:
            if brand_name:
                return [dict(r) for r in conn.execute(
                    "SELECT * FROM activity_log WHERE brand_name=? ORDER BY created_at DESC LIMIT ?",
                    (brand_name, limit)
                ).fetchall()]
            return [dict(r) for r in conn.execute(
                "SELECT * FROM activity_log ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()]

    # ── Activity intelligence operations ─────────────────────────────────────

    def save_activity_import(self, payload: dict, source_filename: str, source_type: str = None,
                             report_id: int = None):
        """Persist a normalized activity import payload."""
        now = datetime.now().isoformat(timespec='seconds')
        summary = payload.get('summary') or {}
        start_date = summary.get('start_date')
        end_date = summary.get('end_date')
        row_count = int(summary.get('row_count') or len(payload.get('events', [])))

        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO activity_batches
                   (source_filename, source_type, report_id, start_date, end_date,
                    row_count, import_status, summary_json, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (
                    source_filename,
                    source_type,
                    report_id,
                    start_date,
                    end_date,
                    row_count,
                    'imported',
                    json.dumps(summary),
                    now,
                    now,
                )
            )
            batch_id = cur.lastrowid

            event_rows = []
            for row in payload.get('events', []):
                event_rows.append((
                    batch_id,
                    report_id,
                    row.get('activity_date'),
                    row.get('salesman_name'),
                    row.get('salesman_code'),
                    row.get('salesman_designation'),
                    row.get('reporting_person_name'),
                    row.get('survey_code'),
                    row.get('survey_name'),
                    row.get('survey_start_date'),
                    row.get('survey_end_date'),
                    row.get('retailer_code'),
                    row.get('retailer_name'),
                    row.get('retailer_type'),
                    row.get('retailer_state'),
                    row.get('retailer_district'),
                    row.get('retailer_city'),
                    row.get('question'),
                    row.get('answer_type'),
                    row.get('label'),
                    row.get('answer'),
                    now,
                ))
            if event_rows:
                conn.executemany(
                    """INSERT INTO activity_events
                       (batch_id, report_id, activity_date, salesman_name, salesman_code,
                        salesman_designation, reporting_person_name, survey_code, survey_name,
                        survey_start_date, survey_end_date, retailer_code, retailer_name,
                        retailer_type, retailer_state, retailer_district, retailer_city,
                        question, answer_type, label, answer, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    event_rows
                )

            visit_rows = []
            for row in payload.get('visits', []):
                visit_rows.append((
                    batch_id,
                    report_id,
                    row.get('visit_key'),
                    row.get('activity_date'),
                    row.get('salesman_name'),
                    row.get('survey_name'),
                    row.get('retailer_code'),
                    row.get('retailer_name'),
                    row.get('retailer_state'),
                    row.get('retailer_city'),
                    int(row.get('event_count') or 0),
                    int(row.get('issue_count') or 0),
                    int(row.get('opportunity_count') or 0),
                    int(row.get('photo_count') or 0),
                    now,
                ))
            if visit_rows:
                conn.executemany(
                    """INSERT INTO activity_visits
                       (batch_id, report_id, visit_key, activity_date, salesman_name, survey_name,
                        retailer_code, retailer_name, retailer_state, retailer_city,
                        event_count, issue_count, opportunity_count, photo_count, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    visit_rows
                )

            issue_rows = []
            for row in payload.get('issues', []):
                issue_rows.append((
                    batch_id,
                    report_id,
                    row.get('activity_date'),
                    row.get('retailer_code'),
                    row.get('retailer_name'),
                    row.get('salesman_name'),
                    row.get('brand_name'),
                    row.get('sku_name'),
                    row.get('issue_type'),
                    row.get('severity') or 'medium',
                    row.get('question'),
                    row.get('label'),
                    row.get('answer'),
                    row.get('status') or 'open',
                    now,
                ))
            if issue_rows:
                conn.executemany(
                    """INSERT INTO activity_issues
                       (batch_id, report_id, activity_date, retailer_code, retailer_name,
                        salesman_name, brand_name, sku_name, issue_type, severity,
                        question, label, answer, status, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    issue_rows
                )

            mention_rows = []
            for row in payload.get('brand_mentions', []):
                mention_rows.append((
                    batch_id,
                    report_id,
                    row.get('brand_name'),
                    row.get('sku_name'),
                    row.get('retailer_code'),
                    row.get('retailer_name'),
                    row.get('activity_date'),
                    row.get('source_kind'),
                    row.get('source_value'),
                    now,
                ))
            if mention_rows:
                conn.executemany(
                    """INSERT INTO activity_brand_mentions
                       (batch_id, report_id, brand_name, sku_name, retailer_code, retailer_name,
                        activity_date, source_kind, source_value, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    mention_rows
                )

        self.log_activity(
            'activity_import',
            f'{source_filename} ({row_count:,} rows)',
            report_id=report_id,
        )
        return batch_id

    def get_activity_batches(self, limit=50):
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM activity_batches ORDER BY created_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_latest_activity_batch(self):
        rows = self.get_activity_batches(limit=1)
        return rows[0] if rows else None

    def get_activity_summary(self, batch_id=None, report_id=None, brand_name=None):
        filters = []
        params = []
        if batch_id:
            filters.append("ae.batch_id=?")
            params.append(batch_id)
        if report_id:
            filters.append("ae.report_id=?")
            params.append(report_id)

        issue_filters = [f.replace('ae.', 'ai.') for f in filters]
        issue_params = list(params)
        visit_filters = [f.replace('ae.', 'av.') for f in filters]
        visit_params = list(params)
        mention_filters = [f.replace('ae.', 'abm.') for f in filters]
        mention_params = list(params)

        if brand_name:
            issue_filters.append("LOWER(COALESCE(ai.brand_name,''))=LOWER(?)")
            issue_params.append(brand_name)
            mention_filters.append("LOWER(COALESCE(abm.brand_name,''))=LOWER(?)")
            mention_params.append(brand_name)

        where_events = (' WHERE ' + ' AND '.join(filters)) if filters else ''
        where_issues = (' WHERE ' + ' AND '.join(issue_filters)) if issue_filters else ''
        where_visits = (' WHERE ' + ' AND '.join(visit_filters)) if visit_filters else ''
        where_mentions = (' WHERE ' + ' AND '.join(mention_filters)) if mention_filters else ''

        with self._connect() as conn:
            totals = conn.execute(
                f"""SELECT COUNT(*) AS events,
                           COUNT(DISTINCT retailer_code) AS stores,
                           COUNT(DISTINCT salesman_name) AS salespeople,
                           COUNT(DISTINCT activity_date) AS active_days
                    FROM activity_events ae
                    {where_events}""",
                params
            ).fetchone()
            visits = conn.execute(
                f"""SELECT COUNT(*) AS visits,
                           SUM(issue_count) AS issues,
                           SUM(opportunity_count) AS opportunities,
                           SUM(photo_count) AS photos
                    FROM activity_visits av
                    {where_visits}""",
                visit_params
            ).fetchone()
            top_issues = conn.execute(
                f"""SELECT issue_type, COUNT(*) AS count
                    FROM activity_issues ai
                    {where_issues}
                    GROUP BY issue_type
                    ORDER BY count DESC, issue_type
                    LIMIT 8""",
                issue_params
            ).fetchall()
            top_brands = conn.execute(
                f"""SELECT COALESCE(brand_name, 'Unmatched') AS brand_name, COUNT(*) AS count
                    FROM activity_brand_mentions abm
                    {where_mentions}
                    GROUP BY COALESCE(brand_name, 'Unmatched')
                    ORDER BY count DESC, brand_name
                    LIMIT 12""",
                mention_params
            ).fetchall()
            top_salespeople = conn.execute(
                f"""SELECT salesman_name,
                           COUNT(*) AS visits,
                           SUM(issue_count) AS issues,
                           SUM(opportunity_count) AS opportunities
                    FROM activity_visits av
                    {where_visits}
                    GROUP BY salesman_name
                    ORDER BY visits DESC, salesman_name
                    LIMIT 12""",
                visit_params
            ).fetchall()
            recent_issues = conn.execute(
                f"""SELECT * FROM activity_issues ai
                    {where_issues}
                    ORDER BY activity_date DESC, id DESC
                    LIMIT 20""",
                issue_params
            ).fetchall()
            recent_visits = conn.execute(
                f"""SELECT * FROM activity_visits av
                    {where_visits}
                    ORDER BY activity_date DESC, id DESC
                    LIMIT 20""",
                visit_params
            ).fetchall()

        return {
            'totals': {
                'events': int(totals['events'] or 0),
                'stores': int(totals['stores'] or 0),
                'salespeople': int(totals['salespeople'] or 0),
                'active_days': int(totals['active_days'] or 0),
                'visits': int(visits['visits'] or 0),
                'issues': int(visits['issues'] or 0),
                'opportunities': int(visits['opportunities'] or 0),
                'photos': int(visits['photos'] or 0),
            },
            'top_issues': [dict(r) for r in top_issues],
            'top_brands': [dict(r) for r in top_brands],
            'top_salespeople': [dict(r) for r in top_salespeople],
            'recent_issues': [dict(r) for r in recent_issues],
            'recent_visits': [dict(r) for r in recent_visits],
        }

    def get_activity_brand_summary(self, brand_name, limit=12):
        with self._connect() as conn:
            totals = conn.execute(
                """SELECT COUNT(*) AS mentions,
                          COUNT(DISTINCT retailer_code) AS stores,
                          COUNT(DISTINCT activity_date) AS active_days
                   FROM activity_brand_mentions
                   WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?)""",
                (brand_name,)
            ).fetchone()
            issue_counts = conn.execute(
                """SELECT issue_type, COUNT(*) AS count
                   FROM activity_issues
                   WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?)
                   GROUP BY issue_type
                   ORDER BY count DESC, issue_type
                   LIMIT ?""",
                (brand_name, limit)
            ).fetchall()
            store_rows = conn.execute(
                """SELECT retailer_code, retailer_name, COUNT(*) AS mentions
                   FROM activity_brand_mentions
                   WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?)
                   GROUP BY retailer_code, retailer_name
                   ORDER BY mentions DESC, retailer_name
                   LIMIT ?""",
                (brand_name, limit)
            ).fetchall()
            recent_issues = conn.execute(
                """SELECT * FROM activity_issues
                   WHERE LOWER(COALESCE(brand_name, ''))=LOWER(?)
                   ORDER BY activity_date DESC, id DESC
                   LIMIT ?""",
                (brand_name, limit)
            ).fetchall()
            latest_visit = conn.execute(
                """SELECT abm.activity_date, abm.retailer_name, ae.salesman_name
                   FROM activity_brand_mentions abm
                   LEFT JOIN activity_events ae
                     ON ae.retailer_code = abm.retailer_code
                    AND ae.activity_date = abm.activity_date
                   WHERE LOWER(COALESCE(abm.brand_name, ''))=LOWER(?)
                   ORDER BY abm.activity_date DESC, abm.id DESC
                   LIMIT 1""",
                (brand_name,)
            ).fetchone()
        return {
            'brand_name': brand_name,
            'mentions': int(totals['mentions'] or 0),
            'stores': int(totals['stores'] or 0),
            'active_days': int(totals['active_days'] or 0),
            'issue_counts': [dict(r) for r in issue_counts],
            'stores_seen': [dict(r) for r in store_rows],
            'recent_issues': [dict(r) for r in recent_issues],
            'latest_visit': dict(latest_visit) if latest_visit else None,
        }

    def get_store_activity_summary(self, retailer_code):
        with self._connect() as conn:
            totals = conn.execute(
                """SELECT retailer_name, retailer_state, retailer_city,
                          COUNT(*) AS events,
                          COUNT(DISTINCT activity_date) AS active_days,
                          COUNT(DISTINCT salesman_name) AS salespeople
                   FROM activity_events
                   WHERE retailer_code=?
                   GROUP BY retailer_name, retailer_state, retailer_city
                   ORDER BY activity_date DESC
                   LIMIT 1""",
                (retailer_code,)
            ).fetchone()
            visits = conn.execute(
                """SELECT * FROM activity_visits
                   WHERE retailer_code=?
                   ORDER BY activity_date DESC, id DESC
                   LIMIT 20""",
                (retailer_code,)
            ).fetchall()
            issues = conn.execute(
                """SELECT * FROM activity_issues
                   WHERE retailer_code=?
                   ORDER BY activity_date DESC, id DESC
                   LIMIT 20""",
                (retailer_code,)
            ).fetchall()
            brands = conn.execute(
                """SELECT COALESCE(brand_name, 'Unmatched') AS brand_name, COUNT(*) AS mentions
                   FROM activity_brand_mentions
                   WHERE retailer_code=?
                   GROUP BY COALESCE(brand_name, 'Unmatched')
                   ORDER BY mentions DESC, brand_name
                   LIMIT 20""",
                (retailer_code,)
            ).fetchall()
        return {
            'store': dict(totals) if totals else None,
            'visits': [dict(r) for r in visits],
            'issues': [dict(r) for r in issues],
            'brands': [dict(r) for r in brands],
        }

    # ── Agent action + memory operations ────────────────────────────────────

    @staticmethod
    def _safe_json_loads(value, fallback):
        try:
            return json.loads(value) if isinstance(value, str) else (value if value is not None else fallback)
        except Exception:
            return fallback

    def _hydrate_agent_memory(self, row):
        item = dict(row)
        item['tags'] = self._safe_json_loads(item.get('tags') or '[]', [])
        item['metadata'] = self._safe_json_loads(item.get('metadata_json') or '{}', {})
        item['subject_type'] = item.get('subject_type') or item.get('scope_type')
        item['subject_key'] = item.get('subject_key') or item.get('scope_key')
        item['pinned'] = bool(item.get('pinned'))
        return item

    def _hydrate_scheduled_job(self, row):
        item = dict(row)
        item['payload'] = self._safe_json_loads(item.get('payload_json') or '{}', {})
        item['last_result'] = self._safe_json_loads(item.get('last_result') or '{}', {})
        item['active'] = bool(item.get('active'))
        item['status'] = item.get('status') or ('active' if item['active'] else 'paused')
        item['cadence'] = item.get('cadence') or item.get('cron_expr')
        return item

    def _sync_agent_memory_fts(self, conn, memory_id, scope_type, scope_key, memory_text):
        try:
            conn.execute("DELETE FROM agent_memories_fts WHERE rowid=?", (memory_id,))
            conn.execute(
                "INSERT INTO agent_memories_fts(rowid, scope_type, scope_key, memory_text) VALUES (?,?,?,?)",
                (memory_id, scope_type, scope_key, memory_text)
            )
        except sqlite3.OperationalError:
            pass

    def create_agent_action(self, agent_type, subject_type, subject_key, title,
                            reason=None, proposed_payload=None, report_id=None,
                            priority='medium', action_signature=None):
        now = datetime.now().isoformat(timespec='seconds')
        payload_json = json.dumps(proposed_payload or {})
        with self._connect() as conn:
            if action_signature:
                existing = conn.execute(
                    """SELECT * FROM agent_actions
                       WHERE action_signature=? AND status IN ('pending', 'approved')
                       ORDER BY created_at DESC LIMIT 1""",
                    (action_signature,)
                ).fetchone()
                if existing:
                    return dict(existing)
            cur = conn.execute(
                """INSERT INTO agent_actions
                   (agent_type, subject_type, subject_key, report_id, priority,
                    title, reason, proposed_payload, action_signature,
                    status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    agent_type,
                    subject_type,
                    subject_key,
                    report_id,
                    priority,
                    title,
                    reason,
                    payload_json,
                    action_signature,
                    'pending',
                    now,
                    now,
                )
            )
            return self.get_agent_action(cur.lastrowid)

    def get_agent_action(self, action_id):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM agent_actions WHERE id=?",
                (action_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_agent_actions(self, status='pending', limit=100):
        with self._connect() as conn:
            if status == 'all':
                rows = conn.execute(
                    "SELECT * FROM agent_actions ORDER BY created_at DESC LIMIT ?",
                    (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    """SELECT * FROM agent_actions
                       WHERE status=?
                       ORDER BY
                         CASE priority WHEN 'critical' THEN 0 WHEN 'high' THEN 1
                                       WHEN 'medium' THEN 2 ELSE 3 END,
                         created_at DESC
                       LIMIT ?""",
                    (status, limit)
                ).fetchall()
        items = [dict(r) for r in rows]
        for item in items:
            try:
                item['proposed_payload'] = json.loads(item.get('proposed_payload') or '{}')
            except Exception:
                item['proposed_payload'] = {}
        return items

    def save_agent_memory(self, scope_type, scope_key, memory_text,
                          memory_kind='note', confidence=0.5, source=None,
                          memory_layer='session', subject_type=None, subject_key=None,
                          recency=None, tags=None, related_report_id=None,
                          related_brand=None, pinned=False, metadata=None):
        now = datetime.now().isoformat(timespec='seconds')
        subject_type = subject_type or scope_type
        subject_key = subject_key or scope_key
        recency = float(recency if recency is not None else datetime.now().timestamp())
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO agent_memories
                   (scope_type, scope_key, memory_kind, memory_text, confidence, source,
                    created_at, updated_at, memory_layer, subject_type, subject_key,
                    recency, tags, related_report_id, related_brand, pinned, metadata_json)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    scope_type,
                    scope_key,
                    memory_kind,
                    memory_text,
                    confidence,
                    source,
                    now,
                    now,
                    memory_layer,
                    subject_type,
                    subject_key,
                    recency,
                    json.dumps(tags or []),
                    related_report_id,
                    related_brand,
                    1 if pinned else 0,
                    json.dumps(metadata or {}),
                )
            )
            memory_id = cur.lastrowid
            self._sync_agent_memory_fts(conn, memory_id, scope_type, scope_key, memory_text)
            return memory_id

    def get_agent_memory(self, memory_id):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM agent_memories WHERE id=?",
                (memory_id,)
            ).fetchone()
        return self._hydrate_agent_memory(row) if row else None

    def list_agent_memories(self, limit=50, memory_layer=None, pinned=None,
                            subject_type=None, subject_key=None, query=None):
        clauses = []
        params = []
        if memory_layer:
            clauses.append("memory_layer=?")
            params.append(memory_layer)
        if pinned is not None:
            clauses.append("pinned=?")
            params.append(1 if pinned else 0)
        if subject_type:
            clauses.append("(subject_type=? OR scope_type=?)")
            params.extend([subject_type, subject_type])
        if subject_key:
            clauses.append("(subject_key=? OR scope_key=?)")
            params.extend([subject_key, subject_key])
        if query:
            clauses.append("memory_text LIKE ?")
            params.append(f'%{query}%')
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT *
                    FROM agent_memories
                    {where_clause}
                    ORDER BY pinned DESC, confidence DESC, recency DESC, updated_at DESC
                    LIMIT ?""",
                (*params, limit)
            ).fetchall()
        return [self._hydrate_agent_memory(row) for row in rows]

    def pin_agent_memory(self, memory_id, pinned=True):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                "UPDATE agent_memories SET pinned=?, updated_at=? WHERE id=?",
                (1 if pinned else 0, now, memory_id)
            )
        return self.get_agent_memory(memory_id)

    def search_agent_memories(self, query, limit=8, subject_type=None, subject_key=None, tags=None):
        q = str(query or '').strip()
        tag_terms = [str(tag).strip().lower() for tag in (tags or []) if str(tag).strip()]
        if not q and not subject_type and not subject_key and not tag_terms:
            return []
        rows = []
        seen = set()

        def _add_rows(found_rows, retrieval_stage):
            for raw in found_rows:
                memory = self._hydrate_agent_memory(raw)
                memory_id = memory.get('id')
                if memory_id in seen:
                    continue
                memory['retrieval_stage'] = retrieval_stage
                rows.append(memory)
                seen.add(memory_id)

        with self._connect() as conn:
            if subject_type and subject_key:
                entity_rows = conn.execute(
                    """SELECT *
                       FROM agent_memories
                       WHERE (subject_type=? OR scope_type=?)
                         AND (subject_key=? OR scope_key=?)
                       ORDER BY pinned DESC, confidence DESC, recency DESC, updated_at DESC
                       LIMIT ?""",
                    (subject_type, subject_type, subject_key, subject_key, max(limit * 2, 12))
                ).fetchall()
                _add_rows(entity_rows, 'entity')

            if tag_terms:
                tag_rows = conn.execute(
                    """SELECT *
                       FROM agent_memories
                       WHERE """ + " OR ".join(["LOWER(tags) LIKE ?" for _ in tag_terms]) + """
                       ORDER BY pinned DESC, confidence DESC, recency DESC, updated_at DESC
                       LIMIT ?""",
                    (*[f'%"{tag}"%' for tag in tag_terms], max(limit * 2, 12))
                ).fetchall()
                _add_rows(tag_rows, 'tag')

            if q:
                try:
                    semantic_rows = conn.execute(
                        """SELECT am.*
                           FROM agent_memories_fts f
                           JOIN agent_memories am ON am.id = f.rowid
                           WHERE agent_memories_fts MATCH ?
                           ORDER BY rank
                           LIMIT ?""",
                        (q, max(limit * 2, 12))
                    ).fetchall()
                except sqlite3.OperationalError:
                    semantic_rows = conn.execute(
                        """SELECT *
                           FROM agent_memories
                           WHERE memory_text LIKE ?
                           ORDER BY updated_at DESC
                           LIMIT ?""",
                        (f'%{q}%', max(limit * 2, 12))
                    ).fetchall()
                _add_rows(semantic_rows, 'semantic')

        query_terms = set(normalize_name_key(q).split()) if q else set()

        def _score(memory):
            score = 0.0
            if memory.get('retrieval_stage') == 'entity':
                score += 60
            elif memory.get('retrieval_stage') == 'tag':
                score += 35
            elif memory.get('retrieval_stage') == 'semantic':
                score += 20
            if memory.get('pinned'):
                score += 30
            score += float(memory.get('confidence') or 0) * 10
            score += min(float(memory.get('recency') or 0) / 1_000_000_000, 25)
            if query_terms:
                memory_terms = set(normalize_name_key(memory.get('memory_text')).split())
                score += len(query_terms & memory_terms) * 3
            return score

        rows.sort(key=lambda item: (_score(item), item.get('updated_at') or ''), reverse=True)
        return rows[:limit]

    def record_agent_feedback(self, action_id, feedback_type, actor=None, note=None):
        now = datetime.now().isoformat(timespec='seconds')
        action = self.get_agent_action(action_id)
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO agent_feedback
                   (action_id, feedback_type, original_status, actor, note, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (action_id, feedback_type, action.get('status') if action else None, actor, note, now)
            )

    def update_agent_action_status(self, action_id, status, actor='admin', note=None):
        now = datetime.now().isoformat(timespec='seconds')
        action = self.get_agent_action(action_id)
        if not action:
            return None
        approved_at = now if status == 'approved' else action.get('approved_at')
        applied_at = now if status == 'applied' else action.get('applied_at')
        with self._connect() as conn:
            conn.execute(
                """UPDATE agent_actions
                   SET status=?, approved_by=?, approved_at=?, applied_at=?, updated_at=?
                   WHERE id=?""",
                (status, actor, approved_at, applied_at, now, action_id)
            )
        self.record_agent_feedback(action_id, status, actor=actor, note=note)
        memory_text = f"{action['title']} -> {status}"
        if note:
            memory_text += f" ({note})"
        self.save_agent_memory(
            scope_type=action['subject_type'],
            scope_key=action['subject_key'],
            memory_text=memory_text,
            memory_kind='action_feedback',
            confidence=0.7 if status == 'approved' else 0.55,
            source='agent_feedback',
        )
        return self.get_agent_action(action_id)

    def save_recommendation_outcome(self, brand_name, recommendation_key, outcome_type,
                                    outcome_value=None, note=None, report_id=None):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO recommendation_outcomes
                   (brand_name, report_id, recommendation_key, outcome_type, outcome_value, note, created_at)
                   VALUES (?,?,?,?,?,?,?)""",
                (brand_name, report_id, recommendation_key, outcome_type, outcome_value, note, now)
            )

    def get_recommendation_outcome_scores(self, brand_name=None):
        clauses = []
        params = []
        if brand_name:
            clauses.append("brand_name=?")
            params.append(brand_name)
        where_clause = f"WHERE {' AND '.join(clauses)}" if clauses else ''
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT recommendation_key,
                           COUNT(*) AS total_events,
                           COALESCE(SUM(CASE
                               WHEN outcome_type IN ('success', 'approved', 'improved', 'positive') THEN COALESCE(outcome_value, 1)
                               WHEN outcome_type IN ('failed', 'rejected', 'negative') THEN -ABS(COALESCE(outcome_value, 1))
                               ELSE 0
                           END), 0) AS weighted_score
                    FROM recommendation_outcomes
                    {where_clause}
                    GROUP BY recommendation_key""",
                params
            ).fetchall()
        return {
            row['recommendation_key']: {
                'total_events': row['total_events'],
                'weighted_score': float(row['weighted_score'] or 0),
            }
            for row in rows
        }

    # ── SKU analytics queries ─────────────────────────────────────────────────

    def get_top_skus_all_brands(self, report_id, limit=20):
        """Return top SKUs by revenue across all brands for a report."""
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                """SELECT brand_name, unique_skus, total_revenue
                   FROM brand_kpis WHERE report_id=?
                   ORDER BY total_revenue DESC LIMIT ?""",
                (report_id, limit)
            ).fetchall()]

    def get_leaderboard(self, report_id):
        """Return brands ranked across 4 categories for a report."""
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT brand_name, total_revenue, num_stores, repeat_pct,
                          perf_score, perf_grade, unique_skus, avg_revenue_per_store
                   FROM brand_kpis WHERE report_id=?
                   ORDER BY total_revenue DESC""",
                (report_id,)
            ).fetchall()
            return [dict(r) for r in rows]

    def get_scheduled_reports(self):
        return self.list_assistant_jobs(limit=100)

    def save_scheduled_report(self, label, cron_expr, file_path=None,
                               start_offset=1, end_offset=0):
        return self.create_assistant_job(
            job_type='report',
            label=label,
            target=file_path,
            cadence=cron_expr,
            payload={
                'start_offset': start_offset,
                'end_offset': end_offset,
            },
        )

    def update_scheduled_last_run(self, schedule_id):
        self.record_assistant_job_run(schedule_id)

    def get_assistant_job(self, job_id):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM scheduled_reports WHERE id=?",
                (job_id,)
            ).fetchone()
        return self._hydrate_scheduled_job(row) if row else None

    def list_assistant_jobs(self, status=None, limit=100):
        params = []
        where_clause = ""
        if status and status != 'all':
            where_clause = "WHERE status=?"
            params.append(status)
        with self._connect() as conn:
            rows = conn.execute(
                f"""SELECT * FROM scheduled_reports
                    {where_clause}
                    ORDER BY
                      CASE status WHEN 'running' THEN 0 WHEN 'active' THEN 1
                                  WHEN 'paused' THEN 2 ELSE 3 END,
                      COALESCE(next_run, last_run, created_at) ASC
                    LIMIT ?""",
                (*params, limit)
            ).fetchall()
        return [self._hydrate_scheduled_job(row) for row in rows]

    def create_assistant_job(self, job_type, target=None, payload=None, cadence='manual',
                             label=None, connector=None, next_run=None, status='active'):
        now = datetime.now().isoformat(timespec='seconds')
        normalized_status = status or 'active'
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO scheduled_reports
                   (label, cron_expr, file_path, start_offset, end_offset, active, last_run,
                    created_at, job_type, target, payload_json, cadence, next_run,
                    last_result, status, updated_at, connector)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    label or f'{job_type} job',
                    cadence or 'manual',
                    target,
                    int((payload or {}).get('start_offset', 1)),
                    int((payload or {}).get('end_offset', 0)),
                    0 if normalized_status == 'paused' else 1,
                    None,
                    now,
                    job_type,
                    target,
                    json.dumps(payload or {}),
                    cadence or 'manual',
                    next_run,
                    json.dumps({}),
                    normalized_status,
                    now,
                    connector,
                )
            )
            return cur.lastrowid

    def update_assistant_job(self, job_id, **updates):
        if not updates:
            return self.get_assistant_job(job_id)
        now = datetime.now().isoformat(timespec='seconds')
        prepared = {}
        for key, value in updates.items():
            if key == 'payload':
                prepared['payload_json'] = json.dumps(value or {})
            elif key == 'last_result':
                prepared['last_result'] = json.dumps(value or {})
            elif key == 'status':
                prepared['status'] = value
                if value in {'paused', 'disabled'}:
                    prepared['active'] = 0
                elif value in {'active', 'running'}:
                    prepared['active'] = 1
            else:
                prepared[key] = value
        prepared['updated_at'] = now
        cols = ', '.join(f"{col}=?" for col in prepared)
        vals = list(prepared.values()) + [job_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE scheduled_reports SET {cols} WHERE id=?", vals)
        return self.get_assistant_job(job_id)

    def pause_assistant_job(self, job_id):
        return self.update_assistant_job(job_id, status='paused')

    def resume_assistant_job(self, job_id):
        return self.update_assistant_job(job_id, status='active')

    def record_assistant_job_run(self, job_id, result=None, next_run=None, status='active'):
        now = datetime.now().isoformat(timespec='seconds')
        updates = {'last_run': now, 'status': status}
        if result is not None:
            updates['last_result'] = result
        if next_run is not None:
            updates['next_run'] = next_run
        return self.update_assistant_job(job_id, **updates)

    def save_tool_execution(self, idempotency_key, tool_name, arguments=None, result=None, status='success'):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO tool_execution_log
                   (idempotency_key, tool_name, arguments_json, result_json, status, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?)
                   ON CONFLICT(idempotency_key) DO UPDATE SET
                     tool_name=excluded.tool_name,
                     arguments_json=excluded.arguments_json,
                     result_json=excluded.result_json,
                     status=excluded.status,
                     updated_at=excluded.updated_at""",
                (
                    idempotency_key,
                    tool_name,
                    json.dumps(arguments or {}),
                    json.dumps(result or {}),
                    status,
                    now,
                    now,
                )
            )

    def get_tool_execution(self, idempotency_key):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM tool_execution_log WHERE idempotency_key=?",
                (idempotency_key,)
            ).fetchone()
        if not row:
            return None
        item = dict(row)
        item['arguments'] = self._safe_json_loads(item.get('arguments_json') or '{}', {})
        item['result'] = self._safe_json_loads(item.get('result_json') or '{}', {})
        return item

    # ── AI Narratives ─────────────────────────────────────────────────────────

    def save_narrative(self, report_id, brand_name, narrative_text):
        """Cache an AI-generated narrative for a brand in a report."""
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO ai_narratives (report_id, brand_name, narrative, created_at)
                   VALUES (?,?,?,?)
                   ON CONFLICT(report_id, brand_name) DO UPDATE SET
                   narrative=excluded.narrative, created_at=excluded.created_at""",
                (report_id, brand_name, narrative_text, now)
            )

    def get_narrative(self, report_id, brand_name):
        """Return cached AI narrative for a brand, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT narrative FROM ai_narratives WHERE report_id=? AND brand_name=?",
                (report_id, brand_name)
            ).fetchone()
            return row['narrative'] if row else None

    def get_all_narratives(self, report_id):
        """Return all cached narratives for a report as {brand_name: text}."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT brand_name, narrative FROM ai_narratives WHERE report_id=?",
                (report_id,)
            ).fetchall()
            return {r['brand_name']: r['narrative'] for r in rows}

    # ── SQLite-backed Job Tracking ─────────────────────────────────────────────

    def create_job(self, job_id):
        """Create a new generation job record."""
        import json as _json
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO generation_jobs
                   (id, status, progress, total, current_brand, report_id,
                    portfolio_file, brands_done, errors, result_json, error_msg, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job_id, 'running', 0, 0, None, None, None,
                 '[]', '[]', '{}', None, now, now)
            )

    def update_job(self, job_id, **kwargs):
        """Update fields on a generation job."""
        import json as _json
        if not kwargs:
            return
        now = datetime.now().isoformat(timespec='seconds')
        # Serialize list/dict fields
        for key in ('brands_done', 'errors', 'result_json'):
            if key in kwargs and isinstance(kwargs[key], (list, dict)):
                kwargs[key] = _json.dumps(kwargs[key])
        kwargs['updated_at'] = now
        cols = ', '.join(f"{k}=?" for k in kwargs)
        vals = list(kwargs.values()) + [job_id]
        with self._connect() as conn:
            conn.execute(f"UPDATE generation_jobs SET {cols} WHERE id=?", vals)

    def get_job(self, job_id):
        """Return a generation job as a dict, or None if not found."""
        import json as _json
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM generation_jobs WHERE id=?", (job_id,)
            ).fetchone()
            if not row:
                return None
            job = dict(row)
            for key in ('brands_done', 'errors', 'result_json'):
                try:
                    job[key] = _json.loads(job[key] or ('{}' if key == 'result_json' else '[]'))
                except Exception:
                    job[key] = {} if key == 'result_json' else []
            return job

    # ── Database health & additive merge ─────────────────────────────────────

    def get_db_health_stats(self):
        """Return aggregate database statistics for the Database page banner."""
        with self._connect() as conn:
            r = conn.execute("""
                SELECT
                    COUNT(*) AS total_reports,
                    MIN(start_date) AS earliest,
                    MAX(end_date) AS latest,
                    COALESCE(SUM(total_revenue), 0) AS total_revenue
                FROM reports
            """).fetchone()
            brands_row = conn.execute(
                "SELECT COUNT(DISTINCT brand_name) AS total_brands FROM brand_kpis"
            ).fetchone()
            sparse_row = conn.execute(
                "SELECT COUNT(*) AS sparse FROM reports WHERE brand_count < 5"
            ).fetchone()
            last_activity = conn.execute(
                "SELECT action, detail, created_at FROM activity_log ORDER BY id DESC LIMIT 1"
            ).fetchone()
        stats = {
            'total_reports':  r['total_reports'] if r else 0,
            'earliest':       r['earliest'] if r else None,
            'latest':         r['latest'] if r else None,
            'total_revenue':  r['total_revenue'] if r else 0,
            'total_brands':   brands_row['total_brands'] if brands_row else 0,
            'sparse_months':  sparse_row['sparse'] if sparse_row else 0,
            'last_action':    dict(last_activity) if last_activity else None,
        }
        return stats

    def clear_brand_from_report(self, report_id: int, brand_name: str):
        """Remove one brand's data from a report without touching other brands.
        Used for additive merge mode — lets you add/update individual brands."""
        with self._connect() as conn:
            for table in ('alerts', 'brand_kpis', 'daily_sales', 'brand_detail_json',
                          'brand_forecast_history', 'store_churn'):
                conn.execute(
                    f"DELETE FROM {table} WHERE report_id=? AND brand_name=?",
                    (report_id, brand_name)
                )
            try:
                conn.execute(
                    "DELETE FROM ai_narratives WHERE report_id=? AND brand_name=?",
                    (report_id, brand_name)
                )
            except Exception:
                pass

    def refresh_report_totals(self, report_id: int):
        """Recompute stored rollups after admin edits or additive merges."""
        with self._connect() as conn:
            report = conn.execute(
                "SELECT * FROM reports WHERE id=?",
                (report_id,)
            ).fetchone()
            if not report:
                return None

            brand_rows = conn.execute(
                "SELECT total_revenue, total_qty, num_stores FROM brand_kpis WHERE report_id=?",
                (report_id,)
            ).fetchall()
            total_revenue = round(sum(float(r['total_revenue'] or 0) for r in brand_rows), 2)
            total_qty = round(sum(float(r['total_qty'] or 0) for r in brand_rows), 2)
            brand_count = len(brand_rows)

            # Exact unique-store recomputation is not possible from the stored aggregates alone.
            # Keep the previous total_stores, but clamp it when the report becomes empty.
            total_stores = 0 if brand_count == 0 else report['total_stores']

            conn.execute(
                """UPDATE reports
                   SET total_revenue=?, total_qty=?, total_stores=?, brand_count=?, generated_at=?
                   WHERE id=?""",
                (total_revenue, total_qty, total_stores, brand_count,
                 datetime.now().isoformat(timespec='seconds'), report_id)
            )
            return dict(conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone())

    def delete_report(self, report_id: int):
        """Delete a report and all dependent rows."""
        report = self.get_report(report_id)
        if not report:
            return False

        with self._connect() as conn:
            for table in ('alerts', 'brand_kpis', 'daily_sales', 'brand_detail_json',
                          'brand_forecast_history', 'store_churn', 'activity_log'):
                try:
                    conn.execute(f"DELETE FROM {table} WHERE report_id=?", (report_id,))
                except Exception:
                    pass
            try:
                conn.execute("DELETE FROM ai_narratives WHERE report_id=?", (report_id,))
            except Exception:
                pass
            conn.execute("DELETE FROM reports WHERE id=?", (report_id,))
        return True
