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
                    created_at   TEXT NOT NULL
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

                CREATE INDEX IF NOT EXISTS idx_forecast_brand ON brand_forecast_history(brand_name);
                CREATE INDEX IF NOT EXISTS idx_churn_report ON store_churn(report_id, brand_name);
            """)
            # ── Schema migration: add report_type to existing databases ──────
            existing = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
            if 'report_type' not in existing:
                conn.execute("ALTER TABLE reports ADD COLUMN report_type TEXT DEFAULT 'monthly'")
            queue_cols = [r[1] for r in conn.execute("PRAGMA table_info(catalog_review_queue)").fetchall()]
            if 'suggested_match_name' not in queue_cols:
                conn.execute("ALTER TABLE catalog_review_queue ADD COLUMN suggested_match_name TEXT")
            if 'similarity_score' not in queue_cols:
                conn.execute("ALTER TABLE catalog_review_queue ADD COLUMN similarity_score REAL DEFAULT 0")

    # ── Report type helpers ───────────────────────────────────────────────────

    @staticmethod
    def _infer_report_type(start_date, end_date, override=None):
        """Auto-detect report period type from date range, or use explicit override."""
        if override and override in ('weekly', 'monthly', 'quarterly', 'custom'):
            return override
        from datetime import date as _date
        try:
            s = datetime.strptime(start_date, '%Y-%m-%d').date()
            e = datetime.strptime(end_date,   '%Y-%m-%d').date()
            days = (e - s).days + 1
            if days <= 7:
                return 'weekly'
            if days <= 14:
                return 'biweekly'
            if 28 <= days <= 31:
                return 'monthly'
            if 85 <= days <= 95:
                return 'quarterly'
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
            if report_type == 'quarterly':
                return f"Q{((s.month - 1) // 3) + 1} {s.year}"
            return s.strftime('%b %Y')     # monthly / custom
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
                conn.execute("DELETE FROM ai_narratives WHERE report_id=?", (report_id,))
            except Exception:
                pass  # table may not exist on older DBs

    def update_report(self, report_id, xls_filename, total_revenue, total_qty,
                      total_stores, brand_count, report_type=None, start_date=None, end_date=None):
        """Update an existing report row's stats after re-generation."""
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
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

    def get_brand_detail_json(self, report_id, brand_name):
        """Return the stored detail JSON dict, or None."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM brand_detail_json WHERE report_id=? AND brand_name=?",
                (report_id, brand_name)
            ).fetchone()
            return dict(row) if row else None

    def get_brand_history(self, brand_name, limit=12):
        """Return brand KPIs across all reports, newest first."""
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                """SELECT bk.*, r.month_label, r.start_date, r.end_date
                   FROM brand_kpis bk JOIN reports r ON bk.report_id = r.id
                   WHERE bk.brand_name=? ORDER BY r.start_date DESC LIMIT ?""",
                (brand_name, limit)
            ).fetchall()]

    def get_all_brand_kpis(self, report_id):
        """Return all brand KPIs for a given report."""
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM brand_kpis WHERE report_id=? ORDER BY total_revenue DESC",
                (report_id,)
            ).fetchall()]

    def get_brand_kpis_single(self, report_id, brand_name):
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM brand_kpis WHERE report_id=? AND brand_name=?",
                (report_id, brand_name)
            ).fetchone()
            return dict(row) if row else None

    def get_daily_sales(self, report_id, brand_name=None):
        with self._connect() as conn:
            if brand_name:
                rows = conn.execute(
                    "SELECT * FROM daily_sales WHERE report_id=? AND brand_name=? ORDER BY date",
                    (report_id, brand_name)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM daily_sales WHERE report_id=? ORDER BY date",
                    (report_id,)
                ).fetchall()
            return [dict(r) for r in rows]

    def get_all_brands_in_db(self):
        """Return sorted list of all brand names ever stored."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT DISTINCT brand_name FROM brand_kpis ORDER BY brand_name"
            ).fetchall()
            return [r['brand_name'] for r in rows]

    def get_report_by_month(self, year: int, month: int):
        """Return report row whose start_date falls in the given year+month, or None."""
        prefix = f"{year:04d}-{month:02d}"
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM reports WHERE start_date LIKE ? ORDER BY start_date DESC LIMIT 1",
                (prefix + '%',)
            ).fetchone()
            return dict(row) if row else None

    def get_yoy_kpis(self, report_id: int):
        """
        Return a dict {brand_name: brand_kpis_row} for the same calendar month
        one year prior to the given report_id.  Returns {} if no matching report.
        """
        report = self.get_report(report_id)
        if not report:
            return {}
        try:
            from datetime import date as _date
            d = _date.fromisoformat(report['start_date'])
            prev_year = d.year - 1
            prev_report = self.get_report_by_month(prev_year, d.month)
        except Exception:
            return {}
        if not prev_report:
            return {}
        rows = self.get_all_brand_kpis(prev_report['id'])
        return {r['brand_name']: r for r in rows}

    def get_portfolio_yoy(self, report_id: int):
        """
        Return (prev_report, prev_total_revenue, prev_total_qty, prev_total_stores)
        for the same calendar month last year.  Returns (None, 0, 0, 0) if unavailable.
        """
        report = self.get_report(report_id)
        if not report:
            return None, 0, 0, 0
        try:
            from datetime import date as _date
            d = _date.fromisoformat(report['start_date'])
            prev_report = self.get_report_by_month(d.year - 1, d.month)
        except Exception:
            return None, 0, 0, 0
        if not prev_report:
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
        with self._connect() as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM scheduled_reports ORDER BY created_at DESC"
            ).fetchall()]

    def save_scheduled_report(self, label, cron_expr, file_path=None,
                               start_offset=1, end_offset=0):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO scheduled_reports
                   (label, cron_expr, file_path, start_offset, end_offset, active, created_at)
                   VALUES (?,?,?,?,?,1,?)""",
                (label, cron_expr, file_path, start_offset, end_offset, now)
            )
            return cur.lastrowid

    def update_scheduled_last_run(self, schedule_id):
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            conn.execute(
                "UPDATE scheduled_reports SET last_run=? WHERE id=?", (now, schedule_id)
            )

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
                    portfolio_file, brands_done, errors, error_msg, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (job_id, 'running', 0, 0, None, None, None,
                 '[]', '[]', None, now, now)
            )

    def update_job(self, job_id, **kwargs):
        """Update fields on a generation job."""
        import json as _json
        if not kwargs:
            return
        now = datetime.now().isoformat(timespec='seconds')
        # Serialize list/dict fields
        for key in ('brands_done', 'errors'):
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
            for key in ('brands_done', 'errors'):
                try:
                    job[key] = _json.loads(job[key] or '[]')
                except Exception:
                    job[key] = []
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
            for table in ('alerts', 'brand_kpis', 'daily_sales', 'brand_detail_json'):
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
