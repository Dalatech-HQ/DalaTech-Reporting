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

import os
import sqlite3
import uuid
from datetime import datetime

# Allow overriding via env var so a Railway Volume can be used for persistence.
# On Railway: set DATABASE_PATH=/data/dala_data.db and mount a Volume at /data
_default_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dala_data.db')
DB_PATH = os.environ.get('DATABASE_PATH', _default_db)

# If DATABASE_PATH points to a Volume path that doesn't exist yet, seed it
# from the bundled dala_data.db committed to the repo. This ensures the first
# deploy with a fresh Volume starts with full historical data instead of empty.
def _seed_volume_db_if_needed():
    if DB_PATH == _default_db:
        return  # not using a volume path, nothing to do
    if os.path.isfile(DB_PATH):
        return  # volume already has a database, leave it alone
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    if os.path.isfile(_default_db):
        import shutil
        shutil.copy2(_default_db, DB_PATH)

_seed_volume_db_if_needed()


class DataStore:
    def __init__(self, db_path=None):
        self.db_path = db_path or DB_PATH
        self._init_db()

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

                CREATE TABLE IF NOT EXISTS ai_narratives (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    report_id   INTEGER NOT NULL,
                    brand_name  TEXT NOT NULL,
                    narrative   TEXT NOT NULL,
                    created_at  TEXT NOT NULL,
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
            """)
            # ── Schema migration: add report_type to existing databases ──────
            existing = [r[1] for r in conn.execute("PRAGMA table_info(reports)").fetchall()]
            if 'report_type' not in existing:
                conn.execute("ALTER TABLE reports ADD COLUMN report_type TEXT DEFAULT 'monthly'")

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

    def get_activity_log(self, limit=50):
        with self._connect() as conn:
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
            for table in ('alerts', 'brand_kpis', 'daily_sales'):
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
