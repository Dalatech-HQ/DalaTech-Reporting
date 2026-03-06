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

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'dala_data.db')


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

                CREATE INDEX IF NOT EXISTS idx_bkpis_report ON brand_kpis(report_id);
                CREATE INDEX IF NOT EXISTS idx_daily_report ON daily_sales(report_id);
                CREATE INDEX IF NOT EXISTS idx_daily_brand  ON daily_sales(brand_name);
                CREATE INDEX IF NOT EXISTS idx_alerts_report ON alerts(report_id);
            """)

    # ── Report operations ─────────────────────────────────────────────────────

    def save_report(self, start_date, end_date, xls_filename,
                    total_revenue, total_qty, total_stores, brand_count):
        """Create a report record. Returns the new report_id."""
        month_label = datetime.strptime(start_date, '%Y-%m-%d').strftime('%b %Y')
        now = datetime.now().isoformat(timespec='seconds')
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO reports
                   (month_label, start_date, end_date, xls_filename,
                    total_revenue, total_qty, total_stores, brand_count, generated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (month_label, start_date, end_date, xls_filename,
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
