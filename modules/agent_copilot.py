"""
agent_copilot.py

Copilot helpers for DALA Analytics.
The goal is to combine strong deterministic reasoning with optional Gemini output,
while also returning actionable commands the UI can execute.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime

from .narrative_ai import gemini_available, _get_client  # type: ignore


def _metric_direction(value: float | None, positive='Growing', negative='Declining'):
    if value is None:
        return 'N/A'
    if value >= 5:
        return positive
    if value <= -5:
        return negative
    return 'Stable'


def _normalize_text(value) -> str:
    text = str(value or '').lower()
    text = re.sub(r'[^a-z0-9]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def _format_money(value) -> str:
    try:
        return f"₦{float(value or 0):,.2f}"
    except Exception:
        return "₦0.00"


def _format_pct(value, digits=1) -> str:
    try:
        return f"{float(value or 0):.{digits}f}%"
    except Exception:
        return f"{0:.{digits}f}%"


def _question_has(question: str, *terms: str) -> bool:
    q = _normalize_text(question)
    return any(term in q for term in terms)


def build_default_agent_actions(ds, report=None):
    report = report or ds.get_latest_report()
    if not report:
        return []

    report_id = report['id']
    created = []
    brand_rows = ds.get_all_brand_kpis(report_id)
    forecasts = {}
    try:
        from .predictor import build_brand_forecasts

        histories = {row['brand_name']: list(reversed(ds.get_brand_history(row['brand_name'], limit=12)))
                     for row in brand_rows}
        forecasts = build_brand_forecasts(histories)
    except Exception:
        forecasts = {}

    for row in brand_rows:
        brand = row['brand_name']
        signature_base = f"{report_id}:{brand}"
        if row.get('stock_days_cover', 0) and float(row.get('stock_days_cover') or 0) <= 7:
            created.append(ds.create_agent_action(
                agent_type='Brand Health Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='high',
                title=f'Protect {brand} stock position',
                reason=f"Stock cover is down to {float(row.get('stock_days_cover') or 0):.0f} days.",
                proposed_payload={
                    'kind': 'stock_follow_up',
                    'brand_name': brand,
                    'stock_days_cover': row.get('stock_days_cover'),
                },
                action_signature=f'{signature_base}:stock_protect',
            ))

        fc = forecasts.get(ds.analytics_brand_name(brand), {})
        if fc and fc.get('growth_label') == 'Declining':
            created.append(ds.create_agent_action(
                agent_type='Forecast Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='medium',
                title=f'Review {brand} revenue decline',
                reason=f"Forecast trend is declining with {fc.get('confidence_band', 'unknown')} confidence.",
                proposed_payload={
                    'kind': 'forecast_review',
                    'brand_name': brand,
                    'forecast': fc.get('forecast'),
                    'pct_change': fc.get('pct_change'),
                    'confidence_band': fc.get('confidence_band'),
                },
                action_signature=f'{signature_base}:forecast_decline',
            ))

        if row.get('repeat_pct', 0) and float(row.get('repeat_pct') or 0) < 30:
            created.append(ds.create_agent_action(
                agent_type='Brand Health Agent',
                subject_type='brand',
                subject_key=brand,
                report_id=report_id,
                priority='medium',
                title=f'Improve repeat ordering for {brand}',
                reason=f"Repeat purchase rate is {float(row.get('repeat_pct') or 0):.1f}%.",
                proposed_payload={
                    'kind': 'repeat_rate_follow_up',
                    'brand_name': brand,
                    'repeat_pct': row.get('repeat_pct'),
                },
                action_signature=f'{signature_base}:repeat_rate',
            ))

    pending_reviews = ds.get_catalog_review_queue(status='pending', limit=10)
    if pending_reviews:
        created.append(ds.create_agent_action(
            agent_type='Data Quality Agent',
            subject_type='catalog',
            subject_key='pending_reviews',
            report_id=report_id,
            priority='high',
            title='Resolve pending brand and SKU review items',
            reason=f'{len(pending_reviews)} catalog review item(s) are waiting for decision.',
            proposed_payload={
                'kind': 'catalog_review',
                'pending_count': len(pending_reviews),
            },
            action_signature=f'{report_id}:catalog_reviews',
        ))

    activity_summary = ds.get_activity_summary(report_id=report_id)
    if activity_summary['totals']['issues'] >= 5:
        top_issue = activity_summary['top_issues'][0]['issue_type'] if activity_summary['top_issues'] else 'field issues'
        created.append(ds.create_agent_action(
            agent_type='Activity Agent',
            subject_type='activity_batch',
            subject_key=str(report_id),
            report_id=report_id,
            priority='high',
            title='Review repeated field issues from latest activity batch',
            reason=f"{activity_summary['totals']['issues']} issues logged. Top theme: {top_issue}.",
            proposed_payload={
                'kind': 'activity_issue_review',
                'top_issue': top_issue,
                'issue_total': activity_summary['totals']['issues'],
            },
            action_signature=f'{report_id}:activity_issue_review',
        ))

    return [action for action in created if action]


def _month_lookup():
    return {
        'jan': 1, 'january': 1,
        'feb': 2, 'february': 2,
        'mar': 3, 'march': 3,
        'apr': 4, 'april': 4,
        'may': 5,
        'jun': 6, 'june': 6,
        'jul': 7, 'july': 7,
        'aug': 8, 'august': 8,
        'sep': 9, 'sept': 9, 'september': 9,
        'oct': 10, 'october': 10,
        'nov': 11, 'november': 11,
        'dec': 12, 'december': 12,
    }


def _find_brand_from_question(ds, question: str, explicit_brand_name: str | None = None) -> str | None:
    if explicit_brand_name:
        return ds.analytics_brand_name(explicit_brand_name)

    q = _normalize_text(question)
    if not q:
        return None

    matches = []
    for brand_name in ds.get_all_brands_in_db():
        normalized_brand = _normalize_text(brand_name)
        if not normalized_brand:
            continue
        if normalized_brand == q:
            return brand_name
        if f" {normalized_brand} " in f" {q} ":
            score = len(normalized_brand) + (normalized_brand.count(' ') * 5)
            matches.append((score, brand_name))

    if not matches:
        return None
    matches.sort(key=lambda item: (-item[0], item[1]))
    return matches[0][1]


def _find_report_from_question(ds, question: str, fallback_report=None):
    reports = ds.get_all_reports()
    if not reports:
        return fallback_report, None

    q = _normalize_text(question)
    if not q:
        return fallback_report or reports[0], None

    match = re.search(r'\breport\s+(\d+)\b', q)
    if match:
        report = ds.get_report(int(match.group(1)))
        if report:
            return report, f"Matched report {report['id']}."

    monthly_reports = [row for row in reports if str(row.get('report_type') or '').lower() == 'monthly']
    weekly_reports = [row for row in reports if str(row.get('report_type') or '').lower() == 'weekly']

    if _question_has(q, 'latest weekly', 'weekly report', 'this week', 'week 1', 'week one', 'week of'):
        if weekly_reports:
            return weekly_reports[0], f"Using weekly report {weekly_reports[0]['month_label']}."

    if _question_has(q, 'latest monthly', 'monthly report', 'this month'):
        if monthly_reports:
            return monthly_reports[0], f"Using monthly report {monthly_reports[0]['month_label']}."

    if 'last month' in q and len(monthly_reports) >= 2:
        return monthly_reports[1], f"Using prior monthly report {monthly_reports[1]['month_label']}."

    month_map = _month_lookup()
    month_match = re.search(
        r'\b(' + '|'.join(month_map.keys()) + r')\s+(\d{4})\b',
        q
    )
    if month_match:
        month_num = month_map[month_match.group(1)]
        year_num = int(month_match.group(2))
        report = ds.get_report_by_month(year_num, month_num)
        if report:
            return report, f"Matched {report['month_label']}."

    if _question_has(q, 'latest report', 'current report', 'latest', 'current'):
        return reports[0], f"Using latest report {reports[0]['month_label']}."

    return fallback_report or reports[0], None


def _focus_from_question(question: str) -> str:
    q = _normalize_text(question)
    if any(term in q for term in ('stock', 'inventory', 'closing stock', 'pickup')):
        return 'stock'
    if any(term in q for term in ('activity', 'field', 'visit', 'issue', 'merchand', 'shelf')):
        return 'activity'
    if any(term in q for term in ('forecast', 'gmv', 'trend', 'outlook', 'future')):
        return 'forecast'
    if any(term in q for term in ('repeat', 'reorder', 'loyalty')):
        return 'repeat'
    if any(term in q for term in ('action', 'pending', 'approve', 'reject')):
        return 'actions'
    if any(term in q for term in ('report', 'dashboard', 'week', 'month')):
        return 'report'
    return 'general'


def _navigation_command_for_question(question: str, report, brand_name=None, retailer_code=None):
    q = _normalize_text(question)
    wants_navigation = any(
        term in q for term in (
            'go to', 'open', 'take me', 'show me', 'navigate', 'bring up'
        )
    )
    if not wants_navigation:
        return None

    if brand_name:
        return {
            'kind': 'open_brand',
            'label': f'Open {brand_name}',
            'brand_name': brand_name,
            'report_id': report['id'] if report else None,
            'auto': not any(term in q for term in ('analyse', 'analyze', 'summar', 'explain', '?')),
        }
    if retailer_code:
        return {
            'kind': 'open_store',
            'label': f'Open store {retailer_code}',
            'retailer_code': retailer_code,
            'auto': True,
        }
    if 'activity' in q:
        return {
            'kind': 'open_page',
            'label': 'Open Activity',
            'page': 'activity_intelligence',
            'report_id': report['id'] if report else None,
            'auto': True,
        }
    if 'forecast' in q:
        return {
            'kind': 'open_page',
            'label': 'Open Forecasting',
            'page': 'forecasting',
            'auto': True,
        }
    if 'catalog' in q:
        return {
            'kind': 'open_page',
            'label': 'Open Catalog',
            'page': 'catalog',
            'auto': True,
        }
    if 'leaderboard' in q:
        return {
            'kind': 'open_page',
            'label': 'Open Leaderboard',
            'page': 'leaderboard',
            'auto': True,
        }
    if 'history' in q:
        return {
            'kind': 'open_page',
            'label': 'Open History',
            'page': 'history',
            'auto': True,
        }
    if report:
        return {
            'kind': 'open_report',
            'label': f"Open {report['month_label']}",
            'report_id': report['id'],
            'auto': not any(term in q for term in ('analyse', 'analyze', 'summar', 'explain', '?')),
        }
    return None


def _match_pending_action(ds, question: str, brand_name=None, report=None):
    q = _normalize_text(question)
    pending = ds.list_agent_actions(status='pending', limit=120)
    if not pending:
        return None

    id_match = re.search(r'\baction\s+(\d+)\b', q)
    if id_match:
        action_id = int(id_match.group(1))
        for action in pending:
            if int(action.get('id') or 0) == action_id:
                return action

    scored = []
    for action in pending:
        score = 0
        title = _normalize_text(action.get('title'))
        subject_key = _normalize_text(action.get('subject_key'))
        reason = _normalize_text(action.get('reason'))

        if brand_name and subject_key == _normalize_text(brand_name):
            score += 6
        if report and action.get('report_id') == report.get('id'):
            score += 2
        if title and title in q:
            score += 5
        if subject_key and subject_key in q:
            score += 4
        if 'stock' in q and 'stock' in title:
            score += 3
        if 'repeat' in q and 'repeat' in title:
            score += 3
        if 'decline' in q and 'decline' in title:
            score += 3
        if 'activity' in q and action.get('agent_type') == 'Activity Agent':
            score += 3
        if reason and any(word and word in q for word in reason.split()):
            score += 1
        scored.append((score, action))

    scored.sort(key=lambda item: (-(item[0]), -(item[1].get('id') or 0)))
    return scored[0][1] if scored and scored[0][0] > 0 else None


def _action_command_for_question(ds, question: str, brand_name=None, report=None):
    q = _normalize_text(question)
    desired_status = None
    if any(term in q for term in ('approve', 'accept', 'go ahead with')):
        desired_status = 'approved'
    elif any(term in q for term in ('reject', 'decline', 'dismiss', 'do not approve')):
        desired_status = 'rejected'

    if not desired_status:
        return None

    action = _match_pending_action(ds, question, brand_name=brand_name, report=report)
    if not action:
        return {
            'handled': True,
            'status': 'error',
            'message': "I could not match that request to a pending action.",
        }

    updated = ds.update_agent_action_status(
        action['id'],
        desired_status,
        actor='copilot',
        note=f"Requested from chat: {question[:180]}",
    )
    if not updated:
        return {
            'handled': True,
            'status': 'error',
            'message': "I found the action, but could not update it.",
        }

    return {
        'handled': True,
        'status': desired_status,
        'message': f"{updated['title']} has been {desired_status}.",
        'action': updated,
    }


def _build_brand_context(ds, report, brand_name):
    brand_name = ds.analytics_brand_name(brand_name)
    kpis = ds.get_brand_kpis_single(report['id'], brand_name) if report and brand_name else None
    history = ds.get_brand_history(brand_name, limit=18) if brand_name else []
    activity = ds.get_activity_brand_summary(brand_name) if brand_name else {}
    pending_actions = [
        action for action in ds.list_agent_actions(status='pending', limit=120)
        if ds.analytics_brand_name(action.get('subject_key')) == brand_name
    ][:6]
    forecast = {}
    growth_outlook = {}
    if brand_name:
        try:
            from .predictor import build_brand_forecasts, monthly_growth_outlook

            forecast = build_brand_forecasts({brand_name: list(reversed(history))}).get(brand_name, {})
            growth_outlook = monthly_growth_outlook(history)
        except Exception:
            forecast = {}
            growth_outlook = {}
    return {
        'kpis': kpis,
        'history': history,
        'activity': activity,
        'pending_actions': pending_actions,
        'forecast': forecast,
        'growth_outlook': growth_outlook,
    }


def _compose_context(ds, report=None, brand_name=None, retailer_code=None,
                     batch_id=None, page_context=None, question=None) -> dict:
    report = report or ds.get_latest_report()
    resolved_report, report_note = _find_report_from_question(ds, question or '', fallback_report=report)
    resolved_brand = _find_brand_from_question(ds, question or '', explicit_brand_name=brand_name)

    if not resolved_report:
        return {'report': None, 'page_context': page_context or {}}

    report = resolved_report
    report_id = report['id']
    all_kpis = ds.get_all_brand_kpis(report_id)
    top_brand = max(all_kpis, key=lambda row: row.get('total_revenue', 0), default=None)
    activity_summary = ds.get_activity_summary(
        batch_id=batch_id,
        report_id=report_id,
        brand_name=resolved_brand,
    )
    alerts = ds.get_alerts(report_id=report_id, unacknowledged_only=True)[:8]
    actions = ds.list_agent_actions(status='pending', limit=12)
    context = {
        'report': report,
        'report_note': report_note,
        'brand_name': resolved_brand,
        'retailer_code': retailer_code,
        'page_context': page_context or {},
        'question_focus': _focus_from_question(question or ''),
        'portfolio': {
            'brand_count': len(all_kpis),
            'total_revenue': round(sum(row.get('total_revenue', 0) for row in all_kpis), 2),
            'top_brand': top_brand['brand_name'] if top_brand else None,
            'top_brand_revenue': round(top_brand.get('total_revenue', 0), 2) if top_brand else 0,
            'top_brands': sorted(
                all_kpis,
                key=lambda row: float(row.get('total_revenue', 0) or 0),
                reverse=True
            )[:5],
            'at_risk_brands': sorted(
                [row for row in all_kpis if float(row.get('stock_days_cover') or 0) <= 7 or float(row.get('repeat_pct') or 0) < 30],
                key=lambda row: (float(row.get('stock_days_cover') or 999), float(row.get('repeat_pct') or 100))
            )[:5],
        },
        'activity': activity_summary,
        'pending_actions': actions,
        'alerts': alerts,
        'related_reports': ds.get_all_reports()[:8],
    }
    if resolved_brand:
        context['brand'] = _build_brand_context(ds, report, resolved_brand)
    if retailer_code:
        context['store'] = ds.get_store_activity_summary(retailer_code)
    if (page_context or {}).get('endpoint') == 'forecasting' or _question_has(question or '', 'forecast', 'outlook'):
        try:
            from .predictor import build_brand_forecasts

            all_brands = ds.get_all_brands_in_db()
            histories = {
                brand: list(reversed(ds.get_brand_history(brand, limit=36)))
                for brand in all_brands
            }
            forecasts = build_brand_forecasts(histories)
            forecast_rows = [
                {
                    'brand_name': brand,
                    'growth_label': row.get('growth_label'),
                    'pct_change': row.get('pct_change'),
                    'confidence_band': row.get('confidence_band'),
                }
                for brand, row in forecasts.items()
                if row.get('pct_change') is not None
            ]
            forecast_rows.sort(key=lambda row: row.get('pct_change', 0), reverse=True)
            context['forecasting'] = {
                'growing': forecast_rows[:5],
                'declining': list(reversed(forecast_rows[-5:])),
            }
        except Exception:
            context['forecasting'] = {'growing': [], 'declining': []}
    return context


def _build_portfolio_answer(context: dict) -> str:
    report = context.get('report') or {}
    portfolio = context.get('portfolio') or {}
    activity = (context.get('activity') or {}).get('totals', {})
    top_brands = portfolio.get('top_brands') or []
    at_risk = portfolio.get('at_risk_brands') or []
    lines = [
        f"{report.get('month_label') or 'Current report'} is carrying {_format_money(portfolio.get('total_revenue', 0))} across {int(portfolio.get('brand_count', 0))} active brands.",
    ]
    if top_brands:
        leaders = ', '.join(
            f"{row['brand_name']} ({_format_money(row.get('total_revenue', 0))})"
            for row in top_brands[:3]
        )
        lines.extend([
            "",
            "Top performers",
            f"- {leaders}",
        ])
    if at_risk:
        risks = '; '.join(
            f"{row['brand_name']} at {float(row.get('stock_days_cover') or 0):.1f} days cover"
            for row in at_risk[:3]
        )
        lines.extend([
            "",
            "Main pressure points",
            f"- {risks}",
            f"- Pending actions waiting for review: {len(context.get('pending_actions') or [])}",
            f"- Latest activity recorded {int(activity.get('visits', 0))} visits and {int(activity.get('issues', 0))} issues.",
        ])
    return '\n'.join(lines)


def _build_brand_answer(context: dict, question: str) -> str:
    brand_name = context.get('brand_name') or 'This brand'
    report = context.get('report') or {}
    brand = context.get('brand') or {}
    kpis = brand.get('kpis') or {}
    activity = brand.get('activity') or {}
    forecast = brand.get('forecast') or {}
    focus = context.get('question_focus')
    pending_actions = brand.get('pending_actions') or []
    issue_counts = activity.get('issue_counts') or []
    issue_summary = ', '.join(
        f"{row.get('issue_type')} ({row.get('count')})"
        for row in issue_counts[:2]
    ) or 'no major logged issues'

    lines = [
        f"{brand_name} in {report.get('month_label') or 'the selected report'} generated {_format_money(kpis.get('total_revenue', 0))} from {int(kpis.get('num_stores', 0) or 0)} stores.",
    ]

    if focus == 'stock':
        lines.extend([
            "",
            "Stock position",
            f"- Stock cover is {float(kpis.get('stock_days_cover') or 0):.1f} days and the status is {kpis.get('inv_health_status') or 'Unknown'}.",
            f"- Repeat rate is {_format_pct(kpis.get('repeat_pct', 0))}, so restocking alone will not solve the demand side.",
            f"- Top store in the selected report is {kpis.get('top_store_name') or 'not available'}.",
        ])
    elif focus == 'activity':
        latest_visit = activity.get('latest_visit') or {}
        issue_theme = ', '.join(
            f"{row.get('issue_type')} ({row.get('count')})"
            for row in issue_counts[:3]
        ) or 'No major field issues recorded'
        lines.extend([
            "",
            "Field read",
            f"- Brand mentions: {int(activity.get('mentions', 0))} across {int(activity.get('stores', 0))} store(s).",
            f"- Main themes: {issue_theme}.",
            f"- Latest visit: {latest_visit.get('retailer_name') or 'Not available'} on {latest_visit.get('activity_date') or 'N/A'}.",
        ])
    elif focus == 'forecast':
        next_rev = forecast.get('forecast')
        lines.extend([
            "",
            "Forward view",
            f"- Forecast direction: {forecast.get('growth_label') or 'N/A'}.",
            f"- Expected next monthly revenue: {_format_money(next_rev)}." if next_rev is not None else "- Expected next monthly revenue is not available yet.",
            f"- Confidence: {forecast.get('confidence_band') or 'N/A'}.",
        ])
    else:
        lines.extend([
            "",
            "What stands out",
            f"- Stock cover is {float(kpis.get('stock_days_cover') or 0):.1f} days.",
            f"- Repeat rate is {_format_pct(kpis.get('repeat_pct', 0))}.",
            f"- Performance grade is {kpis.get('perf_grade') or 'N/A'} with a score of {int(kpis.get('perf_score') or 0)}.",
            f"- Latest field presence: {int(activity.get('mentions', 0))} mentions with {issue_summary}.",
        ])

    if pending_actions:
        lines.extend([
            "",
            "Recommended next moves",
            *[f"- {action.get('title')}: {action.get('reason')}" for action in pending_actions[:3]],
        ])

    if _question_has(question, 'why', 'explain'):
        lines.extend([
            "",
            "Why this matters",
            "- A wide store footprint with tight stock cover can quickly turn into lost sales.",
            "- Very low repeat ordering means the brand may be getting reach without enough pull-through.",
        ])
    return '\n'.join(lines)


def _build_store_answer(context: dict) -> str:
    store_ctx = context.get('store') or {}
    store = store_ctx.get('store') or {}
    issues = store_ctx.get('issue_counts') or []
    brands = store_ctx.get('brand_counts') or []
    issue_summary = ', '.join(f"{row.get('issue_type')} ({row.get('count')})" for row in issues[:3]) or 'No repeated field issues'
    brand_summary = ', '.join(f"{row.get('brand_name')} ({row.get('mentions')})" for row in brands[:3]) or 'No repeated brand mentions'
    return '\n'.join([
        f"{store.get('retailer_name') or context.get('retailer_code')} has {int(store_ctx.get('visit_count', 0))} visit(s) recorded and {int(store_ctx.get('issue_count', 0))} issue(s).",
        "",
        "What stands out",
        f"- Main issues: {issue_summary}.",
        f"- Strongest brand presence: {brand_summary}.",
        f"- Latest activity date: {store_ctx.get('latest_activity_date') or 'N/A'}.",
    ])


def _build_activity_answer(context: dict) -> str:
    report = context.get('report') or {}
    activity = context.get('activity') or {}
    totals = activity.get('totals') or {}
    top_issues = activity.get('top_issues') or []
    top_people = activity.get('top_salespeople') or []
    issue_theme = ', '.join(
        f"{row.get('issue_type')} ({row.get('count')})"
        for row in top_issues[:3]
    ) or 'None'
    rep_theme = ', '.join(
        f"{row.get('salesman_name')} ({row.get('visits')} visits)"
        for row in top_people[:3]
    ) or 'None'
    return '\n'.join([
        f"Activity for {report.get('month_label') or 'the selected report'} recorded {int(totals.get('visits', 0))} visits across {int(totals.get('stores', 0))} stores.",
        "",
        "Field picture",
        f"- Logged issues: {int(totals.get('issues', 0))}.",
        f"- Opportunities captured: {int(totals.get('opportunities', 0))}.",
        f"- Top issue themes: {issue_theme}.",
        f"- Most active reps: {rep_theme}.",
    ])


def _build_forecast_answer(context: dict) -> str:
    report = context.get('report') or {}
    forecast_ctx = context.get('forecasting') or {}
    growing = ', '.join(
        f"{row['brand_name']} ({float(row.get('pct_change') or 0):.1f}%)"
        for row in forecast_ctx.get('growing', [])[:3]
    ) or 'No strong upside signal yet'
    declining = ', '.join(
        f"{row['brand_name']} ({float(row.get('pct_change') or 0):.1f}%)"
        for row in forecast_ctx.get('declining', [])[:3]
    ) or 'No major decline flags'
    return '\n'.join([
        f"Forecast review for {report.get('month_label') or 'the selected report'}.",
        "",
        "Momentum",
        f"- Strongest upside: {growing}.",
        f"- Weakest outlook: {declining}.",
        f"- Pending forecast-related reviews: {sum(1 for action in context.get('pending_actions', []) if action.get('agent_type') == 'Forecast Agent')}.",
    ])


def _build_actions_answer(context: dict) -> str:
    actions = context.get('pending_actions') or []
    if not actions:
        return "There are no pending actions waiting for review right now."
    return '\n'.join([
        f"There are {len(actions)} pending actions waiting for review.",
        "",
        "Highest priority",
        *[f"- {action.get('title')}: {action.get('reason')}" for action in actions[:5]],
    ])


def _build_deterministic_answer(question: str, context: dict) -> str:
    if not context.get('report'):
        return 'No report data is available yet.'
    if context.get('store'):
        return _build_store_answer(context)
    if context.get('brand') and context.get('brand_name'):
        return _build_brand_answer(context, question)
    focus = context.get('question_focus')
    if focus == 'activity':
        return _build_activity_answer(context)
    if focus == 'forecast':
        return _build_forecast_answer(context)
    if focus == 'actions':
        return _build_actions_answer(context)
    return _build_portfolio_answer(context)


def _tool_response(status='success', message='', artifacts=None, next_steps=None, errors=None, data=None, state=None):
    return {
        'status': status,
        'message': message,
        'artifacts': artifacts or [],
        'next_steps': next_steps or [],
        'errors': errors or [],
        'data': data or {},
        'state': state or (
            'failed' if status == 'error'
            else 'waiting' if status in {'needs_confirmation', 'waiting'}
            else 'completed'
        ),
    }


def _get_tool_policy(tool_name: str) -> str:
    destructive = {
        'start_drive_full_import',
        'mass_send_reports',
        'delete_report',
    }
    operational = {
        'approve_action',
        'reject_action',
        'acknowledge_alert',
        'set_target',
        'update_brand_contact',
        'run_connector',
        'create_schedule',
        'pause_schedule',
        'resume_schedule',
        'run_schedule_now',
    }
    if tool_name in destructive:
        return 'destructive_or_bulk'
    if tool_name in operational:
        return 'operational_write'
    return 'safe_read'


def _tool_registry():
    return {
        'open_dashboard': {'category': 'navigation'},
        'open_report': {'category': 'navigation'},
        'open_brand': {'category': 'navigation'},
        'open_store': {'category': 'navigation'},
        'open_page': {'category': 'navigation'},
        'summarize_report': {'category': 'analysis'},
        'summarize_brand': {'category': 'analysis'},
        'summarize_activity': {'category': 'analysis'},
        'summarize_forecast': {'category': 'analysis'},
        'list_actions': {'category': 'analysis'},
        'search_memory': {'category': 'analysis'},
        'list_memory': {'category': 'analysis'},
        'pin_memory': {'category': 'analysis'},
        'approve_action': {'category': 'system_actions'},
        'reject_action': {'category': 'system_actions'},
        'acknowledge_alert': {'category': 'system_actions'},
        'set_target': {'category': 'system_actions'},
        'update_brand_contact': {'category': 'system_actions'},
        'delete_report': {'category': 'system_actions'},
        'run_connector': {'category': 'connectors'},
        'list_connectors': {'category': 'connectors'},
        'create_schedule': {'category': 'scheduling'},
        'list_schedules': {'category': 'scheduling'},
        'pause_schedule': {'category': 'scheduling'},
        'resume_schedule': {'category': 'scheduling'},
        'run_schedule_now': {'category': 'scheduling'},
        'queue_workflow_template': {'category': 'scheduling'},
        'start_drive_full_import': {'category': 'connectors'},
    }


def _tool_category(tool_name: str) -> str:
    return _tool_registry().get(tool_name, {}).get('category', 'analysis')


def _resolve_memory_scope(context: dict):
    if context.get('brand_name'):
        return 'brand', context['brand_name']
    if context.get('retailer_code'):
        return 'store', context['retailer_code']
    if (context.get('report') or {}).get('id'):
        return 'report', str(context['report']['id'])
    return 'system', 'global'


def _memory_tags_for_context(context: dict, question: str):
    tags = ['copilot']
    if context.get('brand_name'):
        tags.append(_normalize_text(context['brand_name']).replace(' ', '_'))
    if context.get('question_focus'):
        tags.append(context['question_focus'])
    for token in _normalize_text(question).split()[:4]:
        if token and token not in tags:
            tags.append(token)
    return tags[:8]


def _connector_catalog(ds, context: dict):
    catalog = []
    try:
        from .drive_sync import drive_available
        drive_ok = drive_available()
    except Exception:
        drive_ok = False
    catalog.append({
        'connector': 'drive',
        'label': 'Google Drive',
        'available': drive_ok,
        'status': 'ready' if drive_ok else 'not_configured',
        'actions': ['status', 'check'],
    })

    try:
        from .sheets import sheets_available
        sheets_ok = sheets_available()
    except Exception:
        sheets_ok = False
    catalog.append({
        'connector': 'sheets',
        'label': 'Google Sheets',
        'available': sheets_ok,
        'status': 'ready' if sheets_ok else 'not_configured',
        'actions': ['status', 'sync_brand'],
    })

    try:
        from .delivery import smtp_configured
        email_ok = smtp_configured()
    except Exception:
        email_ok = False
    catalog.append({
        'connector': 'email',
        'label': 'Email',
        'available': email_ok,
        'status': 'ready' if email_ok else 'not_configured',
        'actions': ['send'],
    })

    try:
        from .delivery import twilio_configured
        whatsapp_ok = twilio_configured()
    except Exception:
        whatsapp_ok = False
    catalog.append({
        'connector': 'whatsapp',
        'label': 'WhatsApp',
        'available': whatsapp_ok,
        'status': 'ready' if whatsapp_ok else 'not_configured',
        'actions': ['send'],
    })

    catalog.append({
        'connector': 'webhook',
        'label': 'Outbound Webhook',
        'available': True,
        'status': 'ready',
        'actions': ['send'],
    })
    return catalog


def _navigate(tool_name: str, arguments: dict, context: dict):
    report_id = arguments.get('report_id') or (context.get('report') or {}).get('id')
    if tool_name == 'open_dashboard':
        url = f"/dashboard{f'?report_id={report_id}' if report_id else ''}"
    elif tool_name == 'open_report':
        url = f"/dashboard?report_id={report_id}" if report_id else '/dashboard'
    elif tool_name == 'open_brand':
        brand_name = arguments.get('brand_name') or context.get('brand_name')
        if not brand_name:
            return _tool_response(status='error', message='brand_name is required for open_brand')
        suffix = f"?report_id={report_id}" if report_id else ''
        url = f"/brand/{brand_name}{suffix}"
    elif tool_name == 'open_store':
        retailer_code = arguments.get('retailer_code') or context.get('retailer_code')
        if not retailer_code:
            return _tool_response(status='error', message='retailer_code is required for open_store')
        url = f"/store-360/{retailer_code}"
    else:
        page = arguments.get('page') or 'copilot'
        path_map = {
            'activity_intelligence': '/activity-intelligence',
            'forecasting': '/forecasting',
            'history': '/history',
            'catalog': '/catalog',
            'agent_actions_page': '/agent-actions',
            'database_page': '/database',
            'copilot': '/copilot',
            'leaderboard': '/leaderboard',
        }
        base = path_map.get(page, '/copilot')
        if report_id and page in {'activity_intelligence', 'dashboard'}:
            url = f"{base}?report_id={report_id}"
        else:
            url = base
    return _tool_response(
        status='success',
        message=f"Navigation ready: {url}",
        artifacts=[{'type': 'url', 'value': url}],
    )


def _run_connector(ds, connector: str, action: str, arguments: dict, context: dict):
    connector = str(connector or '').lower().strip()
    action = str(action or 'status').lower().strip()

    if connector == 'drive':
        from .drive_sync import drive_available, DriveSyncOrchestrator
        if action == 'status':
            return _tool_response(message='Drive status loaded.', data={'available': drive_available()})
        if action == 'check':
            if not drive_available():
                return _tool_response(status='error', message='Google Drive is not configured.')
            orch = DriveSyncOrchestrator()
            results = orch.check_new_files()
            imported = sum(1 for row in results if row.get('status') == 'success')
            skipped = sum(1 for row in results if row.get('status') == 'skipped')
            errors = sum(1 for row in results if row.get('status') == 'error')
            return _tool_response(
                message='Drive check completed.',
                data={'imported': imported, 'skipped': skipped, 'errors': errors, 'results': results[:20]},
            )
        return _tool_response(status='error', message=f'Unsupported drive action: {action}')

    if connector == 'sheets':
        from .sheets import sheets_available, push_brand_to_sheets
        import pandas as pd

        if action == 'status':
            return _tool_response(message='Sheets status loaded.', data={'available': sheets_available()})
        if action == 'sync_brand':
            if not sheets_available():
                return _tool_response(status='error', message='Google Sheets is not configured.')
            report = context.get('report') or ds.get_latest_report()
            if not report:
                return _tool_response(status='error', message='No report available for Sheets sync.')
            brand_name = arguments.get('brand_name') or context.get('brand_name')
            if not brand_name:
                return _tool_response(status='error', message='brand_name is required for sync_brand.')
            daily_rows = ds.get_daily_sales(report['id'], brand_name)
            if not daily_rows:
                return _tool_response(status='error', message=f'No daily rows found for {brand_name}.')
            frame = pd.DataFrame([
                {
                    'Date': row.get('date'),
                    'Brand Partner': brand_name,
                    'SKUs': '',
                    'Particulars': '',
                    'Vch Type': 'System Export',
                    'Vch No.': '',
                    'Quantity': row.get('qty') or 0,
                    'Sales_Value': row.get('revenue') or 0,
                }
                for row in daily_rows
            ])
            url = push_brand_to_sheets(brand_name, frame, report['start_date'], report['end_date'])
            return _tool_response(
                message=f'{brand_name} synced to Sheets.',
                artifacts=[{'type': 'url', 'value': url}],
                data={'sheet_url': url, 'rows': len(frame)},
            )
        return _tool_response(status='error', message=f'Unsupported sheets action: {action}')

    if connector in {'email', 'whatsapp'}:
        from .delivery import send_bulk_reports, send_bulk_whatsapp, smtp_configured, twilio_configured

        report = context.get('report') or ds.get_latest_report()
        if not report:
            return _tool_response(status='error', message='No report available for outbound send.')
        tokens = ds.get_all_tokens()
        if connector == 'email':
            if not smtp_configured():
                return _tool_response(status='error', message='SMTP is not configured.')
            result = send_bulk_reports(tokens, report.get('month_label'), ds=ds)
            ok = sum(1 for row in result if row.get('success'))
            return _tool_response(message='Email send completed.', data={'sent': ok, 'results': result[:30]})
        if not twilio_configured():
            return _tool_response(status='error', message='Twilio is not configured.')
        result = send_bulk_whatsapp(tokens, report.get('month_label'), ds=ds)
        ok = sum(1 for row in result if row.get('success'))
        return _tool_response(message='WhatsApp send completed.', data={'sent': ok, 'results': result[:30]})

    if connector == 'webhook':
        from urllib.request import Request, urlopen
        from urllib.error import URLError, HTTPError

        webhook_url = str(arguments.get('url') or '').strip()
        if not webhook_url:
            return _tool_response(status='error', message='Webhook URL is required.')
        payload = arguments.get('payload') or {
            'event': 'dala.super_agent.manual_run',
            'timestamp': datetime.now().isoformat(timespec='seconds'),
            'context': {
                'report_id': (context.get('report') or {}).get('id'),
                'brand_name': context.get('brand_name'),
                'retailer_code': context.get('retailer_code'),
            },
        }
        body = json.dumps(payload).encode('utf-8')
        req = Request(webhook_url, data=body, headers={'Content-Type': 'application/json'}, method='POST')
        try:
            with urlopen(req, timeout=20) as resp:
                status_code = getattr(resp, 'status', 200)
            return _tool_response(message='Webhook sent.', data={'status_code': status_code})
        except HTTPError as exc:
            return _tool_response(status='error', message='Webhook failed.', errors=[f'HTTP {exc.code}: {exc.reason}'])
        except URLError as exc:
            return _tool_response(status='error', message='Webhook failed.', errors=[str(exc.reason)])

    return _tool_response(status='error', message=f'Unsupported connector: {connector}')


def execute_admin_tool(ds, tool_name: str, arguments=None, context=None,
                       confirmation_token=None, idempotency_key=None,
                       operator_mode=True):
    arguments = arguments or {}
    context = context or {}
    policy_class = _get_tool_policy(tool_name)
    category = _tool_category(tool_name)
    if tool_name not in _tool_registry():
        return {
            'tool_name': tool_name,
            'policy_class': policy_class,
            'category': category,
            'idempotency_key': idempotency_key,
            **_tool_response(status='error', message=f'Unsupported tool: {tool_name}'),
        }
    if policy_class == 'operational_write' and not operator_mode:
        return {
            'tool_name': tool_name,
            'policy_class': policy_class,
            'category': category,
            'idempotency_key': idempotency_key,
            **_tool_response(status='needs_confirmation', message='Operator mode is required for this action.', state='waiting'),
        }
    if policy_class == 'destructive_or_bulk' and str(confirmation_token or '').strip().lower() != 'confirm':
        return {
            'tool_name': tool_name,
            'policy_class': policy_class,
            'category': category,
            'idempotency_key': idempotency_key,
            **_tool_response(
                status='needs_confirmation',
                message='This action is destructive or bulk. Re-run with confirmation_token="confirm".',
                state='waiting',
            ),
        }

    if policy_class != 'safe_read':
        idempotency_key = idempotency_key or hashlib.sha256(
            json.dumps(
                {
                    'tool_name': tool_name,
                    'arguments': arguments,
                    'report_id': (context.get('report') or {}).get('id'),
                    'brand_name': context.get('brand_name'),
                    'retailer_code': context.get('retailer_code'),
                },
                sort_keys=True,
                default=str,
            ).encode('utf-8')
        ).hexdigest()[:24]
        existing = ds.get_tool_execution(idempotency_key)
        if existing:
            replay = existing.get('result') or {}
            replay['idempotent_replay'] = True
            return {
                'tool_name': tool_name,
                'policy_class': policy_class,
                'category': category,
                'idempotency_key': idempotency_key,
                **replay,
            }

    report = context.get('report') or ds.get_latest_report()
    if tool_name in {'open_dashboard', 'open_report', 'open_brand', 'open_store', 'open_page'}:
        result = _navigate(tool_name, arguments, context)
    elif tool_name == 'summarize_report':
        if not report:
            result = _tool_response(status='error', message='No report available.')
        else:
            all_kpis = ds.get_all_brand_kpis(report['id'])
            top = all_kpis[0] if all_kpis else {}
            result = _tool_response(
                message='Report summary ready.',
                data={
                    'report_id': report['id'],
                    'month_label': report.get('month_label'),
                    'brand_count': len(all_kpis),
                    'total_revenue': round(sum(float(row.get('total_revenue', 0) or 0) for row in all_kpis), 2),
                    'top_brand': top.get('brand_name'),
                    'top_brand_revenue': float(top.get('total_revenue', 0) or 0),
                },
            )
    elif tool_name == 'summarize_brand':
        brand_name = arguments.get('brand_name') or context.get('brand_name')
        if not brand_name or not report:
            result = _tool_response(status='error', message='brand_name and active report are required.')
        else:
            kpis = ds.get_brand_kpis_single(report['id'], brand_name)
            activity = ds.get_activity_brand_summary(brand_name)
            result = _tool_response(message='Brand summary ready.', data={'brand_name': brand_name, 'kpis': kpis or {}, 'activity': activity or {}})
    elif tool_name == 'summarize_activity':
        result = _tool_response(
            message='Activity summary ready.',
            data=ds.get_activity_summary(
                batch_id=context.get('batch_id'),
                report_id=(report or {}).get('id'),
                brand_name=context.get('brand_name'),
            ),
        )
    elif tool_name == 'summarize_forecast':
        result = _tool_response(message='Forecast summary ready.', data=context.get('forecasting') or {})
    elif tool_name == 'list_actions':
        result = _tool_response(message='Pending actions loaded.', data={'actions': ds.list_agent_actions(status=arguments.get('status', 'pending'), limit=int(arguments.get('limit') or 25))})
    elif tool_name == 'search_memory':
        result = _tool_response(
            message='Memory results loaded.',
            data={
                'memories': ds.search_agent_memories(
                    arguments.get('query') or '',
                    limit=int(arguments.get('limit') or 8),
                    subject_type=arguments.get('subject_type'),
                    subject_key=arguments.get('subject_key'),
                    tags=arguments.get('tags') or [],
                ),
            },
        )
    elif tool_name == 'list_memory':
        result = _tool_response(
            message='Memory list loaded.',
            data={
                'memories': ds.list_agent_memories(
                    limit=int(arguments.get('limit') or 25),
                    memory_layer=arguments.get('memory_layer'),
                    pinned=arguments.get('pinned'),
                    subject_type=arguments.get('subject_type'),
                    subject_key=arguments.get('subject_key'),
                    query=arguments.get('query'),
                )
            },
        )
    elif tool_name == 'pin_memory':
        memory_id = int(arguments.get('memory_id') or 0)
        if not memory_id:
            result = _tool_response(status='error', message='memory_id is required.')
        else:
            result = _tool_response(message='Memory pin updated.', data={'memory': ds.pin_agent_memory(memory_id, pinned=bool(arguments.get('pinned', True)))})
    elif tool_name in {'approve_action', 'reject_action'}:
        action_id = int(arguments.get('action_id') or 0)
        if not action_id:
            result = _tool_response(status='error', message='action_id is required.')
        else:
            status = 'approved' if tool_name == 'approve_action' else 'rejected'
            updated = ds.update_agent_action_status(action_id, status, actor='copilot', note=arguments.get('note'))
            if not updated:
                result = _tool_response(status='error', message='Action not found.')
            else:
                if updated.get('subject_type') == 'brand':
                    ds.save_recommendation_outcome(
                        updated.get('subject_key'),
                        tool_name,
                        'approved' if status == 'approved' else 'rejected',
                        outcome_value=1 if status == 'approved' else -1,
                        note=arguments.get('note'),
                        report_id=updated.get('report_id'),
                    )
                result = _tool_response(message=f"Action {action_id} {status}.", data={'action': updated})
    elif tool_name == 'acknowledge_alert':
        alert_id = int(arguments.get('alert_id') or 0)
        if not alert_id:
            result = _tool_response(status='error', message='alert_id is required.')
        else:
            ds.acknowledge_alert(alert_id)
            result = _tool_response(message=f'Alert {alert_id} acknowledged.')
    elif tool_name == 'set_target':
        brand_name = arguments.get('brand_name') or context.get('brand_name')
        target_revenue = arguments.get('target_revenue')
        month_label = arguments.get('month_label') or ((report or {}).get('month_label'))
        if not brand_name or target_revenue is None or not month_label:
            result = _tool_response(status='error', message='brand_name, target_revenue, and month_label are required.')
        else:
            ds.set_target(brand_name, month_label, target_revenue=float(target_revenue))
            result = _tool_response(message=f'Target set for {brand_name} ({month_label}).')
    elif tool_name == 'update_brand_contact':
        brand_name = arguments.get('brand_name') or context.get('brand_name')
        if not brand_name:
            result = _tool_response(status='error', message='brand_name is required.')
        else:
            ds.get_or_create_token(brand_name)
            ds.update_brand_contact(
                brand_name,
                email=(arguments.get('email') or '').strip() or None,
                whatsapp=(arguments.get('whatsapp') or '').strip() or None,
            )
            result = _tool_response(message=f'Contact updated for {brand_name}.')
    elif tool_name == 'delete_report':
        report_id = int(arguments.get('report_id') or (report or {}).get('id') or 0)
        if not report_id:
            result = _tool_response(status='error', message='report_id is required.')
        elif not ds.delete_report(report_id):
            result = _tool_response(status='error', message='Report not found.')
        else:
            result = _tool_response(message=f'Report {report_id} deleted.')
    elif tool_name == 'list_connectors':
        result = _tool_response(message='Connector statuses loaded.', data={'connectors': _connector_catalog(ds, context)})
    elif tool_name == 'run_connector':
        connector = arguments.get('connector')
        default_action = 'status' if connector in {'drive', 'sheets'} else 'send'
        result = _run_connector(ds, connector, arguments.get('action', default_action), arguments, context)
    elif tool_name == 'create_schedule':
        job_id = ds.create_assistant_job(
            job_type=arguments.get('job_type') or ('connector' if arguments.get('connector') else 'assistant'),
            target=arguments.get('target'),
            payload=arguments.get('payload') or {},
            cadence=arguments.get('cadence') or 'manual',
            label=arguments.get('label'),
            connector=arguments.get('connector'),
            next_run=arguments.get('next_run'),
            status=str(arguments.get('status') or 'active').strip().lower(),
        )
        result = _tool_response(message='Schedule created.', data={'job': ds.get_assistant_job(job_id)})
    elif tool_name == 'list_schedules':
        result = _tool_response(message='Schedules loaded.', data={'jobs': ds.list_assistant_jobs(status=arguments.get('status', 'all'), limit=int(arguments.get('limit') or 25))})
    elif tool_name == 'pause_schedule':
        schedule_id = int(arguments.get('schedule_id') or 0)
        result = _tool_response(status='error', message='schedule_id is required.') if not schedule_id else _tool_response(message='Schedule paused.', data={'job': ds.pause_assistant_job(schedule_id)})
    elif tool_name == 'resume_schedule':
        schedule_id = int(arguments.get('schedule_id') or 0)
        result = _tool_response(status='error', message='schedule_id is required.') if not schedule_id else _tool_response(message='Schedule resumed.', data={'job': ds.resume_assistant_job(schedule_id)})
    elif tool_name == 'run_schedule_now':
        schedule_id = int(arguments.get('schedule_id') or 0)
        if not schedule_id:
            result = _tool_response(status='error', message='schedule_id is required.')
        else:
            job = ds.get_assistant_job(schedule_id)
            if not job:
                result = _tool_response(status='error', message='Schedule not found.')
            elif job.get('job_type') == 'connector' and job.get('connector'):
                payload = job.get('payload') or {}
                connector_run = _run_connector(
                    ds,
                    job.get('connector'),
                    payload.get('action', 'status' if job.get('connector') in {'drive', 'sheets'} else 'send'),
                    payload,
                    context,
                )
                ds.record_assistant_job_run(schedule_id, result=connector_run.get('data') or {'status': connector_run.get('status')})
                result = _tool_response(
                    status=connector_run.get('status'),
                    message=f"Schedule executed now. {connector_run.get('message')}",
                    artifacts=connector_run.get('artifacts'),
                    next_steps=connector_run.get('next_steps'),
                    errors=connector_run.get('errors'),
                    data={'job': ds.get_assistant_job(schedule_id), 'connector_result': connector_run},
                    state=connector_run.get('state'),
                )
            else:
                ds.record_assistant_job_run(schedule_id, result={'manual_run': True})
                result = _tool_response(message='Schedule marked as run.', data={'job': ds.get_assistant_job(schedule_id)})
    elif tool_name == 'queue_workflow_template':
        template = arguments.get('template') or 'general_follow_up'
        job_id = ds.create_assistant_job(
            job_type='workflow',
            target=template,
            payload=arguments.get('payload') or {},
            cadence='manual',
            label=f'{template} workflow',
        )
        result = _tool_response(message='Workflow queued.', data={'job': ds.get_assistant_job(job_id)})
    elif tool_name == 'start_drive_full_import':
        result = _tool_response(
            message='Use /api/drive-sync/full-import to start a full import in background.',
            next_steps=['Call POST /api/drive-sync/full-import and poll the returned job id.'],
        )
    else:
        result = _tool_response(status='error', message=f'Unsupported tool: {tool_name}')

    if policy_class != 'safe_read' and idempotency_key:
        ds.save_tool_execution(idempotency_key, tool_name, arguments=arguments, result=result, status=result.get('status'))
        scope_type, scope_key = _resolve_memory_scope(context)
        ds.save_agent_memory(
            scope_type=scope_type,
            scope_key=scope_key,
            memory_text=f"{tool_name}: {result.get('message')}",
            memory_kind='tool_execution',
            confidence=0.78 if result.get('status') == 'success' else 0.44,
            source='copilot_execute',
            memory_layer='operational',
            tags=['copilot', 'execution', category, tool_name],
            related_report_id=(context.get('report') or {}).get('id'),
            related_brand=context.get('brand_name'),
            metadata={
                'tool_name': tool_name,
                'status': result.get('status'),
                'idempotency_key': idempotency_key,
                'arguments': arguments,
            },
        )

    return {
        'tool_name': tool_name,
        'policy_class': policy_class,
        'category': category,
        'idempotency_key': idempotency_key,
        **result,
    }


def _guess_tool_from_question(question: str, context: dict):
    q = _normalize_text(question)
    if any(term in q for term in ('go to', 'open', 'take me', 'navigate', 'show me')):
        if 'store' in q:
            return 'open_store'
        if 'brand' in q:
            return 'open_brand'
        if 'activity' in q:
            return 'open_page'
        if 'forecast' in q:
            return 'open_page'
        if 'history' in q:
            return 'open_page'
        if 'report' in q or 'dashboard' in q:
            return 'open_report'
        return 'open_dashboard'
    if any(term in q for term in ('approve', 'accept')):
        return 'approve_action'
    if any(term in q for term in ('reject', 'decline', 'dismiss')):
        return 'reject_action'
    if any(term in q for term in ('memory', 'remember', 'recall', 'pinned')):
        return 'search_memory'
    if any(term in q for term in ('schedule', 'cadence', 'run now', 'pause job', 'resume job')):
        if any(term in q for term in ('run now', 'run immediately')):
            return 'run_schedule_now'
        if 'pause' in q:
            return 'pause_schedule'
        if 'resume' in q:
            return 'resume_schedule'
        if any(term in q for term in ('create', 'set up', 'schedule')):
            return 'create_schedule'
        return 'list_schedules'
    if any(term in q for term in ('sync', 'connector', 'drive', 'sheets', 'whatsapp', 'email', 'webhook')):
        return 'run_connector'
    if any(term in q for term in ('summary', 'summarize', 'analyse', 'analyze')):
        if context.get('brand_name'):
            return 'summarize_brand'
        return 'summarize_report'
    return None


def _planned_steps_from_question(question: str, context: dict, selected_tool=None):
    steps = [
        {'step': 'Understand the request', 'status': 'completed'},
        {'step': 'Resolve context and memory', 'status': 'completed'},
        {'step': 'Choose best tools for this request', 'status': 'completed' if selected_tool else 'pending'},
        {'step': 'Execute allowed tools', 'status': 'pending'},
        {'step': 'Summarize findings and next actions', 'status': 'pending'},
    ]
    if selected_tool:
        steps[3]['status'] = 'in_progress'
    return steps


def _suggested_actions_for_ui(context: dict):
    suggestions = []
    report = context.get('report') or {}
    brand_name = context.get('brand_name')
    if brand_name:
        suggestions.append({'kind': 'open_brand', 'label': f'Open {brand_name}', 'brand_name': brand_name, 'report_id': report.get('id'), 'verb': 'Open'})
        suggestions.append({'kind': 'summarize_brand', 'label': f'Summarize {brand_name}', 'brand_name': brand_name, 'verb': 'Run'})
    if report.get('id'):
        suggestions.append({'kind': 'open_report', 'label': f"Open {report.get('month_label')}", 'report_id': report.get('id'), 'verb': 'Open'})
    suggestions.append({'kind': 'open_page', 'label': 'Open Forecasting', 'page': 'forecasting', 'verb': 'Open'})
    suggestions.append({'kind': 'open_page', 'label': 'Open Activity', 'page': 'activity_intelligence', 'verb': 'Open'})
    return suggestions[:5]


def _detect_high_risk_states(context: dict):
    risks = []
    portfolio = context.get('portfolio') or {}
    activity = context.get('activity') or {}
    alerts = context.get('alerts') or []
    brand = context.get('brand') or {}

    for row in (portfolio.get('at_risk_brands') or [])[:3]:
        risks.append({
            'risk_type': 'stock_risk',
            'subject_type': 'brand',
            'subject_key': row.get('brand_name'),
            'message': f"{row.get('brand_name')} is down to {float(row.get('stock_days_cover') or 0):.1f} days cover.",
            'priority': 'high' if float(row.get('stock_days_cover') or 999) <= 5 else 'medium',
        })

    if (activity.get('totals') or {}).get('issues', 0) >= 5:
        risks.append({
            'risk_type': 'activity_issue_loop',
            'subject_type': 'activity_batch',
            'subject_key': str((context.get('report') or {}).get('id') or 'latest'),
            'message': f"{int(activity['totals'].get('issues', 0))} field issues are waiting for closure review.",
            'priority': 'high',
        })

    if brand and (brand.get('kpis') or {}).get('repeat_pct') is not None:
        repeat_pct = float((brand.get('kpis') or {}).get('repeat_pct') or 0)
        if repeat_pct < 30:
            risks.append({
                'risk_type': 'repeat_order_drop',
                'subject_type': 'brand',
                'subject_key': context.get('brand_name'),
                'message': f"Repeat ordering is only {repeat_pct:.1f}% for {context.get('brand_name')}.",
                'priority': 'medium',
            })

    for alert in alerts[:2]:
        risks.append({
            'risk_type': 'active_alert',
            'subject_type': 'brand' if alert.get('brand_name') else 'report',
            'subject_key': alert.get('brand_name') or str((context.get('report') or {}).get('id') or 'latest'),
            'message': alert.get('message'),
            'priority': alert.get('severity') or 'medium',
        })
    return risks[:6]


def _workflow_templates(context: dict, risks=None):
    risks = risks or _detect_high_risk_states(context)
    templates = []
    if any(risk.get('risk_type') == 'stock_risk' for risk in risks):
        templates.append({
            'template': 'stock_risk_recovery',
            'label': 'Stock Risk Recovery',
            'description': 'Queue follow-up actions for low-cover brands and sync the latest risk context.',
        })
    if any(risk.get('risk_type') == 'activity_issue_loop' for risk in risks):
        templates.append({
            'template': 'activity_issue_closure',
            'label': 'Activity Issue Closure Loop',
            'description': 'Track unresolved field issues and keep the batch in focus until issues drop.',
        })
    if context.get('brand_name'):
        templates.append({
            'template': 'target_catch_up_plan',
            'label': 'Target Catch-up Plan',
            'description': 'Bundle brand context, recent performance, and next actions into a recovery plan.',
        })
    return templates[:3]


def _rank_suggested_actions(ds, context: dict, suggestions):
    scores = ds.get_recommendation_outcome_scores(brand_name=context.get('brand_name'))
    ranked = []
    for action in suggestions:
        recommendation = scores.get(action.get('kind'), {})
        enriched = dict(action)
        enriched['outcome_score'] = float(recommendation.get('weighted_score') or 0)
        enriched['outcome_events'] = int(recommendation.get('total_events') or 0)
        ranked.append(enriched)
    ranked.sort(key=lambda item: (item.get('outcome_score', 0), item.get('label', '')), reverse=True)
    return ranked


def _memory_refs(memories):
    return [
        {
            'id': row.get('id'),
            'scope_type': row.get('scope_type'),
            'scope_key': row.get('scope_key'),
            'subject_type': row.get('subject_type'),
            'subject_key': row.get('subject_key'),
            'memory_kind': row.get('memory_kind'),
            'source': row.get('source'),
            'confidence': row.get('confidence'),
            'updated_at': row.get('updated_at'),
            'excerpt': str(row.get('memory_text') or '')[:180],
            'pinned': bool(row.get('pinned')),
        }
        for row in memories[:5]
    ]


def _build_tool_plan(ds, question: str, context: dict):
    q = _normalize_text(question)
    selected_tool = _guess_tool_from_question(question, context)
    plan = []

    if not selected_tool:
        if context.get('brand_name'):
            selected_tool = 'summarize_brand'
        elif 'activity' in q:
            selected_tool = 'summarize_activity'
        elif 'forecast' in q:
            selected_tool = 'summarize_forecast'
        else:
            selected_tool = 'summarize_report'

    if selected_tool == 'open_page':
        page = 'dashboard'
        if 'forecast' in q:
            page = 'forecasting'
        elif 'activity' in q:
            page = 'activity_intelligence'
        elif 'history' in q:
            page = 'history'
        elif 'catalog' in q:
            page = 'catalog'
        plan.append({'tool_name': 'open_page', 'arguments': {'page': page}, 'auto_execute': True})
    elif selected_tool == 'open_brand':
        plan.append({'tool_name': 'open_brand', 'arguments': {'brand_name': context.get('brand_name')}, 'auto_execute': True})
    elif selected_tool == 'open_store':
        plan.append({'tool_name': 'open_store', 'arguments': {'retailer_code': context.get('retailer_code')}, 'auto_execute': True})
    elif selected_tool in {'open_dashboard', 'open_report'}:
        plan.append({'tool_name': selected_tool, 'arguments': {}, 'auto_execute': True})
    elif selected_tool in {'approve_action', 'reject_action'}:
        matched_action = _match_pending_action(ds, question, brand_name=context.get('brand_name'), report=context.get('report'))
        plan.append({
            'tool_name': selected_tool,
            'arguments': {'action_id': (matched_action or {}).get('id'), 'note': question[:180]},
            'auto_execute': True,
        })
    elif selected_tool == 'run_connector':
        connector = 'drive'
        action = 'check'
        if 'sheet' in q:
            connector = 'sheets'
            action = 'sync_brand' if context.get('brand_name') else 'status'
        elif 'email' in q:
            connector = 'email'
            action = 'send'
        elif 'whatsapp' in q:
            connector = 'whatsapp'
            action = 'send'
        elif 'webhook' in q:
            connector = 'webhook'
            action = 'send'
        plan.append({
            'tool_name': 'run_connector',
            'arguments': {'connector': connector, 'action': action, 'brand_name': context.get('brand_name')},
            'auto_execute': connector != 'webhook',
        })
    elif selected_tool == 'create_schedule':
        connector = None
        if 'drive' in q:
            connector = 'drive'
        elif 'sheet' in q:
            connector = 'sheets'
        elif 'email' in q:
            connector = 'email'
        elif 'whatsapp' in q:
            connector = 'whatsapp'
        elif 'webhook' in q:
            connector = 'webhook'
        plan.append({
            'tool_name': 'create_schedule',
            'arguments': {
                'label': f"Copilot {connector or 'assistant'} job",
                'job_type': 'connector' if connector else 'assistant',
                'connector': connector,
                'target': context.get('brand_name') or (context.get('report') or {}).get('month_label'),
                'payload': {'action': 'check' if connector in {'drive', 'sheets'} else 'send'},
                'cadence': 'daily' if 'daily' in q else 'weekly' if 'weekly' in q else 'manual',
            },
            'auto_execute': True,
        })
    elif selected_tool in {'pause_schedule', 'resume_schedule', 'run_schedule_now'}:
        match = re.search(r'\b(?:job|schedule)\s+(\d+)\b', q)
        plan.append({'tool_name': selected_tool, 'arguments': {'schedule_id': int(match.group(1)) if match else 0}, 'auto_execute': True})
    elif selected_tool == 'list_schedules':
        plan.append({'tool_name': 'list_schedules', 'arguments': {'status': 'all'}, 'auto_execute': True})
    elif selected_tool == 'search_memory':
        plan.append({
            'tool_name': 'search_memory',
            'arguments': {
                'query': question,
                'subject_type': 'brand' if context.get('brand_name') else None,
                'subject_key': context.get('brand_name'),
                'tags': _memory_tags_for_context(context, question),
            },
            'auto_execute': True,
        })
    else:
        plan.append({'tool_name': selected_tool, 'arguments': {}, 'auto_execute': True})

    if plan and plan[0]['tool_name'] not in {'search_memory', 'list_memory'}:
        plan.insert(0, {
            'tool_name': 'search_memory',
            'arguments': {
                'query': question,
                'subject_type': 'brand' if context.get('brand_name') else None,
                'subject_key': context.get('brand_name'),
                'tags': _memory_tags_for_context(context, question),
            },
            'auto_execute': True,
        })
    return plan

def _gemini_reason(question: str, context: dict, memory_refs, tool_plan):
    if not gemini_available():
        return None, False
    try:
        client = _get_client()
        prompt = (
            "You are DALA Chat, an operations planner for sales analytics. "
            "You reason only and never execute tools. "
            "Return concise operator guidance with exact values and next actions. "
            "Do not mention AI and do not use em dashes.\n\n"
            f"Question: {question}\n"
            f"Resolved context JSON: {json.dumps(context, default=str)}\n"
            f"Memory refs JSON: {json.dumps(memory_refs, default=str)}\n"
            f"Tool plan JSON: {json.dumps(tool_plan, default=str)}\n"
        )
        text = (client.generate_content(prompt).text or '').strip()
        return (text or None), bool(text)
    except Exception:
        return None, False


def plan_admin_query(ds, question: str, report=None, brand_name=None,
                     retailer_code=None, batch_id=None, page_context=None):
    context = _compose_context(
        ds,
        report=report,
        brand_name=brand_name,
        retailer_code=retailer_code,
        batch_id=batch_id,
        page_context=page_context,
        question=question,
    )
    subject_type, subject_key = _resolve_memory_scope(context)
    memories = ds.search_agent_memories(
        question,
        limit=8,
        subject_type=subject_type,
        subject_key=subject_key,
        tags=_memory_tags_for_context(context, question),
    )
    context['memories'] = memories
    tool_plan = _build_tool_plan(ds, question, context)
    planned_steps = _planned_steps_from_question(question, context, selected_tool=tool_plan[0]['tool_name'] if tool_plan else None)
    risks = _detect_high_risk_states(context)
    return {
        'context': context,
        'memories': memories,
        'memory_refs': _memory_refs(memories),
        'tool_plan': tool_plan,
        'planned_steps': planned_steps,
        'suggested_actions': _rank_suggested_actions(ds, context, _suggested_actions_for_ui(context)),
        'next_jobs': ds.list_assistant_jobs(limit=6),
        'workflow_templates': _workflow_templates(context, risks=risks),
        'risks': risks,
    }


def execute_admin_plan(ds, tool_plan, context, confirmation_token=None,
                       operator_mode=True, idempotency_key=None):
    results = []
    state = 'completed'
    for index, tool_call in enumerate(tool_plan or []):
        if not tool_call.get('auto_execute', True):
            results.append({
                'tool_name': tool_call.get('tool_name'),
                'policy_class': _get_tool_policy(tool_call.get('tool_name') or ''),
                'category': _tool_category(tool_call.get('tool_name') or ''),
                **_tool_response(status='waiting', message='Tool planned but not auto-executed yet.', state='waiting'),
            })
            state = 'waiting'
            continue
        call_key = f"{idempotency_key}:{index}:{tool_call.get('tool_name')}" if idempotency_key else None
        result = execute_admin_tool(
            ds,
            tool_call.get('tool_name'),
            arguments=tool_call.get('arguments') or {},
            context=context,
            confirmation_token=confirmation_token,
            idempotency_key=call_key,
            operator_mode=operator_mode,
        )
        results.append(result)
        if result.get('status') == 'error':
            state = 'failed'
            break
        if result.get('status') == 'needs_confirmation':
            state = 'waiting'
            break
    primary = results[-1] if results else None
    return {
        'status': state,
        'state': state,
        'message': primary.get('message') if primary else 'No execution performed.',
        'artifacts': primary.get('artifacts', []) if primary else [],
        'next_steps': primary.get('next_steps', []) if primary else [],
        'errors': [error for result in results for error in result.get('errors', [])],
        'results': results,
        'primary_result': primary,
    }


def answer_admin_query(ds, question: str, report=None, brand_name=None,
                       retailer_code=None, batch_id=None, page_context=None,
                       confirmation_token=None, operator_mode=True, idempotency_key=None) -> dict:
    question = str(question or '').strip()
    plan = plan_admin_query(
        ds,
        question,
        report=report,
        brand_name=brand_name,
        retailer_code=retailer_code,
        batch_id=batch_id,
        page_context=page_context,
    )
    context = plan['context']
    execution_result = execute_admin_plan(
        ds,
        plan['tool_plan'],
        context,
        confirmation_token=confirmation_token,
        operator_mode=operator_mode,
        idempotency_key=idempotency_key,
    )

    for step in plan['planned_steps']:
        if step['step'] == 'Execute allowed tools':
            step['status'] = 'completed' if execution_result.get('status') == 'completed' else 'in_progress'
        elif step['step'] == 'Summarize findings and next actions':
            step['status'] = 'completed'

    answer, used_gemini = _gemini_reason(question, context, plan['memory_refs'], plan['tool_plan'])
    if not answer:
        answer = _build_deterministic_answer(question, context)

    primary = execution_result.get('primary_result')
    if primary:
        if primary.get('status') == 'success':
            answer = f"{answer}\n\nExecuted: {primary.get('message')}"
        elif primary.get('status') in {'needs_confirmation', 'error'}:
            answer = f"{answer}\n\nExecution status: {primary.get('message')}"

    ds.save_agent_memory(
        scope_type='copilot_query',
        scope_key=datetime.now().strftime('%Y-%m'),
        memory_text=f"Q: {question}\nA: {answer[:1200]}",
        memory_kind='copilot_session',
        confidence=0.72 if used_gemini else 0.63,
        source='admin_copilot',
        memory_layer='session',
        tags=_memory_tags_for_context(context, question),
        related_report_id=(context.get('report') or {}).get('id'),
        related_brand=context.get('brand_name'),
        metadata={
            'planned_tools': [call.get('tool_name') for call in plan['tool_plan']],
            'execution_status': execution_result.get('status'),
        },
    )
    return {
        'answer': answer,
        'used_gemini': used_gemini,
        'context': context,
        'memories': plan['memories'],
        'resolved_context': {
            'report': context.get('report'),
            'brand_name': context.get('brand_name'),
            'retailer_code': context.get('retailer_code'),
            'focus': context.get('question_focus'),
            'report_note': context.get('report_note'),
            'risks': plan['risks'],
        },
        'suggested_actions': plan['suggested_actions'],
        'planned_steps': plan['planned_steps'],
        'execution_result': execution_result,
        'memory_refs': plan['memory_refs'],
        'next_jobs': plan['next_jobs'],
        'workflow_templates': plan['workflow_templates'],
        'execution_state': execution_result.get('state'),
        'planner_tools': [call.get('tool_name') for call in plan['tool_plan']],
    }


def execute_admin_request(ds, tool_name: str, arguments=None, page_context=None,
                          report=None, brand_name=None, retailer_code=None,
                          batch_id=None, confirmation_token=None,
                          operator_mode=True, idempotency_key=None):
    context = _compose_context(
        ds,
        report=report,
        brand_name=brand_name,
        retailer_code=retailer_code,
        batch_id=batch_id,
        page_context=page_context,
        question=json.dumps(arguments or {}, default=str),
    )
    execution = execute_admin_tool(
        ds,
        tool_name,
        arguments=arguments or {},
        context=context,
        confirmation_token=confirmation_token,
        operator_mode=operator_mode,
        idempotency_key=idempotency_key,
    )
    scope_type, scope_key = _resolve_memory_scope(context)
    return {
        'resolved_context': {
            'report': context.get('report'),
            'brand_name': context.get('brand_name'),
            'retailer_code': context.get('retailer_code'),
            'focus': context.get('question_focus'),
        },
        'execution_result': execution,
        'memory_refs': _memory_refs(
            ds.search_agent_memories(
                tool_name,
                limit=4,
                subject_type=scope_type,
                subject_key=scope_key,
                tags=['copilot', 'execution', tool_name],
            )
        ),
    }
