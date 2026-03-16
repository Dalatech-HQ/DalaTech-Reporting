"""
coach_signals.py

Deterministic, explainable signals for the Sales Coach. Gemini is used only
for optional reasoning and phrasing on top of structured evidence.
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import quote

from .narrative_ai import gemini_available, _get_client


SEVERITY_ORDER = {"critical": 0, "high": 1, "medium": 2, "low": 3}
DEFAULT_SIGNAL_THRESHOLDS = {
    "revenue_declining_pct": -12.0,
    "revenue_declining_high_pct": -20.0,
    "revenue_accelerating_pct": 15.0,
    "repeat_rate_declining_points": -8.0,
    "repeat_rate_declining_high_points": -15.0,
    "retailer_issue_visit_min": 3,
    "retailer_issue_density_high": 0.4,
    "retailer_underpenetrated_brand_count": 3,
    "low_visit_high_potential_transactions_max": 1,
    "forecast_confidence_score_min": 55.0,
    "retailer_concentration_share_pct": 35.0,
}


def get_signal_thresholds(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    thresholds = dict(DEFAULT_SIGNAL_THRESHOLDS)
    if overrides:
        for key, value in overrides.items():
            if key in thresholds and value is not None:
                thresholds[key] = value
    return thresholds


def _signal(scope_type: str, scope_key: str, period_type: str, period_start: str,
            period_end: str, signal_type: str, title: str, message: str,
            severity: str = "medium", confidence: float = 0.7,
            metrics: dict[str, Any] | None = None,
            recommended_actions: list[str] | None = None) -> dict[str, Any]:
    return {
        "scope_type": scope_type,
        "scope_key": scope_key,
        "signal_type": signal_type,
        "severity": severity,
        "confidence": round(float(confidence or 0), 2),
        "period_type": period_type,
        "period_start": period_start,
        "period_end": period_end,
        "evidence": {
            "title": title,
            "message": message,
            "metrics": metrics or {},
        },
        "recommended_actions": recommended_actions or [],
    }


def derive_snapshot_signals(snapshot: dict[str, Any], thresholds: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    thresholds = get_signal_thresholds(thresholds)
    scope_type = snapshot.get("scope_type") or "portfolio"
    scope_key = snapshot.get("scope_key") or "global"
    period_type = snapshot.get("period_type") or "monthly"
    period_start = snapshot.get("period_start")
    period_end = snapshot.get("period_end")
    metrics = snapshot.get("metrics") or {}
    comparisons = snapshot.get("comparisons") or {}
    activity = snapshot.get("activity") or {}
    signals: list[dict[str, Any]] = []

    revenue_mom = comparisons.get("revenue_mom")
    if revenue_mom is not None and revenue_mom <= thresholds["revenue_declining_pct"]:
        signals.append(_signal(
            scope_type, scope_key, period_type, period_start, period_end,
            "revenue_declining",
            "Revenue is deteriorating",
            f"Revenue is down {abs(float(revenue_mom)):.1f}% versus the comparable prior period.",
            severity="high" if revenue_mom <= thresholds["revenue_declining_high_pct"] else "medium",
            confidence=0.82,
            metrics={"revenue_mom": revenue_mom, "revenue": metrics.get("revenue")},
            recommended_actions=[
                "Review the biggest negative movers behind the decline.",
                "Check whether field activity or stock gaps explain the drop.",
            ],
        ))
    elif revenue_mom is not None and revenue_mom >= thresholds["revenue_accelerating_pct"]:
        signals.append(_signal(
            scope_type, scope_key, period_type, period_start, period_end,
            "revenue_accelerating",
            "Revenue is accelerating",
            f"Revenue is up {float(revenue_mom):.1f}% versus the comparable prior period.",
            severity="medium",
            confidence=0.78,
            metrics={"revenue_mom": revenue_mom, "revenue": metrics.get("revenue")},
            recommended_actions=[
                "Protect supply and availability while momentum is favorable.",
                "Double down on the stores or brands driving the lift.",
            ],
        ))

    repeat_rate_delta = comparisons.get("repeat_rate_delta")
    if repeat_rate_delta is not None and repeat_rate_delta <= thresholds["repeat_rate_declining_points"]:
        signals.append(_signal(
            scope_type, scope_key, period_type, period_start, period_end,
            "repeat_rate_declining",
            "Repeat behavior is weakening",
            f"Repeat rate fell by {abs(float(repeat_rate_delta)):.1f} points to {float(metrics.get('repeat_rate') or 0):.1f}%.",
            severity="high" if repeat_rate_delta <= thresholds["repeat_rate_declining_high_points"] else "medium",
            confidence=0.8,
            metrics={"repeat_rate": metrics.get("repeat_rate"), "repeat_rate_delta": repeat_rate_delta},
            recommended_actions=[
                "Investigate the entities that only bought once in the current period.",
                "Prioritize follow-up on stores or brands that dropped out of repeat ordering.",
            ],
        ))

    if scope_type in {"retailer", "retailer_group"}:
        issue_total = int((activity.get("totals") or {}).get("issues") or 0)
        visit_total = int((activity.get("totals") or {}).get("visits") or 0)
        issue_density = round(issue_total / max(visit_total, 1), 2) if visit_total else 0.0
        if visit_total >= thresholds["retailer_issue_visit_min"] and issue_density >= thresholds["retailer_issue_density_high"]:
            signals.append(_signal(
                scope_type, scope_key, period_type, period_start, period_end,
                "high_issue_density",
                "Field execution issues are elevated",
                f"{issue_total} issues were logged across {visit_total} visits for this {'retailer group' if scope_type == 'retailer_group' else 'retailer'}.",
                severity="high",
                confidence=0.76,
                metrics={"issues": issue_total, "visits": visit_total, "issue_density": issue_density},
                recommended_actions=[
                    "Review the latest issue log and resolve the recurring problem types first.",
                    "Use the recent visits section to assign a follow-up owner.",
                ],
            ))

        opportunity_brands = snapshot.get("opportunity_brands") or []
        if len(opportunity_brands) >= thresholds["retailer_underpenetrated_brand_count"]:
            signals.append(_signal(
                scope_type, scope_key, period_type, period_start, period_end,
                "retailer_underpenetrated",
                "Assortment opportunity exists",
                f"{len(opportunity_brands)} strong portfolio brands are not currently active in this {'retailer group' if scope_type == 'retailer_group' else 'retailer'}.",
                severity="medium",
                confidence=0.74,
                metrics={"missing_brand_count": len(opportunity_brands)},
                recommended_actions=[
                    "Pitch the top missing brands in the opportunity table.",
                    "Compare with similar high-revenue retailers before the next visit.",
                ],
            ))

        if int(metrics.get("transactions") or 0) <= thresholds["low_visit_high_potential_transactions_max"] and float(metrics.get("revenue") or 0) > 0:
            signals.append(_signal(
                scope_type, scope_key, period_type, period_start, period_end,
                "low_visit_high_potential",
                "Retailer engagement looks thin for current sales value",
                f"This {'retailer group' if scope_type == 'retailer_group' else 'retailer'} generated sales with very few transactions in the selected period.",
                severity="medium",
                confidence=0.65,
                metrics={"transactions": metrics.get("transactions"), "revenue": metrics.get("revenue")},
                recommended_actions=[
                    "Confirm whether this is a one-off bulk buy or a true growth signal.",
                    "Schedule a follow-up to widen brand coverage if the store is strategic.",
                ],
            ))

    if scope_type == "brand":
        forecast = snapshot.get("forecast") or {}
        confidence_band = str(forecast.get("confidence_band") or "").lower()
        confidence_score = float(forecast.get("confidence_score") or 0)
        if confidence_band in {"weak", "low"} or confidence_score and confidence_score < thresholds["forecast_confidence_score_min"]:
            signals.append(_signal(
                scope_type, scope_key, period_type, period_start, period_end,
                "forecast_low_confidence",
                "Forecast confidence is weak",
                f"Forecast confidence is {forecast.get('confidence_band') or 'low'} at {confidence_score:.0f}/100.",
                severity="medium",
                confidence=0.77,
                metrics={"confidence_band": forecast.get("confidence_band"), "confidence_score": confidence_score},
                recommended_actions=[
                    "Avoid overcommitting to long-horizon forecast assumptions.",
                    "Use nearer-term retailer and activity signals for planning.",
                ],
            ))

        retailer_rows = snapshot.get("retailer_rows") or []
        if retailer_rows:
            top_share = float(retailer_rows[0].get("share_pct") or 0)
            if top_share >= thresholds["retailer_concentration_share_pct"]:
                signals.append(_signal(
                    scope_type, scope_key, period_type, period_start, period_end,
                    "retailer_concentration",
                    "Retailer concentration is high",
                    f"The top retailer contributes {top_share:.1f}% of current brand revenue.",
                    severity="medium",
                    confidence=0.71,
                    metrics={"top_retailer_share_pct": top_share},
                    recommended_actions=[
                        "Protect the top retailer relationship and monitor concentration risk.",
                        "Develop the next tier of retailers to reduce dependence.",
                    ],
                ))

    signals.sort(
        key=lambda item: (
            SEVERITY_ORDER.get(item.get("severity") or "low", 9),
            -(item.get("confidence") or 0),
            item.get("signal_type") or "",
        )
    )
    return signals


def persist_snapshot_signals(ds, snapshot: dict[str, Any], signals: list[dict[str, Any]]):
    saved = []
    for signal in signals:
        saved_signal = ds.save_coach_signal(
            scope_type=signal["scope_type"],
            scope_key=signal["scope_key"],
            signal_type=signal["signal_type"],
            severity=signal["severity"],
            confidence=signal["confidence"],
            period_type=signal["period_type"],
            period_start=signal["period_start"],
            period_end=signal["period_end"],
            evidence=signal["evidence"],
            recommended_actions=signal["recommended_actions"],
            report_id=snapshot.get("report_id"),
        )
        if saved_signal:
            saved.append(saved_signal)
    return saved


def _slug(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return text or "recommendation"


def _dedupe_action_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    deduped = []
    for item in items:
        key = (item.get("kind"), item.get("url"), item.get("api_path"), item.get("recommendation_key"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _score_lookup(ds, snapshot: dict[str, Any]) -> dict[str, Any]:
    return ds.get_recommendation_outcome_scores(
        brand_name=snapshot.get("scope_key") if snapshot.get("scope_type") == "brand" else None,
        scope_type=snapshot.get("scope_type"),
        scope_key=snapshot.get("scope_key"),
    )


def build_recommendation_items(ds, snapshot: dict[str, Any], signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scores = _score_lookup(ds, snapshot)
    items = []
    for signal in signals:
        for action in signal.get("recommended_actions") or []:
            recommendation_key = f"{signal.get('signal_type')}::{_slug(action)}"
            score = scores.get(recommendation_key, {})
            items.append({
                "recommendation_key": recommendation_key,
                "label": action,
                "signal_type": signal.get("signal_type"),
                "severity": signal.get("severity"),
                "weighted_score": float(score.get("weighted_score") or 0),
                "total_events": int(score.get("total_events") or 0),
            })
    items.sort(
        key=lambda item: (
            -float(item.get("weighted_score") or 0),
            SEVERITY_ORDER.get(item.get("severity") or "low", 9),
            item.get("label") or "",
        )
    )
    deduped = []
    seen = set()
    for item in items:
        if item["recommendation_key"] in seen:
            continue
        seen.add(item["recommendation_key"])
        deduped.append(item)
    return deduped


def build_action_items(ds, snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    scope_type = snapshot.get("scope_type") or "portfolio"
    scope_key = snapshot.get("scope_key") or "global"
    report_id = snapshot.get("report_id")
    period_label = snapshot.get("period_label")
    scores = _score_lookup(ds, snapshot)

    def _action(kind: str, label: str, recommendation_key: str, url: str | None = None,
                 api_path: str | None = None, method: str = "GET",
                 payload: dict[str, Any] | None = None, tone: str = "secondary") -> dict[str, Any]:
        score = scores.get(recommendation_key, {})
        return {
            "kind": kind,
            "label": label,
            "recommendation_key": recommendation_key,
            "url": url,
            "api_path": api_path,
            "method": method,
            "payload": payload or {},
            "weighted_score": float(score.get("weighted_score") or 0),
            "total_events": int(score.get("total_events") or 0),
            "tone": tone,
        }

    items: list[dict[str, Any]] = []
    if scope_type == "brand":
        encoded_scope = quote(str(scope_key), safe="")
        items.extend([
            _action("open", "Open Brand", "open_brand_overview", url=f"/brand/{encoded_scope}", tone="primary"),
            _action("open", "Open Trends", "open_brand_trends", url=f"/trends?scope=brand&brand={encoded_scope}", tone="primary"),
            _action("open", "Open Forecast", "open_brand_forecast", url=f"/brand/{encoded_scope}?focus=forecast", tone="secondary"),
            _action("report", "Open Report", "generate_brand_report", url=f"/api/report_html/{report_id}/{encoded_scope}" if report_id else None, tone="secondary"),
        ])
    elif scope_type == "retailer":
        encoded_scope = quote(str(scope_key), safe="")
        query_string = f"?report_id={report_id}" if report_id else ""
        items.extend([
            _action("open", "Open Retailer", "open_retailer_detail", url=f"/retailer/{encoded_scope}{query_string}", tone="primary"),
            _action("report", "Open Report", "generate_retailer_report", url=f"/retailer/{encoded_scope}/report{query_string}", tone="secondary"),
            _action("open", "View Activity", "open_retailer_activity", url=f"/activity-intelligence?store={encoded_scope}" + (f"&report_id={report_id}" if report_id else ""), tone="secondary"),
        ])
    elif scope_type == "retailer_group":
        encoded_scope = quote(str(scope_key), safe="")
        query_string = f"?report_id={report_id}" if report_id else ""
        items.extend([
            _action("open", "Open Group", "open_retailer_group_detail", url=f"/retailer-groups/{encoded_scope}{query_string}", tone="primary"),
            _action("report", "Open Group Report", "generate_retailer_group_report", url=f"/retailer-groups/{encoded_scope}/report{query_string}", tone="secondary"),
        ])
    else:
        items.extend([
            _action("open", "Open Forecasting", "open_portfolio_forecasting", url="/forecasting", tone="primary"),
            _action("open", "Open Trends", "open_portfolio_trends", url="/trends", tone="primary"),
            _action("open", "Open Copilot", "open_copilot", url="/copilot", tone="secondary"),
        ])

    items.extend([
        _action(
            "schedule",
            "Auto-refresh Coach",
            "schedule_coach_refresh",
            api_path="/api/copilot/schedules",
            method="POST",
            payload={
                "label": f"{scope_type.title()} Coach Refresh",
                "job_type": "coach_refresh",
                "target": scope_key,
                "cadence": "nightly",
                "payload": {
                    "scope_type": scope_type,
                    "scope_key": scope_key,
                    "report_id": report_id,
                    "period_label": period_label,
                },
            },
            tone="utility",
        ),
        _action(
            "pin",
            "Pin Insight",
            "pin_coach_summary",
            api_path="/api/coach/pin",
            method="POST",
            payload={
                "scope_type": scope_type,
                "scope_key": scope_key,
                "report_id": report_id,
            },
            tone="utility",
        ),
    ])
    items = _dedupe_action_items(items)
    kind_priority = {"open": 0, "report": 1, "schedule": 2, "pin": 3}
    items.sort(key=lambda item: (kind_priority.get(item.get("kind"), 9), item.get("label") or ""))
    return items


def _strip_code_fence(text: str) -> str:
    cleaned = str(text or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    return cleaned.strip()


def _deterministic_summary(snapshot: dict[str, Any], signals: list[dict[str, Any]]) -> dict[str, Any]:
    metrics = snapshot.get("metrics") or {}
    scope_type = snapshot.get("scope_type") or "portfolio"
    revenue = float(metrics.get("revenue") or 0)
    headline = "Portfolio summary" if scope_type == "portfolio" else "Performance summary"
    if signals:
        primary = signals[0]
        headline = primary["evidence"]["title"]
        summary = primary["evidence"]["message"]
    else:
        scope_label = "the portfolio" if scope_type == "portfolio" else "this scope"
        summary = (
            f"No major risk signal is open for {scope_label}. "
            f"Current revenue is ₦{revenue:,.2f} with repeat rate at {float(metrics.get('repeat_rate') or 0):.1f}%."
        )
    recommended_actions = []
    for signal in signals[:3]:
        for action in signal.get("recommended_actions") or []:
            if action not in recommended_actions:
                recommended_actions.append(action)
    if not recommended_actions:
        recommended_actions.append("Monitor the next comparable period for meaningful movement.")
    return {
        "headline": headline,
        "summary": summary,
        "recommended_actions": recommended_actions[:4],
        "used_gemini": False,
    }


def summarize_snapshot(snapshot: dict[str, Any], signals: list[dict[str, Any]],
                       use_gemini: bool = True) -> dict[str, Any]:
    deterministic = _deterministic_summary(snapshot, signals)
    if not use_gemini or not gemini_available():
        return deterministic
    try:
        client = _get_client()
        prompt = (
            "You are DALA Coach. Use only the provided structured evidence. "
            "Return JSON with keys headline, summary, recommended_actions. "
            "Keep the summary to 2 sentences and recommended_actions to 3 short items.\n\n"
            f"Snapshot JSON: {json.dumps(snapshot, default=str)}\n"
            f"Signals JSON: {json.dumps(signals, default=str)}\n"
        )
        response = client.generate_content(prompt)
        text = _strip_code_fence(getattr(response, "text", "") or "")
        payload = json.loads(text)
        headline = str(payload.get("headline") or deterministic["headline"]).strip()
        summary = str(payload.get("summary") or deterministic["summary"]).strip()
        actions = [str(item).strip() for item in (payload.get("recommended_actions") or []) if str(item).strip()]
        return {
            "headline": headline,
            "summary": summary,
            "recommended_actions": actions[:4] or deterministic["recommended_actions"],
            "used_gemini": True,
        }
    except Exception:
        return deterministic


def build_coach_payload(ds, snapshot: dict[str, Any], persist: bool = True,
                        use_gemini: bool = True) -> dict[str, Any]:
    signals = derive_snapshot_signals(snapshot)
    saved_signals = persist_snapshot_signals(ds, snapshot, signals) if persist else signals
    summary = summarize_snapshot(snapshot, saved_signals if persist else signals, use_gemini=use_gemini)
    recommendation_items = build_recommendation_items(ds, snapshot, saved_signals if persist else signals)
    if recommendation_items:
        summary["recommended_actions"] = [item["label"] for item in recommendation_items[:4]]
    action_items = build_action_items(ds, snapshot)
    return {
        "summary": summary,
        "signals": saved_signals if persist else signals,
        "recommendation_items": recommendation_items,
        "action_items": action_items,
        "thresholds": get_signal_thresholds(),
    }
