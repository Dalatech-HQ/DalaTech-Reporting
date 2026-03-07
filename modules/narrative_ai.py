"""
narrative_ai.py — AI-powered brand performance narratives using Google Gemini.

Generates human-readable, board-ready paragraphs for each brand based on their KPIs.
Requires GEMINI_API_KEY environment variable.

Usage:
    from modules.narrative_ai import generate_brand_narrative, generate_portfolio_narrative
    text = generate_brand_narrative(brand_name, kpis, history=[], portfolio_avg=None)
    portfolio_text = generate_portfolio_narrative(all_kpis, report_meta)
"""

import os
import json

# ── Gemini client (lazy init) ─────────────────────────────────────────────────

_gemini_client = None

def _get_client():
    global _gemini_client
    if _gemini_client is None:
        api_key = os.environ.get('GEMINI_API_KEY', '')
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not set in environment variables.")
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        _gemini_client = genai.GenerativeModel('gemini-1.5-flash')
    return _gemini_client


def gemini_available():
    """Returns True if Gemini API key is configured."""
    return bool(os.environ.get('GEMINI_API_KEY', ''))


# ── Prompt builders ───────────────────────────────────────────────────────────

def _build_brand_prompt(brand_name, kpis, history=None, portfolio_avg=None):
    """Build the Gemini prompt for a single brand narrative."""
    rev      = kpis.get('total_revenue', 0)
    qty      = kpis.get('total_qty', 0)
    stores   = kpis.get('num_stores', 0)
    repeat   = kpis.get('repeat_pct', 0)
    grade    = kpis.get('perf_grade', '-')
    score    = kpis.get('perf_score', 0)
    stock_d  = kpis.get('stock_days_cover', 0)
    wow      = kpis.get('wow_rev_change', 0)
    inv_st   = kpis.get('inv_health_status', 'Unknown')
    top_sku  = kpis.get('top_sku_name', '')
    top_store= kpis.get('top_store_name', '')

    # Trend context from history
    trend_context = ''
    if history and len(history) >= 2:
        prev = history[-1] if isinstance(history[-1], dict) else {}
        prev_rev = prev.get('total_revenue', 0)
        if prev_rev > 0:
            mom_change = round((rev - prev_rev) / prev_rev * 100, 1)
            direction = 'up' if mom_change >= 0 else 'down'
            trend_context = (
                f"Month-over-month revenue is {direction} {abs(mom_change)}% "
                f"(from ₦{prev_rev:,.0f} last period). "
            )
        if len(history) >= 3:
            revs = [h.get('total_revenue', 0) for h in history if isinstance(h, dict)]
            if all(r > 0 for r in revs):
                if revs[-1] > revs[0]:
                    trend_context += "The brand shows an upward trajectory over recent periods. "
                else:
                    trend_context += "The brand has been on a declining trend recently. "

    avg_context = ''
    if portfolio_avg and portfolio_avg > 0:
        if rev > portfolio_avg * 1.2:
            avg_context = f"Revenue is {round((rev/portfolio_avg - 1)*100)}% above the portfolio average of ₦{portfolio_avg:,.0f}. "
        elif rev < portfolio_avg * 0.8:
            avg_context = f"Revenue is {round((1 - rev/portfolio_avg)*100)}% below the portfolio average. "
        else:
            avg_context = "Revenue is close to the portfolio average. "

    prompt = f"""You are a professional business analyst for DALA Technologies, a Nigerian FMCG distribution company.
Write a concise, insightful 3–4 sentence performance narrative for the brand "{brand_name}" based on the following data.

KEY METRICS:
- Revenue: ₦{rev:,.0f}
- Units Sold: {qty:,.0f}
- Active Stores: {stores}
- Repeat Purchase Rate: {repeat:.1f}%
- Performance Grade: {grade} (Score: {score}/100)
- Stock Days Cover: {stock_d:.1f} days ({inv_st})
- Week-over-Week Revenue Change: {wow:+.1f}%
{"- Top Store: " + top_store if top_store else ""}
{"- Best Performing SKU: " + top_sku if top_sku else ""}
{trend_context}{avg_context}

WRITING RULES:
1. Start with the brand name and a strong opening line about performance.
2. Highlight 1–2 key strengths and 1 area of concern or opportunity.
3. End with a clear, actionable recommendation for the DALA team.
4. Use Nigerian Naira symbol ₦ for money. Keep it professional but readable.
5. Do NOT mention AI, this prompt, or anything about data sources.
6. Maximum 100 words. No bullet points. Flowing paragraphs only.

Write the narrative now:"""
    return prompt


def _build_portfolio_prompt(all_kpis, report_meta):
    """Build the Gemini prompt for an executive portfolio summary."""
    total_rev   = sum(k.get('total_revenue', 0) for k in all_kpis.values())
    brand_count = len(all_kpis)
    grades = [k.get('perf_grade', '') for k in all_kpis.values()]
    top_brand = max(all_kpis.items(), key=lambda x: x[1].get('total_revenue', 0), default=(None, {}))[0]
    grade_A_count = grades.count('A')
    grade_D_or_F  = sum(1 for g in grades if g in ('D', 'F'))
    period = report_meta.get('month_label', 'this period')

    prompt = f"""You are a senior business analyst writing an executive summary for DALA Technologies, a Nigerian FMCG distribution company.
Write a board-ready 4–5 sentence portfolio executive summary for {period}.

PORTFOLIO STATS:
- Total Portfolio Revenue: ₦{total_rev:,.0f}
- Active Brand Partners: {brand_count}
- Top Performer: {top_brand}
- Grade A Brands: {grade_A_count} of {brand_count}
- Brands Needing Attention (D/F grade): {grade_D_or_F}

WRITING RULES:
1. Open with a strong headline summary of portfolio performance.
2. Mention the top performer and overall portfolio health.
3. Flag any systemic risks (e.g., multiple brands with low stock, declining revenue).
4. Close with strategic priorities for the next period.
5. Professional, confident tone — suitable for board/investor presentation.
6. Maximum 130 words. No bullet points.

Write the executive summary now:"""
    return prompt


# ── Main functions ────────────────────────────────────────────────────────────

def generate_brand_narrative(brand_name, kpis, history=None, portfolio_avg=None):
    """
    Generate an AI narrative for a single brand.

    Returns:
        (str, bool): (narrative_text, from_cache)
        Returns (None, False) if Gemini is not configured.
    """
    if not gemini_available():
        return None, False

    try:
        client = _get_client()
        prompt = _build_brand_prompt(brand_name, kpis, history, portfolio_avg)
        response = client.generate_content(prompt)
        return response.text.strip(), False
    except Exception as exc:
        raise RuntimeError(f"Gemini narrative failed for {brand_name}: {exc}") from exc


def generate_portfolio_narrative(all_kpis, report_meta=None):
    """
    Generate an executive AI summary for the whole portfolio.

    Returns:
        str: narrative text, or None if Gemini not configured.
    """
    if not gemini_available():
        return None

    try:
        client = _get_client()
        prompt = _build_portfolio_prompt(all_kpis, report_meta or {})
        response = client.generate_content(prompt)
        return response.text.strip()
    except Exception as exc:
        raise RuntimeError(f"Gemini portfolio narrative failed: {exc}") from exc


def generate_bulk_narratives(all_kpis, ds, report_id):
    """
    Generate AI narratives for all brands in a report and cache them in DB.
    Silently skips if Gemini is not available.

    Returns:
        dict: {brand_name: narrative_text}
    """
    if not gemini_available():
        return {}

    results = {}
    total_portfolio_revenue = sum(k.get('total_revenue', 0) for k in all_kpis.values())
    portfolio_avg = total_portfolio_revenue / max(len(all_kpis), 1)

    for brand_name, kpis in all_kpis.items():
        try:
            history = ds.get_brand_history(brand_name, limit=6) if ds else []
            narrative, _ = generate_brand_narrative(brand_name, kpis, history, portfolio_avg)
            if narrative:
                results[brand_name] = narrative
                # Cache in DB
                if ds:
                    ds.save_narrative(report_id, brand_name, narrative)
        except Exception:
            pass  # Don't fail the whole generation if one narrative fails

    # Portfolio narrative
    try:
        report_meta = ds.get_report(report_id) if ds else {}
        portfolio_text = generate_portfolio_narrative(all_kpis, report_meta)
        if portfolio_text and ds:
            ds.save_narrative(report_id, '__portfolio__', portfolio_text)
    except Exception:
        pass

    return results


def generate_recommendations(brand_name: str, kpis: dict, churn_data: list = None, portfolio_avg: float = None) -> str:
    """
    Generate 3 concrete action-oriented recommendations for a brand using Gemini.

    Returns:
        str: Numbered action bullets, or None if Gemini not available.
    """
    if not gemini_available():
        return None
    try:
        rev = kpis.get('total_revenue', 0)
        stores = kpis.get('num_stores', 0)
        repeat = kpis.get('repeat_pct', 0)
        stock_d = kpis.get('stock_days_cover', 0)
        grade = kpis.get('perf_grade', '-')
        top_sku = kpis.get('top_sku_name', '')
        top_store = kpis.get('top_store_name', '')
        churned_count = len([c for c in (churn_data or []) if c.get('churn_type') == 'churned'])

        prompt = f"""You are a sales strategy advisor for DALA Technologies, a consumer goods distributor in Nigeria.

Brand: {brand_name}
Revenue: ₦{rev:,.0f} | Grade: {grade} | Stores: {stores} | Repeat Rate: {repeat}%
Stock Days Cover: {stock_d} | Top SKU: {top_sku or 'N/A'} | Top Store: {top_store or 'N/A'}
Churned Stores: {churned_count}
{"Portfolio Average Revenue: ₦"+f"{portfolio_avg:,.0f}" if portfolio_avg else ""}

Provide exactly 3 specific, actionable recommendations for the DALA sales team to improve this brand's performance next month.
Each must be concrete (mention actual numbers, store names if possible, or specific actions).
Format as:
1. [action]
2. [action]
3. [action]

Keep each to 1-2 sentences. Be direct and practical."""

        client = _get_client()
        response = client.generate_content(prompt)
        return response.text.strip()
    except Exception as exc:
        raise RuntimeError(f"Gemini recommendations failed for {brand_name}: {exc}") from exc
