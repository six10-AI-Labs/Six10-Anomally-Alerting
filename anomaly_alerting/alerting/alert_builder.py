# alerting/alert_builder.py
# Layer 4 — build structured alert messages grouped by severity and sorted by tier.

import pandas as pd
from typing import List, Dict
from datetime import datetime, timedelta
import config
import json
import os

TIER_SORT_ORDER = ["homerun", "triple", "double", "single", "less_than_single"]
SEVERITY_SORT_ORDER = ["critical", "warning", "watch"]

# Metrics displayed as percentages (multiply by 100, stored as decimals internally)
PCT_METRICS = {"conversion_rate", "return_rate", "acos", "tacos", "margin"}

# Metrics displayed as dollar revenue
DOLLAR_METRICS = {"sales"}

# Helium10 snapshot metrics — each has its own display unit
RANK_METRICS   = {"keyword_avg_rank"}
RATING_METRICS = {"review_rating"}
COUNT_METRICS  = {"review_count", "organic_top10_count"}

# Tier display labels for the email
TIER_LABELS = {
    "homerun":         "HOMERUN  (>$2.5M)",
    "triple":          "TRIPLE   ($1.5M–$2.5M)",
    "double":          "DOUBLE   ($750K–$1.5M)",
    "single":          "SINGLE   ($250K–$750K)",
    "less_than_single": "LESS THAN A SINGLE  (<$250K)",
}

# Triggered-by plain English labels
TRIGGER_LABELS = {
    "rolling":            "Short-term rolling baseline",
    "yoy":               "Year-over-Year baseline",
    "both":              "Both baselines",
    "absolute_threshold": "Absolute business threshold",
}


def _fmt_value(val, metric: str) -> str:
    """Format a metric value for display in its natural unit."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    if metric in PCT_METRICS:
        return f"{val * 100:.1f}%"
    if metric in DOLLAR_METRICS:
        return f"${val:,.2f}"
    if metric in RANK_METRICS:
        return f"#{int(val):,}"
    if metric in RATING_METRICS:
        return f"{val:.1f}★"
    if metric in COUNT_METRICS:
        return f"{int(val):,}"
    return f"{val:,.2f}"


def _fmt_deviation(val) -> str:
    """Format a % deviation with sign."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val * 100:.1f}%"


def _fmt_zscore(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "N/A"
    sign = "+" if val >= 0 else ""
    return f"{sign}{val:.2f}σ"


def group_alerts_by_severity(flagged_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Group flagged ASIN rows by their severity level.

    Returns:
        Dict with keys 'critical', 'warning', 'watch', 'improvement', each mapping
        to a sub-dataframe of rows at that severity level.
    """
    grouped = {}
    for sev in SEVERITY_SORT_ORDER + ["improvement"]:
        subset = flagged_df[flagged_df["severity"] == sev].copy()
        grouped[sev] = subset
    return grouped


def sort_by_tier(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sort a dataframe of alerts by baseball tier (highest-revenue tier first),
    then by ASIN and metric for stable ordering within the same tier.
    """
    if df.empty:
        return df
    tier_order = {t: i for i, t in enumerate(TIER_SORT_ORDER)}
    df = df.copy()
    df["_tier_rank"] = df["tier"].map(tier_order).fillna(len(TIER_SORT_ORDER))
    df = df.sort_values(["_tier_rank", "asin", "metric"]).drop(columns=["_tier_rank"])
    return df


def format_alert_row(row: pd.Series) -> str:
    """
    Format a single flagged row into a human-readable alert string.

    Example output:
        ASIN: B012345678  |  Pool Chlorine Starter Kit
        Metric:     return_rate
        Value:      6.2%    (Expected: 1.8%  |  YoY Baseline: 2.1%)
        Triggered:  Absolute business threshold  |  Severity: CRITICAL
        Detail:     Z-Score: +4.10σ   YoY Dev: +195.0%
    """
    metric = row.get("metric", "")
    asin = row.get("asin", "")
    title = row.get("title", "")
    tier = row.get("tier", "")
    severity = str(row.get("severity", "")).upper()
    triggered_by = row.get("triggered_by", "")
    yoy_available = row.get("yoy_available", True)

    actual = _fmt_value(row.get("actual_value"), metric)
    expected = _fmt_value(row.get("expected_value"), metric)
    yoy_base = _fmt_value(row.get("yoy_baseline"), metric) if yoy_available else "N/A (< 12 months data)"
    z_score = _fmt_zscore(row.get("z_score"))
    yoy_dev = _fmt_deviation(row.get("yoy_deviation")) if yoy_available else "N/A"
    trigger_label = TRIGGER_LABELS.get(triggered_by, triggered_by)

    # Build the name line
    name_part = f"  {title}" if title and not pd.isna(title) else ""
    bsr = row.get("category_bsr")
    bsr_part = f"  |  BSR: {int(bsr):,}" if bsr and not pd.isna(bsr) else ""

    lines = [
        f"  ASIN: {asin}{name_part}{bsr_part}",
        f"  Metric:    {metric}",
        f"  Value:     {actual}   (Expected: {expected}  |  YoY Baseline: {yoy_base})",
        f"  Triggered: {trigger_label}   |   Severity: {severity}",
        f"  Detail:    Z-Score: {z_score}   YoY Dev: {yoy_dev}",
    ]
    return "\n".join(lines)


def build_email_body(grouped_alerts: Dict[str, pd.DataFrame], run_date: str) -> str:
    """
    Assemble the full email body from grouped and sorted alert sections.

    Structure:
        Header
        [CRITICAL ALERTS]  — sorted by tier
        [WARNING ALERTS]
        [WATCH ALERTS]
        Footer
    """
    total_alerts = sum(len(df) for df in grouped_alerts.values())
    unique_asins = len(
        pd.concat(list(grouped_alerts.values()), ignore_index=True)["asin"].unique()
    ) if total_alerts > 0 else 0

    # Data lag note: T-2 means data is 2 days behind run date
    try:
        run_dt = datetime.strptime(run_date, "%Y-%m-%d")
        data_through = (run_dt - timedelta(days=2)).strftime("%Y-%m-%d")
    except Exception:
        data_through = "T-2"

    SEP = "=" * 60
    THIN = "-" * 60

    lines = [
        SEP,
        f"  SIX10 ANOMALY ALERT DIGEST — {run_date}",
        f"  Total alerts: {total_alerts}   |   ASINs affected: {unique_asins}",
        SEP,
        "",
    ]

    for sev in SEVERITY_SORT_ORDER:
        df = grouped_alerts.get(sev, pd.DataFrame())
        count = len(df)
        header = f"[{sev.upper()} ALERTS — {count}]"
        lines.append(header)
        lines.append(THIN)

        if count == 0:
            lines.append("  None")
            lines.append("")
            continue

        sorted_df = sort_by_tier(df)
        current_tier = None

        for _, row in sorted_df.iterrows():
            row_tier = row.get("tier", "")
            if row_tier != current_tier:
                current_tier = row_tier
                tier_label = TIER_LABELS.get(row_tier, row_tier.upper())
                lines.append(f"\n  [{tier_label}]")
                lines.append("")
            lines.append(format_alert_row(row))
            lines.append("")

        lines.append("")

    lines += [
        THIN,
        f"  Run date: {run_date}   |   Data through: {data_through} (T-2 lag, Sellerise 48hr delay)",
        f"  Total alerts: {total_alerts}   |   ASINs affected: {unique_asins}",
        "  Thresholds are starting points — validate and tune after launch.",
        SEP,
    ]

    return "\n".join(lines)


def build_email_subject(grouped_alerts: Dict[str, pd.DataFrame], run_date: str) -> str:
    """
    Generate the email subject line summarizing the alert counts.

    Example: '[Six10 Alerts] ~$4,700/day at risk | 2026-03-25 | 2 Critical | 5 Warning | 3 Watch | 4 Improving'
    """
    n_critical    = len(grouped_alerts.get("critical",    []))
    n_warning     = len(grouped_alerts.get("warning",     []))
    n_watch       = len(grouped_alerts.get("watch",       []))
    n_improvement = len(grouped_alerts.get("improvement", []))
    
    dollar_impact = 0.0
    if getattr(config, "SHOW_DOLLAR_IMPACT_IN_SUBJECT", False):
        for sev, df in grouped_alerts.items():
            if sev == "improvement" or df.empty:
                continue
            for _, r in df.iterrows():
                m = r.get("metric", "")
                actual = r.get("actual_value")
                yoy = r.get("yoy_baseline")
                expected = r.get("expected_value")
                base = yoy if r.get("triggered_by") in ("yoy", "both") else expected
                
                if m == "sales" and pd.notna(actual) and pd.notna(base):
                    loss = base - actual
                    if loss > 0: dollar_impact += loss
                elif m == "margin" and "sales_roll_mean" in r and pd.notna(r["sales_roll_mean"]):
                    if pd.notna(actual) and pd.notna(base) and base > actual:
                        diff = base - actual
                        dollar_impact += diff * r["sales_roll_mean"]

    impact_str = f"~${dollar_impact:,.0f}/day at risk | " if dollar_impact > 0 else ""
    
    subject = (
        f"[Six10 Alerts] {impact_str}{run_date} | "
        f"{n_critical} Critical | {n_warning} Warning | {n_watch} Watch"
    )
    if n_improvement > 0:
        subject += f" | {n_improvement} Improving"
    return subject


def filter_alerts(flagged_df: pd.DataFrame, dismissed_asins: list = None) -> pd.DataFrame:
    """
    Apply hierarchical filtering to the flagged rows to reduce noise.
    
    Filters:
      1. ASIN Dismissal: Exclude ASINs found in the dismissed_asins list (from Google Sheet).
      2. Baseball Tiers: Optionally exclude 'less_than_single' products.
      3. Global Caps: Limit the number of shown alerts per severity for scannability.
    """
    if flagged_df is None or flagged_df.empty:
        return flagged_df

    df = flagged_df.copy()

    # Apply ASIN dismissal filter (fetched from Google Sheet via main.py)
    if dismissed_asins:
        count_before = len(df)
        df = df[~df["asin"].isin(dismissed_asins)]
        count_after = len(df)
        if count_before > count_after:
            print(f"  [Filter] Excluded {count_before - count_after} alerts for ASINs marked as 'Dismissed' in Sheet.")

    improvements = df[df["severity"] == "improvement"]
    alerts = df[df["severity"] != "improvement"]

    if config.SUPPRESS_LESS_THAN_SINGLE:
        alerts = alerts[alerts["tier"] != "less_than_single"]
        improvements = improvements[improvements["tier"] != "less_than_single"]
    elif config.SUPPRESS_WATCH_FOR_LESS_THAN_SINGLE:
        alerts = alerts[~((alerts["tier"] == "less_than_single") & (alerts["severity"] == "watch"))]

    if config.SUPPRESS_WATCH_ALERTS:
        alerts = alerts[alerts["severity"] != "watch"]

    # Apply per-severity caps in tier order (homerun → triple → double → single)
    # Keeps the email scannable for senior management.
    caps = getattr(config, "ALERT_CAPS", {})
    if caps:
        capped = []
        for sev in ["critical", "warning", "watch"]:
            sev_df = alerts[alerts["severity"] == sev].copy()
            cap = caps.get(sev)
            if cap and len(sev_df) > cap:
                sev_df = sort_by_tier(sev_df).head(cap)
            capped.append(sev_df)
        alerts = pd.concat(capped, ignore_index=True) if capped else pd.DataFrame()

        imp_cap = caps.get("improvement")
        if imp_cap and len(improvements) > imp_cap:
            improvements = sort_by_tier(improvements).head(imp_cap)

    return pd.concat([alerts, improvements], ignore_index=True)


def _truncate(text, max_len=42) -> str:
    if not text or (isinstance(text, float) and pd.isna(text)):
        return ""
    text = str(text)
    return text if len(text) <= max_len else text[:max_len - 1] + "…"


def _short_trigger(triggered_by: str) -> str:
    return {
        "rolling":            f"{config.ROLLING_WINDOW_DAYS}-day avg",
        "yoy":               "vs last year",
        "both":              f"{config.ROLLING_WINDOW_DAYS}-day + YoY",
        "absolute_threshold": "Business rule",
    }.get(triggered_by, triggered_by)


def _deviation_display(row: pd.Series) -> str:
    """
    Show deviation as (actual - expected) in the metric's own units.

    For YoY-triggered alerts, uses the YoY baseline as expected so the
    deviation reflects how far the metric is from last year — not from the
    rolling mean (which is often close to actual and makes deviation look trivial).

    Positive = metric is above expected.
    Negative = metric is below expected.
    """
    metric     = row.get("metric", "")
    actual     = row.get("actual_value")
    triggered  = row.get("triggered_by", "")
    yoy_base   = row.get("yoy_baseline")

    # For YoY-triggered alerts, compare against the YoY baseline
    if triggered in ("yoy", "both") and yoy_base is not None and not (isinstance(yoy_base, float) and pd.isna(yoy_base)):
        expected = yoy_base
    else:
        expected = row.get("expected_value")

    try:
        if expected is None or actual is None:
            return "—"
        if pd.isna(expected) or pd.isna(actual):
            return "—"
        diff = actual - expected   # positive = actual above expected
        sign = "+" if diff >= 0 else ""
        if metric in PCT_METRICS:
            return f"{sign}{diff * 100:.1f}pp"
        if metric in DOLLAR_METRICS:
            sign = "+" if diff >= 0 else "-"
            return f"{sign}${abs(diff):,.2f}"
        if metric in RANK_METRICS:
            return f"{sign}{diff:,.0f}"
        if metric in RATING_METRICS:
            return f"{sign}{diff:.2f}★"
        if metric in COUNT_METRICS:
            return f"{sign}{diff:,.0f}"
        return f"{sign}{diff:,.2f}"
    except (TypeError, ValueError):
        return "—"


# =============================================================================
# HTML EMAIL BUILDER
# =============================================================================

_SEV_COLORS = {
    "critical":    {"bg": "#dc2626", "light": "#fef2f2", "border": "#fca5a5", "text": "#991b1b", "badge_bg": "#fee2e2"},
    "warning":     {"bg": "#d97706", "light": "#fffbeb", "border": "#fcd34d", "text": "#92400e", "badge_bg": "#fef3c7"},
    "watch":       {"bg": "#2563eb", "light": "#eff6ff", "border": "#93c5fd", "text": "#1e3a8a", "badge_bg": "#dbeafe"},
    "improvement": {"bg": "#16a34a", "light": "#f0fdf4", "border": "#86efac", "text": "#14532d", "badge_bg": "#dcfce7"},
}

_SEV_EMOJI = {"critical": "🔴", "warning": "🟡", "watch": "🔵", "improvement": "🟢"}

_TIER_BADGE_COLORS = {
    "homerun":          ("⚾", "#7c3aed", "#ede9fe"),
    "triple":           ("⚾", "#0369a1", "#e0f2fe"),
    "double":           ("⚾", "#065f46", "#d1fae5"),
    "single":           ("⚾", "#374151", "#f3f4f6"),
    "less_than_single": ("⚾", "#6b7280", "#f9fafb"),
}


def _html_tier_header(tier: str) -> str:
    label = TIER_LABELS.get(tier, tier.upper())
    emoji, color, bg = _TIER_BADGE_COLORS.get(tier, ("⚾", "#374151", "#f3f4f6"))
    return (
        '<tr><td colspan="6" style="background:{bg};padding:6px 24px;'
        'font-size:11px;font-weight:bold;color:{color};letter-spacing:0.04em;'
        'border-top:2px solid #e2e8f0;">{label}</td></tr>'
    ).format(bg=bg, color=color, label=label)


def _html_asin_row(asin: str, group_df: pd.DataFrame, sev: str, row_shade: bool) -> str:
    """
    Format a single ASIN (product) row in the HTML table.
    Groups all metric alerts for this ASIN into a single row with bulleted details.
    """
    colors = _SEV_COLORS[sev]
    first_row = group_df.iloc[0]
    title = str(first_row.get("title", "")) if first_row.get("title") and not (isinstance(first_row.get("title"), float) and pd.isna(first_row.get("title"))) else ""
    
    # Shared info (BSR, Unresolved Day Badge)
    bsr = first_row.get("category_bsr")
    bsr_html = ""
    if bsr and not (isinstance(bsr, float) and pd.isna(bsr)):
        bsr_html = (
            ' <span style="font-size:10px;color:#64748b;background:#f1f5f9;'
            'padding:1px 5px;border-radius:3px;margin-left:4px;">BSR {v:,}</span>'
        ).format(v=int(bsr))

    days = first_row.get("consecutive_days", 1)
    unresolved_html = ""
    if days > 1 and sev == "critical":
        unresolved_html = f' <span style="font-size:10px;background:#fef2f2;color:#dc2626;padding:1px 5px;border-radius:3px;border:1px solid #fca5a5;font-weight:bold;margin-left:6px;">DAY {days}</span>'

    bg = "#fafafa" if row_shade else "#ffffff"
    left_border = "border-left:3px solid {c};".format(c=colors["bg"])
    
    # 1. Product Info Column
    td_first = ('style="padding:10px 10px 10px 21px;border-bottom:1px solid #f1f5f9;'
                'vertical-align:top;font-size:12px;' + left_border + '"')
    product_html = (
        '<div style="font-weight:600;color:#1e293b;">{title}{unresolved}</div>'
        '<div style="font-size:10px;color:#94a3b8;margin-top:1px;">{asin}{bsr}</div>'
    ).format(title=title or asin, unresolved=unresolved_html, asin=asin, bsr=bsr_html)

    # 2. Issues Column (Bulleted Metrics)
    issues_html = '<ul style="margin:0;padding:0 0 0 14px;font-size:11px;color:#475569;line-height:1.4;">'
    for _, row in group_df.iterrows():
        metric = row.get("metric", "")
        actual = _fmt_value(row.get("actual_value"), metric)
        deviation = _deviation_display(row)
        triggered = _short_trigger(row.get("triggered_by", ""))
        issues_html += f'<li style="margin-bottom:3px;"><b>{metric}:</b> {actual} ({deviation}) via {triggered}</li>'
    issues_html += '</ul>'

    # 3. AI Suggestions & Insights Column
    # Determine most significant issue (max absolute z-score or priority metric)
    most_sig_idx = 0
    max_z = -1
    for i in range(len(group_df)):
        try:
            z = abs(float(group_df.iloc[i].get("z_score", 0)))
            if z > max_z:
                max_z = z
                most_sig_idx = i
        except: pass

    insights_html = '<div style="font-size:11px;color:#334155;line-height:1.4;">'
    for i in range(len(group_df)):
        row = group_df.iloc[i]
        insight = row.get("llm_insight", "")
        if not insight:
            insight = generate_plain_english(row)
            
        is_most = (i == most_sig_idx and len(group_df) > 1)
        prefix = '<b style="color:#0369a1;">[SIGNIFICANT]</b> ' if is_most else ""
        insights_html += f'<div style="margin-bottom:6px;">{prefix}{insight}</div>'
    insights_html += '</div>'

    # 4. Actions Column
    action_td_style = 'style="padding:10px;border-bottom:1px solid #f1f5f9;vertical-align:top;"'
    
    # Dismiss ASIN
    dismiss_url = f"{config.ACTION_BRIDGE_URL}?action=dismiss&asin={asin}"
    actions_html = (
        f'<div style="margin-bottom:6px;">'
        f'<a href="{dismiss_url}" style="color:#ef4444;text-decoration:none;font-size:11px;font-weight:bold;" '
        f'title="Stop alerting for this product">Dismiss ASIN ×</a>'
        f'</div>'
    )
    
    # Monday Buttons
    person_mapping = getattr(config, "MONDAY_PERSON_MAPPING", {})
    if person_mapping:
        actions_html += '<div style="font-size:9px;color:#94a3b8;margin-bottom:4px;text-transform:uppercase;font-weight:bold;">Add to Monday:</div>'
        actions_html += '<div style="display:flex;flex-wrap:wrap;gap:4px;">'
        for person in person_mapping:
            # We encode the summary to avoid breaking URLs
            import urllib.parse
            summary = f"Anomaly: {asin}"
            if len(group_df) == 1:
                summary += f" - {group_df.iloc[0].get('metric')}"
            summary_enc = urllib.parse.quote(summary)
            
            monday_url = f"{config.ACTION_BRIDGE_URL}?action=monday&asin={asin}&person={person}&summary={summary_enc}"
            actions_html += (
                f'<a href="{monday_url}" style="background:#ede9fe;color:#7c3aed;padding:2px 6px;'
                f'text-decoration:none;border-radius:3px;font-size:10px;font-weight:bold;border:1px solid #ddd6fe;white-space:nowrap;">'
                f'+ {person}</a>'
            )
        actions_html += '</div>'

    td = 'style="padding:10px;border-bottom:1px solid #f1f5f9;vertical-align:top;"'
    
    return (
        '<tr style="background:{bg};">'
        '<td {td_first}>{product_html}</td>'
        '<td {td}>{issues_html}</td>'
        '<td {td} style="width:35%;">{insights_html}</td>'
        '<td {td} style="width:15%;">{actions_html}</td>'
        '</tr>'
    ).format(
        bg=bg,
        td_first=td_first, td=td,
        product_html=product_html,
        issues_html=issues_html,
        insights_html=insights_html,
        actions_html=actions_html
    )


def _html_section(sev: str, df: pd.DataFrame) -> str:
    colors = _SEV_COLORS[sev]
    
    # Group by ASIN to determine unique product count
    unique_asins = df["asin"].unique() if not df.empty else []
    count = len(unique_asins)
    emoji = _SEV_EMOJI[sev]

    # Section header bar
    sev_label = "POSITIVE SIGNALS" if sev == "improvement" else sev.upper()
    item_word  = "product" if sev != "improvement" else "metric"
    header = (
        '<table width="100%" cellpadding="0" cellspacing="0" style="margin-top:16px;">'
        '<tr><td style="background:{bg};color:#fff;padding:10px 24px;'
        'font-weight:bold;font-size:14px;letter-spacing:0.02em;">'
        '{emoji} {sev_label} &mdash; {count} {item_word}{plural}'
        '</td></tr></table>'
    ).format(
        bg=colors["bg"], emoji=emoji,
        sev_label=sev_label, count=count,
        item_word=item_word,
        plural="s" if count != 1 else "",
    )

    if count == 0:
        if sev == "improvement":
            return ""   # Don't show the improvements section at all if nothing is improving
        return header + (
            '<table width="100%" cellpadding="0" cellspacing="0">'
            '<tr><td style="padding:10px 24px;color:#94a3b8;font-size:12px;">None</td></tr>'
            '</table>'
        )

    # Column headers
    col_headers = (
        '<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:collapse;">'
        '<thead><tr style="background:#f8fafc;">'
        '<th style="padding:6px 10px 6px 24px;text-align:left;font-size:10px;'
        'color:#94a3b8;border-bottom:2px solid #e2e8f0;width:25%;">PRODUCT</th>'
        '<th style="padding:6px 10px;text-align:left;font-size:10px;color:#94a3b8;'
        'border-bottom:2px solid #e2e8f0;width:25%;">ISSUES</th>'
        '<th style="padding:6px 10px;text-align:left;font-size:10px;color:#94a3b8;'
        'border-bottom:2px solid #e2e8f0;width:35%;">AI INSIGHTS & ACTIONS</th>'
        '<th style="padding:6px 10px;text-align:left;font-size:10px;color:#94a3b8;'
        'border-bottom:2px solid #e2e8f0;width:15%;">ACTIONS</th>'
        '</tr></thead><tbody>'
    )

    sorted_df = sort_by_tier(df)
    rows_html = ""
    current_tier = None
    shade = False

    # Grouping logic: Iterate through tiers, then group within tier by ASIN
    tier_order = {t: i for i, t in enumerate(TIER_SORT_ORDER)}
    sorted_df["_tier_rank"] = sorted_df["tier"].map(tier_order).fillna(len(TIER_SORT_ORDER))
    
    # We iterate by tier groups
    for row_tier in TIER_SORT_ORDER:
        tier_df = sorted_df[sorted_df["tier"] == row_tier]
        if tier_df.empty:
            continue
            
        # Add tier header
        rows_html += _html_tier_header(row_tier)
        shade = False
        
        # Group by ASIN within this tier
        tier_asins = tier_df["asin"].unique()
        for asin in tier_asins:
            asin_group = tier_df[tier_df["asin"] == asin]
            rows_html += _html_asin_row(asin, asin_group, sev, shade)
            shade = not shade

    return header + col_headers + rows_html + "</tbody></table>"


def _html_legend() -> str:
    """Render a compact severity legend explaining what each level means."""
    return """
  <!-- SEVERITY LEGEND -->
  <tr><td style="padding:10px 24px 0 24px;">
    <table cellpadding="0" cellspacing="0" style="width:100%;border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;">
      <tr style="background:#f8fafc;">
        <td colspan="2" style="padding:7px 14px;font-size:11px;font-weight:bold;color:#64748b;letter-spacing:0.05em;border-bottom:1px solid #e2e8f0;">SEVERITY GUIDE</td>
      </tr>
      <tr>
        <td style="padding:6px 14px;border-bottom:1px solid #f1f5f9;width:90px;">
          <span style="background:#fee2e2;color:#dc2626;font-weight:bold;font-size:11px;padding:2px 8px;border-radius:3px;">CRITICAL</span>
        </td>
        <td style="padding:6px 14px;font-size:12px;color:#475569;border-bottom:1px solid #f1f5f9;">Act today — metric has moved to an abnormal level that requires immediate investigation.</td>
      </tr>
      <tr>
        <td style="padding:6px 14px;border-bottom:1px solid #f1f5f9;">
          <span style="background:#fef3c7;color:#d97706;font-weight:bold;font-size:11px;padding:2px 8px;border-radius:3px;">WARNING</span>
        </td>
        <td style="padding:6px 14px;font-size:12px;color:#475569;border-bottom:1px solid #f1f5f9;">Investigate soon — meaningful deviation from normal. May worsen if not addressed.</td>
      </tr>
      <tr>
        <td style="padding:6px 14px;">
          <span style="background:#dbeafe;color:#2563eb;font-weight:bold;font-size:11px;padding:2px 8px;border-radius:3px;">WATCH</span>
        </td>
        <td style="padding:6px 14px;font-size:12px;color:#475569;">Monitor — early signal outside normal range. Could be noise or the start of a trend.</td>
      </tr>
    </table>
  </td></tr>
"""


def _pointed_reason(metric: str, asin: str, asin_flags: dict, top_return_reason=None) -> str:
    """
    Return one pointed observation derived from cross-metric context for this ASIN.
    Replaces the generic 4-item possible causes list.
    """
    flags = asin_flags.get(asin, set()) if asin_flags else set()

    if metric == "margin":
        if "tacos" in flags and "acos" in flags:
            return "Ad spend likely driver — both ACoS and TACoS also flagged for this product."
        elif "tacos" in flags:
            return "Ad spend likely driver — TACoS also flagged for this product."
        elif "acos" in flags:
            return "Ad spend likely driver — ACoS also flagged for this product."
        else:
            return "Check COGS or FBA fees — ad spend looks stable."

    elif metric == "sales":
        if "conversion_rate" in flags:
            return "Listing or pricing issue — conversion rate also down."
        else:
            return "Check inventory levels, listing visibility, or ad spend."

    elif metric == "return_rate":
        if top_return_reason and not (isinstance(top_return_reason, float) and pd.isna(top_return_reason)):
            return f"Top return reason on record: <em>{top_return_reason}</em>."
        return "Check FBA returns report for this ASIN."

    elif metric == "acos":
        if "sales" in flags:
            return "ACoS rising while sales also declining — check bid strategy or targeting."
        return "Review keyword bids and ad targeting."

    elif metric == "tacos":
        if "sales" in flags:
            return "TACoS rising as sales decline — ad spend not generating enough revenue."
        return "Check campaign budgets relative to total revenue."

    elif metric == "conversion_rate":
        if "sales" in flags:
            return "Both conversion and sales down — likely listing or pricing issue."
        return "Check listing content, images, or recent pricing changes."

    elif metric == "keyword_avg_rank":
        return "Check ad spend on key terms and listing relevance."

    elif metric == "review_rating":
        return "Check recent reviews for quality or fulfilment complaints."

    elif metric == "review_count":
        return "Check for unusual review activity or Amazon review removals."

    elif metric == "organic_top10_count":
        return "Check listing content changes and keyword targeting."

    return ""


def generate_plain_english(row: pd.Series, asin_flags: dict = None) -> str:
    """
    Generate a 1-2 sentence plain English explanation for a single alert row.
    Makes the alert understandable to any business stakeholder.

    Args:
        row: Single flagged alert row.
        asin_flags: Dict mapping asin → set of flagged metrics across the full run.
                    Used to derive a pointed cross-metric observation instead of a
                    generic cause list. Pass None to fall back to generic hints.
    """
    metric = row.get("metric", "")
    actual = row.get("actual_value")
    expected = row.get("expected_value")
    yoy_baseline = row.get("yoy_baseline")
    yoy_deviation = row.get("yoy_deviation")
    z_score = row.get("z_score")
    triggered_by = row.get("triggered_by", "")
    title = row.get("title", "")
    asin = row.get("asin", "")
    product_name = title if title and not (isinstance(title, float) and pd.isna(title)) else asin

    METRIC_LABELS = {
        "conversion_rate":    "Conversion rate",
        "return_rate":        "Return rate",
        "acos":               "ACoS",
        "tacos":              "TACoS",
        "sales":              "Sales",
        "margin":             "Margin",
        "keyword_avg_rank":   "Average keyword rank",
        "review_rating":      "Review rating",
        "review_count":       "Review count",
        "organic_top10_count": "Organic Top-10 keyword count",
    }
    label = METRIC_LABELS.get(metric, metric.replace("_", " ").title())

    top_reason = row.get("top_return_reason")
    reason_suffix = ""
    if metric == "return_rate" and top_reason and not (isinstance(top_reason, float) and pd.isna(top_reason)):
        reason_suffix = f" Top return reason: <em>{top_reason}</em>."

    # Cross-metric pointed reason (replaces generic 4-item cause list)
    pointed = _pointed_reason(metric, asin, asin_flags, top_reason)
    causes_suffix = f" <span style='color:#64748b;font-size:11px;'>{pointed}</span>" if pointed else ""

    actual_fmt   = _fmt_value(actual, metric)
    expected_fmt = _fmt_value(expected, metric)

    # Margin daily dollar impact: (margin_pp_drop) × sales_roll_mean = daily $ lost vs last year
    margin_impact_suffix = ""
    if metric == "margin" and triggered_by in ("yoy", "both") and yoy_baseline is not None:
        try:
            sales_roll = row.get("sales_roll_mean")
            if (sales_roll is not None and not pd.isna(sales_roll) and sales_roll > 0
                    and actual is not None and not pd.isna(actual)
                    and not pd.isna(yoy_baseline)):
                daily_loss = abs(actual - yoy_baseline) * sales_roll
                margin_impact_suffix = (
                    f" <span style='color:#dc2626;font-size:11px;font-weight:600;'>"
                    f"~${daily_loss:,.0f}/day in lost margin.</span>"
                )
        except (TypeError, ValueError):
            pass

    # For both-direction metrics, determine if the move was up or down
    move_direction = ""
    if metric in ("review_rating", "review_count") and z_score is not None:
        try:
            if not pd.isna(z_score):
                move_direction = "increased" if z_score > 0 else "decreased"
        except (TypeError, ValueError):
            pass

    if triggered_by == "absolute_threshold":
        return (
            f"{label} for <strong>{product_name}</strong> has hit a business-critical threshold "
            f"at {actual_fmt}. This triggers an alert regardless of trend.{reason_suffix}{causes_suffix}"
        )

    if triggered_by in ("yoy", "both") and yoy_baseline is not None:
        try:
            if not pd.isna(yoy_baseline) and actual is not None and not pd.isna(actual):
                diff = actual - yoy_baseline
                abs_direction = "above" if diff >= 0 else "below"
                if metric in PCT_METRICS:
                    abs_diff_fmt = f"{abs(diff) * 100:.1f}pp"
                elif metric in DOLLAR_METRICS:
                    abs_diff_fmt = f"${abs(diff):,.2f}"
                else:
                    abs_diff_fmt = f"{abs(diff):,.2f}"
                yoy_fmt = _fmt_value(yoy_baseline, metric)
                return (
                    f"{label} for <strong>{product_name}</strong> is {actual_fmt} — "
                    f"{abs_diff_fmt} {abs_direction} the same week last year ({yoy_fmt}). "
                    f"The {config.ROLLING_WINDOW_DAYS}-day average was {expected_fmt}.{margin_impact_suffix}{reason_suffix}{causes_suffix}"
                )
        except (TypeError, ValueError):
            pass

    if z_score is not None:
        try:
            if not pd.isna(z_score):
                direction = "above" if z_score > 0 else "below"
                move_note = f" ({move_direction} unusually fast)" if move_direction else ""
                return (
                    f"{label} for <strong>{product_name}</strong> is {actual_fmt}{move_note}, "
                    f"{abs(z_score):.1f} standard deviations {direction} its {config.ROLLING_WINDOW_DAYS}-day average of {expected_fmt}. "
                    f"This is an unusual move for this product.{reason_suffix}{causes_suffix}"
                )
        except (TypeError, ValueError):
            pass

    return f"{label} for <strong>{product_name}</strong> is {actual_fmt} vs expected {expected_fmt}.{reason_suffix}"


def _html_top10_explanations(grouped_alerts: Dict[str, pd.DataFrame]) -> str:
    """
    Build an HTML section with plain-English explanations for the top alerts.
    Collapsed by product (ASIN) to prevent repetition.
    Picks Criticals first (sorted by tier), then Warnings.
    """
    all_summary_rows = []
    for sev in ["critical", "warning"]:
        df = grouped_alerts.get(sev, pd.DataFrame())
        if not df.empty:
            all_summary_rows.append(sort_by_tier(df))
    
    if not all_summary_rows:
        return ""
        
    combined = pd.concat(all_summary_rows, ignore_index=True)
    if combined.empty:
        return ""

    # Build cross-metric context for _pointed_reason
    asin_flags: Dict[str, set] = {}
    for sev, df in grouped_alerts.items():
        if sev == "improvement" or df.empty:
            continue
        for _, r in df.iterrows():
            a = r.get("asin", "")
            m = r.get("metric", "")
            if a:
                asin_flags.setdefault(a, set()).add(m)

    # Group by ASIN to ensure each product only appears once in the executive summary
    # We take the maximum severity found for each ASIN to determine the color.
    rows_html = ""
    processed_asins = set()
    counter = 1
    
    # Iterate through combined in origin tier/severity order
    for _, row in combined.iterrows():
        asin = row.get("asin")
        if asin in processed_asins:
            continue
        processed_asins.add(asin)
        
        # Get all alerts for this specific ASIN to combine their text
        asin_df = combined[combined["asin"] == asin]
        
        # Use the most severe alert's color
        best_sev = asin_df["severity"].iloc[0] 
        colors = _SEV_COLORS.get(best_sev, _SEV_COLORS["watch"])
        
        # Build consolidated explanation
        explanations = []
        for _, alert_row in asin_df.iterrows():
            expl = generate_plain_english(alert_row, asin_flags=asin_flags)
            explanations.append(expl)
        
        # If multiple alerts, combine them with bullet points or semi-colons
        if len(explanations) > 1:
            # First sentence usually identifies the product, subsequent sentences add metrics
            # We'll just join them into a list
            final_explanation = "<br/>&bull; ".join(explanations)
            if not final_explanation.startswith("&bull;"):
                final_explanation = "&bull; " + final_explanation
        else:
            final_explanation = explanations[0]

        rows_html += (
            '<tr style="background:{bg};">'
            '<td style="padding:10px 14px;font-size:12px;vertical-align:top;width:24px;'
            'color:{num_color};font-weight:bold;">{i}.</td>'
            '<td style="padding:10px 14px 10px 0;font-size:12px;color:#334155;line-height:1.5;">'
            '{explanation}</td>'
            '</tr>'
        ).format(
            bg="#fafafa" if counter % 2 == 0 else "#ffffff",
            num_color=colors["bg"],
            i=counter,
            explanation=final_explanation,
        )
        counter += 1
        if counter > 10: # Cap at 10 unique ASINs for the high-level summary
            break

    return (
        '<tr><td style="padding:16px 24px 0 24px;">'
        '<div style="font-size:11px;font-weight:bold;color:#64748b;letter-spacing:0.05em;'
        'margin-bottom:8px;text-transform:uppercase;">Top Alerts — Executive Summary</div>'
        '<table width="100%" cellpadding="0" cellspacing="0" '
        'style="border:1px solid #e2e8f0;border-radius:6px;overflow:hidden;">'
        '{rows}'
        '</table></td></tr>'
    ).format(rows=rows_html)


def build_html_body(grouped_alerts: Dict[str, pd.DataFrame], run_date: str, data_date: str = None, 
                    source_status: dict = None, llm_summary: str = None) -> str:
    """
    Build a scannable HTML email body — one row per alert, color-coded by severity,
    grouped by tier within each severity section.

    Designed to be readable in 2–4 minutes by a business stakeholder.
    Uses inline CSS for compatibility with Gmail and Outlook.

    Args:
        grouped_alerts: Dict of severity → dataframe from group_alerts_by_severity().
        run_date: Today's date string (YYYY-MM-DD) — when the pipeline was run.
        data_date: Actual latest date in the data (YYYY-MM-DD). Shown as "Data through" in
                   the email. If not provided, falls back to run_date - 2 days (T-2 estimate).
        source_status: Dict of source -> status info (from load_data.py).
        llm_summary: Executive summary from Claude 3.5 Sonnet.
    """
    all_dfs = [df for df in grouped_alerts.values() if not df.empty]
    total_alerts = sum(len(df) for df in grouped_alerts.values())
    unique_asins = len(pd.concat(all_dfs, ignore_index=True)["asin"].unique()) if all_dfs else 0

    n_critical    = len(grouped_alerts.get("critical",    []))
    n_warning     = len(grouped_alerts.get("warning",     []))
    n_watch       = len(grouped_alerts.get("watch",       []))
    n_improvement = len(grouped_alerts.get("improvement", []))

    try:
        run_dt = datetime.strptime(run_date, "%Y-%m-%d")
        run_date_fmt = run_dt.strftime("%b %d, %Y")
        if data_date:
            data_dt = datetime.strptime(data_date, "%Y-%m-%d")
            data_through = data_dt.strftime("%b %d, %Y")
        else:
            data_dt = run_dt - timedelta(days=2)
            data_through = data_dt.strftime("%b %d, %Y")
    except Exception:
        data_through = data_date or "T-2"
        run_date_fmt = run_date
        data_dt = None

    sections = "".join(_html_section(sev, grouped_alerts.get(sev, pd.DataFrame()))
                       for sev in SEVERITY_SORT_ORDER)
    improvements_section = _html_section("improvement", grouped_alerts.get("improvement", pd.DataFrame()))

    # Build Lag Report HTML
    lag_html = ""
    if source_status:
        lag_items = []
        for src, info in source_status.items():
            color = "#dc2626" if info["status"] == "LAGGING" else "#16a34a"
            lag_items.append(f'<span style="color:{color};font-weight:bold;">{src.upper()}: {info["status"]}</span>')
        lag_html = ' &nbsp;&bull;&nbsp; '.join(lag_items)

    # Rerun Button HTML (using bridge URL from config)
    rerun_url = f"{config.RERUN_BRIDGE_URL}?action=rerun&date={data_date or run_date}"
    rerun_button = (
        f'<div style="margin-top:12px;">'
        f'<a href="{rerun_url}" style="background:#2563eb;color:#ffffff;padding:8px 16px;'
        f'text-decoration:none;border-radius:6px;font-size:12px;font-weight:bold;">'
        f'Run Pipeline for {data_through}</a>'
        f'</div>'
    )

    # LLM Summary Section
    llm_box = ""
    if llm_summary:
        llm_box = f"""
  <tr><td style="padding:16px 24px 0 24px;">
    <div style="background:#f0f9ff;border:1px solid #bae6fd;border-radius:8px;padding:16px;">
      <div style="font-size:11px;font-weight:bold;color:#0369a1;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:6px;">Claude 3.5 Sonnet Analysis</div>
      <div style="font-size:14px;color:#0c4a6e;line-height:1.5;">{llm_summary}</div>
    </div>
  </td></tr>
        """

    html = """<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;font-family:Arial,Helvetica,sans-serif;font-size:13px;color:#1e293b;background:#f8fafc;">
<table width="100%" cellpadding="0" cellspacing="0" style="max-width:900px;margin:0 auto;background:#ffffff;border:1px solid #e2e8f0;">

  <!-- HEADER -->
  <tr><td style="background:#1e293b;padding:16px 24px;">
    <span style="color:#ffffff;font-size:20px;font-weight:bold;">Six10 Anomaly Digest</span>
    <span style="color:#94a3b8;font-size:12px;margin-left:16px;">
      Run: {run_date_fmt} &nbsp;&bull;&nbsp; Data through: {data_through}
    </span>
    <div style="margin-top:8px;font-size:11px;color:#94a3b8;">
      {lag_html}
    </div>
    {rerun_button}
  </td></tr>

  <!-- SUMMARY STRIP -->
  <tr><td style="background:#f1f5f9;padding:14px 24px;border-bottom:1px solid #e2e8f0;">
    <table cellpadding="0" cellspacing="0"><tr>
      <td style="background:#fee2e2;border:1px solid #fca5a5;border-radius:6px;
                 padding:10px 22px;text-align:center;">
        <div style="font-size:26px;font-weight:bold;color:#dc2626;line-height:1;">{n_critical}</div>
        <div style="font-size:10px;color:#991b1b;text-transform:uppercase;margin-top:3px;letter-spacing:0.05em;">Critical</div>
      </td>
      <td style="width:8px;"></td>
      <td style="background:#fef3c7;border:1px solid #fcd34d;border-radius:6px;
                 padding:10px 22px;text-align:center;">
        <div style="font-size:26px;font-weight:bold;color:#d97706;line-height:1;">{n_warning}</div>
        <div style="font-size:10px;color:#92400e;text-transform:uppercase;margin-top:3px;letter-spacing:0.05em;">Warning</div>
      </td>
      <td style="width:8px;"></td>
      <td style="background:#dbeafe;border:1px solid #93c5fd;border-radius:6px;
                 padding:10px 22px;text-align:center;">
        <div style="font-size:26px;font-weight:bold;color:#2563eb;line-height:1;">{n_watch}</div>
        <div style="font-size:10px;color:#1e3a8a;text-transform:uppercase;margin-top:3px;letter-spacing:0.05em;">Watch</div>
      </td>
      <td style="width:8px;"></td>
      <td style="background:#dcfce7;border:1px solid #86efac;border-radius:6px;
                 padding:10px 22px;text-align:center;">
        <div style="font-size:26px;font-weight:bold;color:#16a34a;line-height:1;">{n_improvement}</div>
        <div style="font-size:10px;color:#14532d;text-transform:uppercase;margin-top:3px;letter-spacing:0.05em;">Improving</div>
      </td>
      <td style="width:24px;"></td>
      <td style="color:#475569;font-size:13px;vertical-align:middle;">
        <strong style="font-size:18px;color:#1e293b;">{unique_asins}</strong>
        <span style="color:#64748b;"> ASINs affected</span>
      </td>
    </tr></table>
  </td></tr>

  <!-- AI SUMMARY -->
  {llm_box}

  <!-- SEVERITY LEGEND + TOP 10 + ALERT SECTIONS -->
  {legend}
  {top10}
  <tr><td style="padding:0 0 8px 0;">{sections}</td></tr>
  <tr><td style="padding:0 0 8px 0;">{improvements_section}</td></tr>

  <!-- FOOTER -->
  <tr><td style="padding:12px 24px;background:#f8fafc;border-top:2px solid #e2e8f0;
                 color:#94a3b8;font-size:11px;line-height:1.6;">
    Run date: {run_date} &nbsp;&bull;&nbsp;
    Data through: {data_through} &nbsp;&bull;&nbsp;
    Total: {total_alerts} alerts across {unique_asins} ASINs &nbsp;&bull;&nbsp;
    Thresholds are starting points &mdash; tune after launch.
  </td></tr>

</table>
</body></html>""".format(
        run_date_fmt=run_date_fmt,
        data_through=data_through,
        lag_html=lag_html,
        rerun_button=rerun_button,
        n_critical=n_critical,
        n_warning=n_warning,
        n_watch=n_watch,
        n_improvement=n_improvement,
        unique_asins=unique_asins,
        llm_box=llm_box,
        legend=_html_legend(),
        top10=_html_top10_explanations(grouped_alerts),
        sections=sections,
        improvements_section=improvements_section,
        run_date=run_date,
        total_alerts=total_alerts,
    )

    return html


def build_alert_payload(flagged_df: pd.DataFrame, run_date: str, data_date: str = None, 
                        source_status: dict = None, llm_summary: str = None) -> dict:
    """
    Full alert building pipeline — takes detection output, returns subject + body.

    Args:
        flagged_df: Dataframe of flagged rows from get_flagged_rows().
        run_date: Today's run date string (YYYY-MM-DD).
        data_date: Actual latest date in the data (YYYY-MM-DD). Displayed as "Data through"
                   in the email. If not provided, falls back to run_date - 2 days.
        source_status: Dict of source -> status info (from load_data.py).
        llm_summary: Executive summary from Claude 3.5 Sonnet.

    Returns:
        Dict with keys 'subject' (str), 'body' (str), 'content_type' (str).
    """
    if flagged_df is None or flagged_df.empty:
        # Build a "No alerts" but "Still Reporting" email if data was found
        # Or a "Missing Data" email if sources are lagging
        lag_msg = ""
        if source_status:
            lagging = [s for s, info in source_status.items() if info["status"] == "LAGGING"]
            if lagging:
                lag_msg = f"\n\nCRITICAL DATA LAG DETECTED: {', '.join(lagging).upper()}.\nPlease update the relevant CSV/Excel files in Google Drive for {run_date}."

        return {
            "subject": f"[Six10 Alerts] {run_date} | No alerts today",
            "body": build_html_body({}, run_date, data_date=data_date, source_status=source_status, llm_summary=llm_summary),
            "content_type": "html"
        }

    grouped = group_alerts_by_severity(flagged_df)
    subject = build_email_subject(grouped, run_date)
    body = build_html_body(grouped, run_date, data_date=data_date, source_status=source_status, llm_summary=llm_summary)
    return {"subject": subject, "body": body, "content_type": "html"}
