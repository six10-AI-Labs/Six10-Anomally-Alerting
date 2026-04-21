# alerting/llm_assistant.py
# Layer 6 — Claude 3.5 Sonnet integration for alert sanity checking and summarization.

import os
import json
import pandas as pd
from anthropic import Anthropic
from typing import Tuple, Optional, List

def run_llm_analysis(flagged_df: pd.DataFrame, source_status: dict, run_date: str, data_date: str) -> Tuple[pd.DataFrame, str]:
    """
    Use Claude 3.5 Sonnet to:
    1. Sanity check the flagged alerts for consistency.
    2. Remove obvious false positives.
    3. Generate a concise plain-English summary of the most critical issues.

    Returns:
        Tuple of (Filtered DataFrame, Summary String)
    """
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("  [LLM] No API key found, skipping enhancement.")
        return flagged_df, ""

    if flagged_df.empty:
        return flagged_df, "No anomalies detected today."

    client = Anthropic(api_key=api_key)
    
    # Prepare data for LLM (token optimization: keep only essential columns)
    cols_to_send = ["asin", "title", "metric", "actual_value", "expected_value", "yoy_baseline", "severity", "triggered_by", "consecutive_days"]
    llm_input_df = flagged_df[cols_to_send].copy()
    
    # Format source status for LLM context
    lag_context = "\n".join([f"- {k}: {v['status']} (Latest: {v['latest_date']})" for k, v in source_status.items()])

    prompt = f"""
You are an expert Amazon business analyst. You are reviewing an automated anomaly report for Six10 Ventures.
Current Run Date: {run_date}
Data Date: {data_date}

Source Data Status:
{lag_context}

Below is the list of flagged anomalies (statistics-based):
{llm_input_df.to_json(orient='records')}

STRICT BUSINESS RULES FOR YOUR ANALYSIS:
1. CONSISTENCY CHECK: If a source is LAGGING (e.g., Sellerise), metrics from that source might be unreliable. Note this.
2. PATTERN RECOGNITION: If one ASIN has 5+ alerts across different metrics, it's likely a major operational issue (e.g., listing suppressed).
3. SANITY CHECK: Flag any alert that looks like statistical noise (e.g., tiny absolute changes that triggered a high Z-score).
4. RELEVANCE: Focus on Critical and Warning alerts for top-tier ASINs.

YOUR TASKS:
1. Identify any anomalies that should be DISMISSED as likely false positives/noise. Return their ASIN and Metric pairs in a JSON block.
2. Write a 2-3 sentence "Executive Summary" for the email header. Focus on the #1 most critical problem.
3. Suggest 1 "Pointed Action" for the top 3 most severe issues.

RESPONSE FORMAT:
Your response must be a JSON object with exactly these keys:
- "executive_summary": "string"
- "dismissed_alerts": [{"asin": "...", "metric": "..."}]
- "issue_insights": [{"asin": "...", "insight": "...", "action": "..."}]

Optimize for token usage. Be extremely concise.
"""

    try:
        response = client.messages.create(
            model="claude-3-5-sonnet-20240620",
            max_tokens=1000,
            temperature=0,
            system="You are a senior analyst at Six10 Ventures. Your goal is to ensure the daily alert report is 100% accurate and actionable. Always respond with raw JSON.",
            messages=[{"role": "user", "content": prompt}]
        )
        
        response_text = response.content[0].text
        
        # Robust JSON extraction
        import re
        json_match = re.search(r'\{.*\}', response_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group(0))
        else:
            result = json.loads(response_text)
        
        # 1. Apply LLM dismissals
        dismissed = result.get("dismissed_alerts", [])
        if dismissed:
            print(f"  [LLM] Dismissing {len(dismissed)} likely false positives.")
            for d in dismissed:
                flagged_df = flagged_df[~((flagged_df['asin'] == d['asin']) & (flagged_df['metric'] == d['metric']))]

        # 2. Extract summary
        exec_summary = result.get("executive_summary", "")
        
        # 3. Add LLM insights back to the dataframe for display in the email
        flagged_df['llm_insight'] = ""
        for insight in result.get("issue_insights", []):
            mask = flagged_df['asin'] == insight['asin']
            if mask.any():
                combined_text = f"{insight['insight']} <strong>Action:</strong> {insight['action']}"
                flagged_df.loc[mask, 'llm_insight'] = combined_text

        return flagged_df, exec_summary

    except Exception as e:
        print(f"  [LLM ERROR] Failed to run Claude analysis: {e}")
        return flagged_df, ""
