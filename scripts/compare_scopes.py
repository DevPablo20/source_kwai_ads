#!/usr/bin/env python
"""
Compare what two (or more) sets of Kwai MAPI credentials can actually reach.

Kwai's OAuth response advertises a `scope` string, but that string has proved to
be a poor predictor of what an app registration may call -- a "channel developer"
app carrying `ad_mapi_report` is still refused by `crmAccountQueryByAgentOrCorp`.
So this probes the endpoints themselves and reports the observed result per
credential set, rather than trusting the advertised scope.

Usage:
    python scripts/compare_scopes.py secrets/config.json secrets/config_new.json
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

AUTH_BASE = "https://business.kwai.com"
API_BASE = "https://developers.kwai.com"

# (label, path, extra body fields). Report endpoints get the account/date window
# merged in; account-listing endpoints get agentId/corpId instead.
REPORT_ENDPOINTS = [
    ("campaigns (dspCampaignEffectQuery)", "/rest/n/mapi/report/dspCampaignEffectQuery", {"granularity": 1}),
    ("ad_groups (dspUnitEffectQuery)", "/rest/n/mapi/report/dspUnitEffectQuery", {"granularity": 1}),
    ("ads (dspCreativeEffectQuery)", "/rest/n/mapi/report/dspCreativeEffectQuery", {"granularity": 1}),
    ("daily (dspCreativeEffectQuery g=3)", "/rest/n/mapi/report/dspCreativeEffectQuery", {"granularity": 3}),
    (
        "population (dspPopulationAnalysisEffectQuery)",
        "/rest/n/mapi/report/dspPopulationAnalysisEffectQuery",
        {"granularity": 1, "effectReportQueryTargetEnum": "EFFECT_TARGET_CREATIVE", "populationAnalysisTypeEnum": 0},
    ),
]


def refresh(config: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.get(
        f"{AUTH_BASE}/oauth/token",
        params={
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"],
            "client_id": config["client_id"],
            "client_secret": config["client_secret"],
        },
        timeout=30,
    )
    try:
        return resp.json()
    except ValueError:
        return {"_http": resp.status_code, "_raw": resp.text[:300]}


def call(token: str, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
    resp = requests.post(f"{API_BASE}{path}", headers={"Access-Token": token, "Content-Type": "application/json"}, json=body, timeout=60)
    try:
        return resp.json()
    except ValueError:
        return {"_http": resp.status_code, "_raw": resp.text[:300]}


def describe(payload: Dict[str, Any]) -> str:
    """Collapse a Kwai response into one line: rows returned, or the error it gave."""
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("data"), list):
        rows = data["data"]
        return f"OK    rows={len(rows):<5} total={data.get('total')}"
    if isinstance(data, list):
        return f"OK    rows={len(data)}"
    code = payload.get("result", payload.get("status", payload.get("_http")))
    msg = payload.get("err_msg") or payload.get("message") or payload.get("_raw") or ""
    return f"DENIED result={code} {str(msg)[:90]}"


def window(days: int = 30):
    tz = timezone(timedelta(hours=-3))
    today = datetime.now(tz).date()
    start = datetime.combine(today - timedelta(days=days), datetime.min.time(), tzinfo=tz)
    end = datetime.combine(today, datetime.min.time(), tzinfo=tz) - timedelta(milliseconds=1)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)


def audit(label: str, config: Dict[str, Any]) -> Dict[str, Any]:
    print(f"\n{'=' * 78}\n{label}\n{'=' * 78}")
    print(f"client_id     : {config.get('client_id')}")
    print(f"agent_id      : {config.get('agent_id')}   corp_id: {config.get('corp_id')}")
    print(f"account_ids   : {config.get('account_ids')}")

    token_resp = refresh(config)
    token = token_resp.get("access_token")
    if not token:
        print(f"\n  TOKEN REFRESH FAILED: {json.dumps(token_resp)[:300]}")
        return {"label": label, "token": None, "scope": None, "accounts": [], "endpoints": {}}

    scope = token_resp.get("scope")
    print(f"advertised scope: {scope!r}   expires_in={token_resp.get('expires_in')}")

    result: Dict[str, Any] = {"label": label, "token": token, "scope": scope, "endpoints": {}, "accounts": []}

    # --- account listing -------------------------------------------------
    print("\n-- account listing (crmAccountQueryByAgentOrCorp) --")
    for key, field in (("corp_id", "corpId"), ("agent_id", "agentId")):
        if config.get(key) is None:
            continue
        payload = call(token, "/rest/n/mapi/report/crmAccountQueryByAgentOrCorp", {field: config[key], "pageNo": 1, "pageSize": 500})
        line = describe(payload)
        print(f"  by {field:8}={config[key]:<12} {line}")
        result["endpoints"][f"accounts_by_{field}"] = line
        rows = ((payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}).get("data") or []
        for row in rows:
            if row.get("accountId") is not None:
                result["accounts"].append({"accountId": row.get("accountId"), "accountName": row.get("accountName")})

    # --- per-account report reach ---------------------------------------
    begin, end = window()
    probe_accounts: List[int] = [a["accountId"] for a in result["accounts"]] or list(config.get("account_ids") or [])
    if not probe_accounts:
        print("\n  (no accounts to probe -- neither listing nor account_ids yielded any)")
        return result

    print(f"\n-- report endpoints, last 30 days, {len(probe_accounts)} account(s) --")
    for account_id in probe_accounts:
        print(f"  account {account_id}:")
        for name, path, extra in REPORT_ENDPOINTS:
            body = {
                "accountId": account_id,
                "dataBeginTime": begin,
                "dataEndTime": end,
                "timeZoneIana": "UTC-3",
                "pageNo": 1,
                "pageSize": 500,
                **extra,
            }
            line = describe(call(token, path, body))
            print(f"    {name:46} {line}")
            result["endpoints"][f"{account_id}:{name}"] = line
    return result


def main(paths: List[str]) -> None:
    audits = []
    for path in paths:
        audits.append(audit(path, json.load(open(path))))

    if len(audits) < 2:
        return

    print(f"\n\n{'=' * 78}\nSCOPE DIFF\n{'=' * 78}")
    for a, b in zip(audits, audits[1:]):
        acc_a = {x["accountId"] for x in a["accounts"]}
        acc_b = {x["accountId"] for x in b["accounts"]}
        print(f"\n{a['label']}  vs  {b['label']}")
        print(f"  advertised scope : {a['scope']!r}  vs  {b['scope']!r}")
        print(f"  accounts listed  : {len(acc_a)} vs {len(acc_b)}")
        if acc_a - acc_b:
            print(f"    only in {a['label']}: {sorted(acc_a - acc_b)}")
        if acc_b - acc_a:
            print(f"    only in {b['label']}: {sorted(acc_b - acc_a)}")
        if acc_a & acc_b:
            print(f"    in both: {sorted(acc_a & acc_b)}")
        keys = sorted(set(a["endpoints"]) | set(b["endpoints"]))
        diffs = [(k, a["endpoints"].get(k, "-- not probed --"), b["endpoints"].get(k, "-- not probed --")) for k in keys]
        diffs = [d for d in diffs if d[1].split()[0] != d[2].split()[0]]
        if diffs:
            print("  endpoints that differ in reachability:")
            for k, va, vb in diffs:
                print(f"    {k}\n       {a['label']}: {va}\n       {b['label']}: {vb}")
        else:
            print("  endpoint reachability: identical")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    main(sys.argv[1:])
