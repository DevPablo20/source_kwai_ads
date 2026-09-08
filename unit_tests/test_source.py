import logging

import requests_mock

from source_kwai_ads.source import SourceKwaiAds

BASE_URL = "https://developers.kwai.com"

CONFIG = {
    "client_id": "cid",
    "client_secret": "csecret",
    "refresh_token": "rtoken",
    "agent_id": 76311496,
    "start_date": "2026-08-01",
}


def test_streams_returns_the_five_expected_streams():
    source = SourceKwaiAds()
    streams = source.streams(CONFIG)
    assert sorted(s.name for s in streams) == sorted(["advertisers", "campaigns", "ad_groups", "ads", "ads_reports_daily"])


def test_check_connection_succeeds_with_valid_credentials():
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(
            f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp",
            json={"status": "OK", "message": "", "data": {"total": 1, "data": [{"accountId": 76837727}]}},
        )
        ok, error = source.check_connection(logging.getLogger("airbyte"), CONFIG)
    assert ok is True
    assert error is None


def test_check_connection_uses_access_token_header():
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(
            f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp",
            json={"status": "OK", "message": "", "data": {"total": 1, "data": [{"accountId": 76837727}]}},
        )
        source.check_connection(logging.getLogger("airbyte"), CONFIG)
        account_request = next(r for r in m.request_history if "crmAccountQueryByAgentOrCorp" in r.url)
    assert account_request.headers.get("Access-Token") == "tok"


def test_check_connection_fails_gracefully_on_http_error():
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp", status_code=403, json={"message": "forbidden"})
        ok, error = source.check_connection(logging.getLogger("airbyte"), CONFIG)
    assert ok is False
    assert error is not None


def test_check_connection_with_account_ids_never_calls_account_listing_endpoint():
    # Some app registrations get rejected outright on crmAccountQueryByAgentOrCorp
    # (see source_kwai_ads/streams/advertisers.py). When account_ids is configured,
    # check_connection must probe a report endpoint directly instead.
    config = {**CONFIG, "account_ids": [76837727]}
    del config["agent_id"]
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(
            f"{BASE_URL}/rest/n/mapi/report/dspCampaignEffectQuery",
            json={"status": 200, "data": {"total": 1, "data": [{"campaignId": 1, "campaignName": "C"}]}},
        )
        ok, error = source.check_connection(logging.getLogger("airbyte"), config)
    assert ok is True
    assert error is None
    assert not any("crmAccountQueryByAgentOrCorp" in r.url for r in m.request_history)


CORP_CONFIG = {
    "client_id": "cid",
    "client_secret": "csecret",
    "refresh_token": "rtoken",
    "corp_id": 2148,
    "start_date": "2026-08-01",
    "end_date": "2026-08-03",
}


def test_every_report_stream_partitions_by_all_listed_accounts():
    """
    All four report streams must slice over the full account list discovered from
    the parent -- not just whichever one happens to read it first.

    The parent is consumed through `Stream.read()`, which records completion in a
    resumable full-refresh stream's own cursor. A single shared `Advertisers`
    instance therefore returns its accounts to the first report stream and an empty
    list to the other three, which then sync nothing at all while reporting success.
    This went unnoticed for as long as `account_ids` was always configured, since
    that path never touches the parent.
    """
    from airbyte_cdk.models import SyncMode

    accounts = [{"accountId": 100 + i, "accountName": f"acct {i}"} for i in range(38)]
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(
            f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp",
            json={"status": 200, "data": {"total": len(accounts), "data": accounts}},
        )
        streams = {s.name: s for s in source.streams(CORP_CONFIG)}
        expected = {a["accountId"] for a in accounts}
        for name in ("campaigns", "ad_groups", "ads", "ads_reports_daily"):
            slices = list(streams[name].stream_slices(sync_mode=SyncMode.full_refresh))
            assert {s["account_id"] for s in slices} == expected, f"{name} did not slice over every account"


def test_report_stream_slices_are_repeatable_on_the_same_instance():
    # Re-slicing one stream must not exhaust its own parent either.
    from airbyte_cdk.models import SyncMode

    accounts = [{"accountId": 1}, {"accountId": 2}]
    source = SourceKwaiAds()
    with requests_mock.Mocker() as m:
        m.get(f"{BASE_URL}/oauth/token", json={"data": {"access_token": "tok", "expires_in": 3599}})
        m.post(
            f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp",
            json={"status": 200, "data": {"total": 2, "data": accounts}},
        )
        campaigns = {s.name: s for s in source.streams(CORP_CONFIG)}["campaigns"]
        first = list(campaigns.stream_slices(sync_mode=SyncMode.full_refresh))
        second = list(campaigns.stream_slices(sync_mode=SyncMode.full_refresh))
    assert [s["account_id"] for s in first] == [1, 2]
    assert [s["account_id"] for s in second] == [1, 2]
