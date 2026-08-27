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
