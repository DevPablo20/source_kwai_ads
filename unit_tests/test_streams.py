import datetime

import pytest
import requests
import requests_mock
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http.error_handlers import ResponseAction

from source_kwai_ads.streams.advertisers import Advertisers
from source_kwai_ads.streams.ads_reports_daily import AdsReportsDaily
from source_kwai_ads.streams.base import KwaiStream
from source_kwai_ads.streams.reports import AdGroups, Ads, Campaigns

BASE_URL = "https://developers.kwai.com"


class _DummyStream(KwaiStream):
    primary_key = "id"

    def path(self, **kwargs):
        return "/rest/n/mapi/report/dummy"

    def request_body_params(self, stream_state, stream_slice=None, next_page_token=None):
        return {"accountId": 123}


@pytest.fixture
def dummy_stream():
    return _DummyStream(page_size=2, authenticator=None)


@pytest.fixture
def advertisers():
    return Advertisers(agent_id=76311496, authenticator=None)


class TestPagination:
    def test_next_page_token_advances_while_under_total(self, dummy_stream):
        with requests_mock.Mocker() as m:
            m.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"status": "OK", "message": "", "data": {"total": 3, "data": [{"id": 1}, {"id": 2}]}})
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"pageNo": 1, "pageSize": 2})
            assert dummy_stream.next_page_token(resp) == {"pageNo": 2}

    def test_next_page_token_stops_when_page_covers_total(self, dummy_stream):
        with requests_mock.Mocker() as m:
            m.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"status": "OK", "message": "", "data": {"total": 3, "data": [{"id": 3}]}})
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"pageNo": 2, "pageSize": 2})
            assert dummy_stream.next_page_token(resp) is None

    def test_next_page_token_stops_on_empty_page(self, dummy_stream):
        with requests_mock.Mocker() as m:
            m.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"status": "OK", "message": "", "data": {"total": 3, "data": []}})
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"pageNo": 1, "pageSize": 2})
            assert dummy_stream.next_page_token(resp) is None

    def test_parse_response_yields_records(self, dummy_stream):
        with requests_mock.Mocker() as m:
            m.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={"status": "OK", "message": "", "data": {"total": 2, "data": [{"id": 1}, {"id": 2}]}})
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dummy", json={})
            assert list(dummy_stream.parse_response(resp)) == [{"id": 1}, {"id": 2}]


class TestErrorHandler:
    def _response(self, status_code, body):
        resp = requests.Response()
        resp.status_code = status_code
        resp._content = body
        req = requests.PreparedRequest()
        req.prepare(method="POST", url=f"{BASE_URL}/x", data=b"{}")
        resp.request = req
        return resp

    def test_rate_limit_signaled_in_200_body_is_retried(self, dummy_stream):
        resp = self._response(200, b'{"status": "FAIL", "message": "too many requests, slow down", "data": null}')
        resolution = dummy_stream.get_error_handler().interpret_response(resp)
        assert resolution.response_action == ResponseAction.RATE_LIMITED

    def test_generic_error_in_200_body_fails_without_retry(self, dummy_stream):
        resp = self._response(200, b'{"status": "FAIL", "message": "invalid accountId", "data": null}')
        resolution = dummy_stream.get_error_handler().interpret_response(resp)
        assert resolution.response_action == ResponseAction.FAIL

    def test_real_kwai_error_envelope_fails_with_readable_message(self, dummy_stream):
        # Confirmed against a live account: the actual error shape uses result/err_msg,
        # not the status/message the docs implied.
        body = (
            b'{"result": 403, "err_msg": "userId = 12717, The channel developer can not use agentId to query account list.", '
            b'"host": "public-hwsgp-kce-node120.sgpaz3.sgp.kwaidc.com", "port": 21916}'
        )
        resp = self._response(200, body)
        resolution = dummy_stream.get_error_handler().interpret_response(resp)
        assert resolution.response_action == ResponseAction.FAIL
        assert "channel developer" in resolution.error_message
        assert "result=403" in resolution.error_message

    def test_success_envelope_is_success(self, dummy_stream):
        resp = self._response(200, b'{"status": "OK", "message": "", "data": {"total": 0, "data": []}}')
        resolution = dummy_stream.get_error_handler().interpret_response(resp)
        assert resolution.response_action == ResponseAction.SUCCESS

    def test_http_500_falls_back_to_default_handling(self, dummy_stream):
        resp = self._response(500, b"Internal Server Error")
        resolution = dummy_stream.get_error_handler().interpret_response(resp)
        assert resolution.response_action == ResponseAction.RETRY


class TestAdvertisersIdentity:
    def test_construction_without_agent_or_corp_id_is_allowed(self):
        # Valid when account_ids bypasses this stream entirely (see TestAccountPartitioning).
        Advertisers(authenticator=None)

    def test_requesting_without_agent_or_corp_id_fails_at_request_time(self):
        stream = Advertisers(authenticator=None)
        with pytest.raises(ValueError):
            stream.request_body_params(stream_state={})

    def test_agent_id_sends_agentId(self):
        stream = Advertisers(agent_id=76311496, authenticator=None)
        assert stream.request_body_params(stream_state={}) == {"agentId": 76311496}

    def test_corp_id_sends_corpId(self):
        stream = Advertisers(corp_id=999, authenticator=None)
        assert stream.request_body_params(stream_state={}) == {"corpId": 999}

    def test_corp_id_takes_precedence_over_agent_id(self):
        stream = Advertisers(agent_id=76311496, corp_id=999, authenticator=None)
        assert stream.request_body_params(stream_state={}) == {"corpId": 999}


class TestAccountPartitioning:
    def test_child_stream_slices_one_per_account(self, advertisers):
        campaigns = Campaigns(parent=advertisers, start_date="2026-08-01", end_date="2026-08-02", authenticator=None)
        with requests_mock.Mocker() as m:
            m.post(
                f"{BASE_URL}/rest/n/mapi/report/crmAccountQueryByAgentOrCorp",
                json={"status": "OK", "message": "", "data": {"total": 2, "data": [{"accountId": 111}, {"accountId": 222}]}},
            )
            slices = list(campaigns.stream_slices(sync_mode=SyncMode.full_refresh))
        assert [s["account_id"] for s in slices] == [111, 222]
        assert all("dataBeginTime" in s and "dataEndTime" in s for s in slices)

    def test_account_ids_bypasses_the_account_listing_endpoint_entirely(self, advertisers):
        # crmAccountQueryByAgentOrCorp is confirmed rejected outright for some app
        # registrations (see Advertisers docstring). When account_ids is configured,
        # it must never be called -- not mocking it here means the test fails loudly
        # (ConnectionError from requests_mock) if that endpoint gets hit.
        campaigns = Campaigns(parent=advertisers, start_date="2026-08-01", account_ids=[111, 222], authenticator=None)
        with requests_mock.Mocker():
            slices = list(campaigns.stream_slices(sync_mode=SyncMode.full_refresh))
        assert [s["account_id"] for s in slices] == [111, 222]

    def test_account_ids_preserves_configured_order(self, advertisers):
        campaigns = Campaigns(parent=advertisers, start_date="2026-08-01", account_ids=[333, 111, 222], authenticator=None)
        slices = list(campaigns.stream_slices(sync_mode=SyncMode.full_refresh))
        assert [s["account_id"] for s in slices] == [333, 111, 222]

    def test_request_body_includes_timezone(self, advertisers):
        campaigns = Campaigns(parent=advertisers, start_date="2026-08-01", time_zone="UTC-3", authenticator=None)
        body = campaigns.request_body_json(stream_state={}, stream_slice={"account_id": 111, "dataBeginTime": 1, "dataEndTime": 2})
        assert body["timeZoneIana"] == "UTC-3"
        assert body["granularity"] == 3


class TestEntityReportDedup:
    def test_duplicate_entity_ids_across_days_are_deduped(self, advertisers):
        campaigns = Campaigns(parent=advertisers, start_date="2026-08-01", authenticator=None)
        with requests_mock.Mocker() as m:
            m.post(
                f"{BASE_URL}/rest/n/mapi/report/dspCampaignEffectQuery",
                json={
                    "status": "OK",
                    "message": "",
                    "data": {"total": 2, "data": [{"campaignId": 9, "time": "2026-08-01"}, {"campaignId": 9, "time": "2026-08-02"}]},
                },
            )
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dspCampaignEffectQuery", json={})
            records = list(campaigns.parse_response(resp))
        assert len(records) == 1

    @pytest.mark.parametrize("stream_cls,pk", [(Campaigns, "campaignId"), (AdGroups, "unitId"), (Ads, "creativeId")])
    def test_stream_shape(self, advertisers, stream_cls, pk):
        stream = stream_cls(parent=advertisers, start_date="2026-08-01", authenticator=None)
        assert stream.primary_key == pk
        assert stream.get_json_schema()["type"] == "object"


class TestAdsReportsDaily:
    def _stream(self, advertisers, **kwargs):
        defaults = dict(parent=advertisers, start_date="2026-08-01", end_date="2026-08-27", window_in_days=10, lookback_window_days=3, authenticator=None)
        defaults.update(kwargs)
        return AdsReportsDaily(**defaults)

    def test_fresh_account_windows_span_full_range(self, advertisers):
        stream = self._stream(advertisers)
        windows = list(stream._account_windows(111))
        assert len(windows) == 3  # 27 days split into 10-day windows

    def test_resumes_from_lookback_adjusted_state(self, advertisers):
        stream = self._stream(advertisers)
        stream.state = {"111": {"time": "2026-08-20"}}
        begin_ms, _ = next(iter(stream._account_windows(111)))
        resumed_date = datetime.datetime.fromtimestamp(begin_ms / 1000, tz=datetime.timezone.utc).date()
        assert str(resumed_date) == "2026-08-17"

    def test_state_is_isolated_per_account(self, advertisers):
        stream = self._stream(advertisers)
        stream.state = {"111": {"time": "2026-08-25"}}
        window_111 = next(iter(stream._account_windows(111)))
        window_222 = next(iter(stream._account_windows(222)))
        assert window_111 != window_222

    def test_parse_response_adds_cost_decimal_and_updates_state(self, advertisers):
        stream = self._stream(advertisers, page_size=10)
        stream_slice = {"account_id": 111, "dataBeginTime": 1000, "dataEndTime": 2000}
        with requests_mock.Mocker() as m:
            m.post(
                f"{BASE_URL}/rest/n/mapi/report/dspCreativeEffectQuery",
                json={
                    "status": "OK",
                    "message": "",
                    "data": {"total": 1, "data": [{"creativeId": 1, "time": "2026-08-05", "cost": 5000000}]},
                },
            )
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dspCreativeEffectQuery", json={})
            records = list(stream.parse_response(resp, stream_slice=stream_slice))
        assert records[0]["cost_decimal"] == 5.0
        assert stream.state == {"111": {"time": "2026-08-05"}}

    def test_parse_response_handles_real_epoch_ms_time_field(self, advertisers):
        # Confirmed live against the real API: `time` is epoch milliseconds (midnight
        # in the requested timeZoneIana), not a "YYYY-MM-DD" string as the docs implied.
        # This crashed in production ('>' not supported between int and str) before the
        # cursor value was normalized -- lock in the fix.
        stream = self._stream(advertisers, page_size=10, time_zone="UTC-3")
        stream_slice = {"account_id": 111, "dataBeginTime": 1000, "dataEndTime": 2000}
        with requests_mock.Mocker() as m:
            m.post(
                f"{BASE_URL}/rest/n/mapi/report/dspCreativeEffectQuery",
                json={
                    "status": 200,
                    "data": {"total": 1, "data": [{"creativeId": 1, "time": 1785553200000, "cost": 5000000}]},
                },
            )
            resp = requests.post(f"{BASE_URL}/rest/n/mapi/report/dspCreativeEffectQuery", json={})
            records = list(stream.parse_response(resp, stream_slice=stream_slice))
        assert records[0]["time"] == 1785553200000  # raw record value is never mutated
        assert stream.state == {"111": {"time": "2026-08-01"}}

    def test_day_boundaries_align_to_configured_timezone_not_utc(self, advertisers):
        # 00:00 UTC-3 on 2026-08-17 is 03:00 UTC -- if window math used UTC instead of
        # the configured offset, this would be off by the offset amount at the edges.
        stream = self._stream(advertisers, time_zone="UTC-3")
        begin_ms = stream._day_start_ms(datetime.date(2026, 8, 17))
        begin_utc = datetime.datetime.fromtimestamp(begin_ms / 1000, tz=datetime.timezone.utc)
        assert begin_utc == datetime.datetime(2026, 8, 17, 3, 0, tzinfo=datetime.timezone.utc)

    def test_supports_incremental_sync_mode(self, advertisers):
        stream = self._stream(advertisers)
        assert stream.cursor_field == "time"
        assert stream.supports_incremental is True
