import pytest
import requests_mock

from source_kwai_ads.auth import KwaiOauth2Authenticator

TOKEN_URL = "https://developers.kwai.com/oauth/token"


@pytest.fixture
def authenticator():
    return KwaiOauth2Authenticator(TOKEN_URL, "cid", "csecret", "rtoken")


def test_refresh_access_token_is_a_get_request_with_query_params(authenticator):
    with requests_mock.Mocker() as m:
        m.get(TOKEN_URL, json={"data": {"access_token": "abc123", "expires_in": 3599}})
        authenticator.refresh_access_token()

        assert m.last_request.method == "GET"
        assert m.last_request.qs["grant_type"] == ["refresh_token"]
        assert m.last_request.qs["client_id"] == ["cid"]
        assert m.last_request.qs["client_secret"] == ["csecret"]
        assert m.last_request.qs["refresh_token"] == ["rtoken"]


def test_refresh_access_token_extracts_from_nested_envelope(authenticator):
    with requests_mock.Mocker() as m:
        m.get(TOKEN_URL, json={"data": {"access_token": "nested-token", "expires_in": 3599}})
        token, _ = authenticator.refresh_access_token()
        assert token == "nested-token"


def test_refresh_access_token_extracts_from_flat_envelope(authenticator):
    with requests_mock.Mocker() as m:
        m.get(TOKEN_URL, json={"access_token": "flat-token", "expires_in": 3599})
        token, _ = authenticator.refresh_access_token()
        assert token == "flat-token"


def test_get_auth_header_uses_access_token_header(authenticator):
    with requests_mock.Mocker() as m:
        m.get(TOKEN_URL, json={"data": {"access_token": "abc123", "expires_in": 3599}})
        header = authenticator.get_auth_header()
        assert header == {"Access-Token": "abc123"}
