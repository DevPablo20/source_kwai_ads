import logging
from typing import Any, Mapping, Tuple

import requests
from airbyte_cdk.models import FailureType
from airbyte_cdk.sources.streams.http.exceptions import DefaultBackoffException
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator
from airbyte_cdk.utils import AirbyteTracedException
from airbyte_cdk.utils.datetime_helpers import AirbyteDateTime

logger = logging.getLogger("airbyte")


class KwaiOauth2Authenticator(Oauth2Authenticator):
    """
    Kwai's MAPI token refresh diverges from the CDK's Oauth2Authenticator default in two ways:
      - the refresh call is a GET request with the credentials as query params, not a POST
      - the access token travels in an `Access-Token` header, not `Authorization: Bearer`
    Everything else (nested-response key extraction, token caching/locking, expiry tracking)
    is inherited unchanged from Oauth2Authenticator.
    """

    def __init__(self, token_refresh_endpoint: str, client_id: str, client_secret: str, refresh_token: str):
        super().__init__(
            token_refresh_endpoint=token_refresh_endpoint,
            client_id=client_id,
            client_secret=client_secret,
            refresh_token=refresh_token,
            send_refresh_request_as_query_params=True,
        )

    def get_auth_header(self) -> Mapping[str, Any]:
        return {"Access-Token": self.get_access_token()}

    def refresh_access_token(self) -> Tuple[str, AirbyteDateTime]:
        try:
            response = requests.get(
                self.get_token_refresh_endpoint(),
                params=self.build_refresh_request_query_params(),
                headers=self.build_refresh_request_headers(),
            )
            if not response.ok:
                self._log_response(response)
                response.raise_for_status()
            response_json = response.json()
        except (requests.exceptions.ConnectionError, requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout) as e:
            raise AirbyteTracedException(
                message="OAuth access token refresh request failed due to a network error.",
                internal_message=f"Network error during Kwai OAuth token refresh: {e}",
                failure_type=FailureType.transient_error,
            ) from e
        except requests.exceptions.RequestException as e:
            if e.response is not None and (e.response.status_code == 429 or e.response.status_code >= 500):
                raise DefaultBackoffException(request=e.response.request, response=e.response, failure_type=FailureType.transient_error)
            message = "Refresh token is invalid or expired. Please re-authenticate from Sources/Kwai Ads/Settings."
            raise AirbyteTracedException(internal_message=message, message=message, failure_type=FailureType.config_error) from e

        self._ensure_access_token_in_response(response_json)
        self._log_response(response)
        return self._extract_access_token(response_json), self._extract_token_expiry_date(response_json)
