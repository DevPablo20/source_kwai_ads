import logging
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import Oauth2Authenticator

from .auth import KwaiOauth2Authenticator
from .streams.ads_reports_daily import AdsReportsDaily
from .streams.advertisers import Advertisers
from .streams.base import DEFAULT_PAGE_SIZE
from .streams.reports import AdGroups, Ads, Campaigns

TOKEN_REFRESH_ENDPOINT = "https://developers.kwai.com/oauth/token"


class SourceKwaiAds(AbstractSource):
    def _build_authenticator(self, config: Mapping[str, Any]) -> Oauth2Authenticator:
        return KwaiOauth2Authenticator(
            token_refresh_endpoint=TOKEN_REFRESH_ENDPOINT,
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=config["refresh_token"],
        )

    def _build_advertisers(self, config: Mapping[str, Any], **kwargs: Any) -> Advertisers:
        return Advertisers(agent_id=config.get("agent_id"), corp_id=config.get("corp_id"), **kwargs)

    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        # `crmAccountQueryByAgentOrCorp` (the `advertisers`/parent stream) is rejected
        # outright for some app registrations regardless of agentId/corpId -- see
        # Advertisers' docstring. When `account_ids` is configured, report streams never
        # call that endpoint at all (KwaiReportStream._iter_parent_accounts short-circuits
        # to the configured IDs), so probe with a report endpoint directly instead of
        # forcing a call that's known to fail for those accounts.
        try:
            authenticator = self._build_authenticator(config)
            if config.get("account_ids"):
                probe: Stream = Campaigns(
                    parent=self._build_advertisers(config, authenticator=authenticator),
                    start_date=config["start_date"],
                    end_date=config["start_date"],
                    time_zone=config.get("time_zone", "UTC-3"),
                    account_ids=config["account_ids"],
                    authenticator=authenticator,
                    page_size=1,
                )
            else:
                probe = self._build_advertisers(config, authenticator=authenticator, page_size=1)
            stream_slice = next(iter(probe.stream_slices(sync_mode=SyncMode.full_refresh)), None)
            records = probe.read_records(sync_mode=SyncMode.full_refresh, stream_slice=stream_slice)
            next(iter(records), None)
        except Exception as e:
            logger.error(f"Kwai Ads check_connection failed: {e}")
            return False, str(e)
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self._build_authenticator(config)
        common_kwargs: Mapping[str, Any] = {
            "authenticator": authenticator,
            "page_size": config.get("page_size", DEFAULT_PAGE_SIZE),
        }

        advertisers = self._build_advertisers(config, **common_kwargs)

        report_kwargs: Mapping[str, Any] = {
            **common_kwargs,
            "parent": advertisers,
            "start_date": config["start_date"],
            "end_date": config.get("end_date"),
            "time_zone": config.get("time_zone", "UTC-3"),
            "account_ids": config.get("account_ids"),
        }

        return [
            advertisers,
            Campaigns(**report_kwargs),
            AdGroups(**report_kwargs),
            Ads(**report_kwargs),
            AdsReportsDaily(
                **report_kwargs,
                window_in_days=config.get("window_in_days", 30),
                lookback_window_days=config.get("lookback_window_days", 3),
            ),
        ]
