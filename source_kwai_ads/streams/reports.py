from abc import ABC
from typing import Any, Mapping, MutableMapping, Optional

from .base import KwaiReportStream


class KwaiEntityReportStream(KwaiReportStream, ABC):
    """
    Backs `campaigns`, `ad_groups` and `ads`: entity identity + metrics reconstructed
    from a report endpoint, since no metadata endpoint (budget, bid, status,
    targeting) is reachable under the `ad_mapi_report` scope alone.

    These are full_refresh streams and the underlying report endpoint is queried
    with `granularity=1`, which Kwai confirms live as the consolidated view over
    the requested window: one row per entity, no time breakdown. Measured against
    account 76837727 for June 2026, the consolidated rows reconcile exactly with
    the daily rows they summarise (2 campaigns / 9 ad sets / 9 creatives, and
    R$ 171,979.20 of spend either way).

    This replaces an earlier granularity=3 + in-memory de-duplication approach.
    That combination asked for a daily breakdown and then dropped every row of an
    entity but the first one seen, so the metrics that survived described a single
    arbitrary day rather than the whole requested range -- `cost` on the `ads`
    stream, for instance, reported one day's spend under a column that reads as
    the period total. Asking the API to aggregate removes both the wrong numbers
    and the need to discard records at all.
    """

    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        params: MutableMapping[str, Any] = dict(super().request_body_params(stream_state, stream_slice, next_page_token))
        params["granularity"] = 1
        return params


class Campaigns(KwaiEntityReportStream):
    primary_key = "campaignId"

    def path(self, **kwargs: Any) -> str:
        return "/rest/n/mapi/report/dspCampaignEffectQuery"


class AdGroups(KwaiEntityReportStream):
    primary_key = "unitId"

    def path(self, **kwargs: Any) -> str:
        return "/rest/n/mapi/report/dspUnitEffectQuery"


class Ads(KwaiEntityReportStream):
    primary_key = "creativeId"

    def path(self, **kwargs: Any) -> str:
        return "/rest/n/mapi/report/dspCreativeEffectQuery"
