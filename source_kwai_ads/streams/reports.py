from abc import ABC
from typing import Any, Iterable, Mapping, MutableMapping, Optional, Set

from .base import KwaiReportStream


class KwaiEntityReportStream(KwaiReportStream, ABC):
    """
    Backs `campaigns`, `ad_groups` and `ads`: entity identity + metrics reconstructed
    from a report endpoint, since no metadata endpoint (budget, bid, status,
    targeting) is reachable under the `ad_mapi_report` scope alone.

    These are full_refresh streams, but the underlying report endpoint is queried
    with `granularity=3` (daily) because it's unconfirmed whether Kwai accepts an
    aggregated view for a whole date range without a granularity (see
    scripts/probe_api.py, incognita #2). Until that's confirmed, the same entity
    shows up once per day it had activity, so records are de-duplicated by entity id
    within a sync. Once confirmed, dropping the forced granularity (if the API
    aggregates by default) would let this dedup step be removed entirely.
    """

    def __init__(self, **kwargs: Any):
        super().__init__(**kwargs)
        self._seen_ids: Set[Any] = set()

    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        params: MutableMapping[str, Any] = dict(super().request_body_params(stream_state, stream_slice, next_page_token))
        params["granularity"] = 3
        return params

    def parse_response(self, response: Any, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        for record in super().parse_response(response, **kwargs):
            entity_id = record.get(self.primary_key)
            if entity_id is not None and entity_id in self._seen_ids:
                continue
            if entity_id is not None:
                self._seen_ids.add(entity_id)
            yield record


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
