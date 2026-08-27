from typing import Any, Mapping, Optional

from .base import KwaiStream


class Advertisers(KwaiStream):
    """
    Lists the ad accounts managed by the agency or corp (`crmAccountQueryByAgentOrCorp`).
    This is the parent stream for every report-based stream: account-level
    partitioning elsewhere reads `accountId` off the records yielded here.
    `use_cache=True` so the parent is only fetched once per sync even though every
    child stream re-reads it to build its own slices.

    The endpoint accepts either `agentId` or `corpId`; `corp_id` takes precedence
    when both are configured. Confirmed against a live account whose app is
    registered as a "channel developer": this endpoint is rejected outright
    ("The channel developer can not use agentId to query account list.")
    regardless of which agentId is passed. For that app type, a working
    integration for the same account never calls this endpoint at all -- it
    queries report endpoints directly against a manually configured list of
    account IDs. This stream is only actually used when neither `agent_id` nor
    `corp_id` is needed, i.e. when the connector's `account_ids` config is empty
    (see `KwaiReportStream._iter_parent_accounts`); when `account_ids` is set,
    this class is constructed but never queried, so the identity check below is
    deferred to request time instead of failing at construction.
    """

    primary_key = "accountId"
    use_cache = True

    def __init__(self, *, agent_id: Optional[int] = None, corp_id: Optional[int] = None, **kwargs: Any):
        super().__init__(**kwargs)
        self._agent_id = agent_id
        self._corp_id = corp_id

    def path(self, **kwargs: Any) -> str:
        return "/rest/n/mapi/report/crmAccountQueryByAgentOrCorp"

    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        if self._corp_id is not None:
            return {"corpId": self._corp_id}
        if self._agent_id is not None:
            return {"agentId": self._agent_id}
        raise ValueError(
            "The `advertisers` stream requires `agent_id` or `corp_id` to be configured. "
            "If you only want specific accounts, configure `account_ids` instead and leave "
            "`advertisers` unselected in the sync -- the other streams don't need it in that case."
        )
