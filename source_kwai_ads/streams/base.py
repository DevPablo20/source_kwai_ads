import json
import logging
import re
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.sources.streams.http import HttpStream, HttpSubStream
from airbyte_cdk.sources.streams.http.error_handlers import ErrorResolution, HttpStatusErrorHandler, ResponseAction

logger = logging.getLogger("airbyte")

# Confirmed working against a live account's report endpoints (a working third-party
# integration for the same account uses pageSize=500 for report calls; it uses 100 for
# a separate, unconfirmed set of metadata endpoints this connector doesn't call).
DEFAULT_PAGE_SIZE = 500

_RATE_LIMIT_KEYWORDS = ("too many", "rate limit", "frequency", "exceed", "qps")

_UTC_OFFSET_RE = re.compile(r"^UTC([+-]\d{1,2})(?::?(\d{2}))?$", re.IGNORECASE)


def parse_utc_offset(time_zone: str) -> timezone:
    """
    Parses the connector's "UTC-3"-style `time_zone` config value into a fixed-offset
    tzinfo. Confirmed live: Kwai buckets daily report rows (and stamps their `time`
    field) by midnight in the *requested* timeZoneIana, not UTC -- so date-window
    boundaries need to be computed in that same offset to line up with Kwai's day
    boundaries instead of drifting by the offset amount at the edges of a sync window.
    """
    match = _UTC_OFFSET_RE.match(time_zone.strip())
    if not match:
        return timezone.utc
    hours = int(match.group(1))
    minutes = int(match.group(2) or 0)
    sign = 1 if hours >= 0 else -1
    return timezone(timedelta(hours=hours, minutes=sign * minutes))


class KwaiErrorHandler(HttpStatusErrorHandler):
    """
    The Kwai MAPI reports application-level errors -- including, per the docs, rate
    limiting -- inside a 200 OK body. The default status-code-based handler never
    inspects that body, so genuine throttling would otherwise look like a successful
    empty response. This handler peeks at the body first and defers to the normal
    HTTP-status handling (429, 5xx, ...) otherwise.

    Confirmed against a live error response: the error envelope is
    {result, err_msg, host, port, timestamp, traceId} (e.g. result=403,
    err_msg="..."), not the {status, message} shape the docs implied. A working
    third-party integration for the same account confirms the success envelope is
    {status: 200, data: {data: [...], ...}} -- status as an integer, not the "OK"
    string the docs implied either. So success and error responses use different
    field names entirely (status vs result). Success is still detected
    structurally here -- a `data` object containing a nested `data` list -- rather
    than by hardcoding the literal 200, since that's more robust to any endpoint
    that doesn't follow the pattern exactly.
    """

    def interpret_response(self, response_or_exception: Optional[Any] = None) -> ErrorResolution:
        if isinstance(response_or_exception, requests.Response) and response_or_exception.ok:
            try:
                body = response_or_exception.json()
            except ValueError:
                body = None
            if isinstance(body, dict) and not self._is_success_envelope(body):
                message = str(body.get("err_msg") or body.get("message") or "")
                result_code = body.get("result", body.get("status"))
                if any(keyword in message.lower() for keyword in _RATE_LIMIT_KEYWORDS):
                    return ErrorResolution(
                        response_action=ResponseAction.RATE_LIMITED,
                        failure_type=FailureType.transient_error,
                        error_message=f"Kwai API rate limit signaled in response body: {message}",
                    )
                return ErrorResolution(
                    response_action=ResponseAction.FAIL,
                    failure_type=FailureType.config_error,
                    error_message=f"Kwai API returned an error envelope: result={result_code!r} message={message!r}",
                )
        return super().interpret_response(response_or_exception)

    @staticmethod
    def _is_success_envelope(body: Mapping[str, Any]) -> bool:
        data = body.get("data")
        return isinstance(data, dict) and "data" in data


class KwaiStream(HttpStream, ABC):
    url_base = "https://developers.kwai.com"
    http_method = "POST"
    primary_key: Optional[str] = None

    def __init__(self, *, page_size: int = DEFAULT_PAGE_SIZE, **kwargs: Any):
        super().__init__(**kwargs)
        self.page_size = page_size

    def get_error_handler(self) -> Optional[HttpStatusErrorHandler]:
        return KwaiErrorHandler(logger=logger)

    def request_body_json(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        body: MutableMapping[str, Any] = {
            "pageNo": (next_page_token or {}).get("pageNo", 1),
            "pageSize": self.page_size,
        }
        body.update(self.request_body_params(stream_state, stream_slice, next_page_token))
        return body

    @abstractmethod
    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        """Endpoint-specific body fields (accountId, agentId, dataBeginTime, dataEndTime, granularity, ...)."""

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        body = response.json()
        data = body.get("data") or {}
        items = data.get("data") or []
        total = data.get("total", 0)

        # The page we just fetched is read back from the request body we sent, so pagination
        # doesn't need any separate mutable counters on the stream instance.
        try:
            sent_body = json.loads(response.request.body or b"{}")
        except (ValueError, TypeError):
            sent_body = {}
        current_page = sent_body.get("pageNo", 1)

        if not items or current_page * self.page_size >= total:
            return None
        return {"pageNo": current_page + 1}

    def parse_response(self, response: requests.Response, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        body = response.json()
        data = body.get("data") or {}
        yield from data.get("data") or []


class KwaiReportStream(HttpSubStream, KwaiStream, ABC):
    """
    Base for the /rest/n/mapi/report/* endpoints: partitioned by advertiser account,
    each slice carrying the accountId plus a dataBeginTime/dataEndTime window in
    epoch ms, as required by every report endpoint.

    Confirmed against a live account whose app is registered as a "channel
    developer": `crmAccountQueryByAgentOrCorp` (the `advertisers`/parent stream) is
    unconditionally rejected for that app type, regardless of which agentId/corpId
    is passed -- but a real, working integration for the same account queries
    report endpoints directly with a hardcoded list of account IDs and never calls
    the account-list endpoint at all. So when `account_ids` is configured, this
    class never touches the parent stream -- it synthesizes account slices directly
    from the configured IDs. The parent is only consulted when `account_ids` is
    empty, for users whose app *can* list accounts by agent/corp.
    """

    def __init__(
        self,
        *,
        parent: HttpStream,
        start_date: str,
        end_date: Optional[str] = None,
        time_zone: str = "UTC-3",
        account_ids: Optional[List[int]] = None,
        **kwargs: Any,
    ):
        super().__init__(parent=parent, **kwargs)
        self._start_date = start_date
        self._end_date = end_date
        self._time_zone = time_zone
        self._tzinfo = parse_utc_offset(time_zone)
        self._account_ids = list(account_ids) if account_ids else None

    def _iter_parent_accounts(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:
        if self._account_ids is not None:
            for account_id in self._account_ids:
                yield {"accountId": account_id}
            return

        for parent_slice in HttpSubStream.stream_slices(self, sync_mode=sync_mode, cursor_field=cursor_field, stream_state=stream_state):
            yield parent_slice["parent"]

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        begin_ms, end_ms = self._date_range_ms()
        for account in self._iter_parent_accounts(sync_mode, cursor_field, stream_state):
            yield {
                "account_id": account["accountId"],
                "dataBeginTime": begin_ms,
                "dataEndTime": end_ms,
            }

    def _date_range_ms(self) -> Tuple[int, int]:
        start = datetime.strptime(self._start_date, "%Y-%m-%d").replace(tzinfo=self._tzinfo)
        end = (
            datetime.strptime(self._end_date, "%Y-%m-%d").replace(tzinfo=self._tzinfo)
            if self._end_date
            else datetime.now(self._tzinfo)
        )
        return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        stream_slice = stream_slice or {}
        return {
            "accountId": stream_slice["account_id"],
            "dataBeginTime": stream_slice["dataBeginTime"],
            "dataEndTime": stream_slice["dataEndTime"],
            "timeZoneIana": self._time_zone,
        }
