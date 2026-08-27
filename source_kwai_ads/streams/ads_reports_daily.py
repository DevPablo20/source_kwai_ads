from datetime import datetime, timedelta
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.core import CheckpointMixin

from .base import KwaiReportStream

_DATE_FMT = "%Y-%m-%d"


class AdsReportsDaily(KwaiReportStream, CheckpointMixin):
    """
    Daily performance fact table per creative (`dspCreativeEffectQuery`,
    granularity=3). This is the one incremental stream in the connector.

    State is tracked per account (`{"<account_id>": {"time": "2026-08-01"}}`)
    rather than globally: different accounts in the same agency sync at different
    paces, and a single shared cursor would make the sync re-fetch -- or worse,
    silently skip -- days for whichever account lags behind the others.

    Each account resumes from `max(start_date, last_synced_date - lookback_window_days)`,
    re-covering a few trailing days on every run since ad platforms commonly revise
    conversion numbers retroactively. The range up to `end_date` (or today) is then
    split into `window_in_days`-sized slices so a single request never spans an
    unbounded date range.
    """

    cursor_field = "time"

    def __init__(self, *, window_in_days: int = 30, lookback_window_days: int = 3, **kwargs: Any):
        super().__init__(**kwargs)
        self._window_in_days = window_in_days
        self._lookback_window_days = lookback_window_days
        self._state: MutableMapping[str, Any] = {}

    @property
    def primary_key(self) -> List[str]:
        return ["accountId", "creativeId", "time"]

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = value or {}

    def path(self, **kwargs: Any) -> str:
        return "/rest/n/mapi/report/dspCreativeEffectQuery"

    def request_body_params(
        self,
        stream_state: Optional[Mapping[str, Any]],
        stream_slice: Optional[Mapping[str, Any]] = None,
        next_page_token: Optional[Mapping[str, Any]] = None,
    ) -> Mapping[str, Any]:
        params: MutableMapping[str, Any] = dict(super().request_body_params(stream_state, stream_slice, next_page_token))
        params["granularity"] = 3
        return params

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for account in self._iter_parent_accounts(sync_mode, cursor_field, stream_state):
            account_id = account["accountId"]
            for begin_ms, end_ms in self._account_windows(account_id):
                yield {"account_id": account_id, "dataBeginTime": begin_ms, "dataEndTime": end_ms}

    def _account_windows(self, account_id: Any) -> Iterable[Tuple[int, int]]:
        global_start = datetime.strptime(self._start_date, _DATE_FMT).date()
        end_date = datetime.strptime(self._end_date, _DATE_FMT).date() if self._end_date else datetime.now(self._tzinfo).date()

        account_cursor = self.state.get(str(account_id), {}).get(self.cursor_field)
        if account_cursor:
            resume_date = datetime.strptime(account_cursor, _DATE_FMT).date() - timedelta(days=self._lookback_window_days)
            window_start = max(global_start, resume_date)
        else:
            window_start = global_start

        cursor = window_start
        while cursor <= end_date:
            window_end = min(cursor + timedelta(days=self._window_in_days - 1), end_date)
            yield self._day_start_ms(cursor), self._day_end_ms(window_end)
            cursor = window_end + timedelta(days=1)

    def _day_start_ms(self, day) -> int:
        return int(datetime.combine(day, datetime.min.time(), tzinfo=self._tzinfo).timestamp() * 1000)

    def _day_end_ms(self, day) -> int:
        return int(datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=self._tzinfo).timestamp() * 1000) - 1

    def _normalize_cursor_value(self, value: Any) -> Optional[str]:
        """
        Kwai's `time` field is epoch milliseconds (e.g. 1785553200000), stamped at
        midnight in the requested `timeZoneIana` -- confirmed live, not the
        "YYYY-MM-DD" string the docs' examples suggested. This derives a comparable
        "YYYY-MM-DD" bucket for internal per-account state tracking without mutating
        the raw record value.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value / 1000, tz=self._tzinfo).strftime(_DATE_FMT)
        return str(value)[:10]

    def parse_response(self, response: Any, *, stream_slice: Optional[Mapping[str, Any]] = None, **kwargs: Any) -> Iterable[Mapping[str, Any]]:
        account_id = str(stream_slice["account_id"]) if stream_slice else None
        for record in super().parse_response(response, stream_slice=stream_slice, **kwargs):
            if record.get("cost") is not None:
                record["cost_decimal"] = record["cost"] / 1_000_000

            record_date = self._normalize_cursor_value(record.get(self.cursor_field))
            if account_id and record_date:
                account_state = self._state.setdefault(account_id, {})
                if record_date > account_state.get(self.cursor_field, ""):
                    account_state[self.cursor_field] = record_date

            yield record
