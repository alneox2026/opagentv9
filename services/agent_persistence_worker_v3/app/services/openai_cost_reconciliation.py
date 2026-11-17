"""Import daily OpenAI provider costs and optionally allocate them to wallets."""

from __future__ import annotations

import asyncio
import hashlib
import os
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx

from common.openai_cost_allocation import (
    POLICY_VERSION,
    allocate_integer,
    container_size_for_line_item,
    cost_line_item_weight_kind,
    item_weight,
    line_item_model,
    model_item_weight,
    usd_to_nanos,
)
from services.agent_persistence_worker_v3.app.core.config import get_settings
from services.agent_persistence_worker_v3.app.core.errors import RetryableWorkerError
from services.agent_persistence_worker_v3.app.services.firestore_client import get_firestore_client
from services.agent_persistence_worker_v3.app.services.openai_cost_wallet_adjustment import (
    OpenAICostWalletAdjustmentService,
)

OPENAI_COSTS_URL = "https://api.openai.com/v1/organization/costs"


@dataclass(frozen=True)
class OpenAICostReconciliationResult:
    status: str
    reconciled_days: int = 0
    provider_cost_usd: float = 0.0
    agents_api_cost_usd: float = 0.0
    local_estimate_usd: float = 0.0


class OpenAICostReconciliationService:
    """Fetch and persist daily provider actuals alongside local model estimates."""

    def __init__(
        self,
        *,
        firestore_client_factory: Callable[[], Any] | None = None,
        http_client: httpx.Client | None = None,
        now: Callable[[], datetime] | None = None,
        wallet_adjustment_service: OpenAICostWalletAdjustmentService | None = None,
    ) -> None:
        self._settings = get_settings()
        self._firestore_client_factory = firestore_client_factory or get_firestore_client
        self._http_client = http_client
        self._now = now or (lambda: datetime.now(timezone.utc))
        self._wallet_adjustment_service = wallet_adjustment_service or OpenAICostWalletAdjustmentService(
            firestore_client_factory=self._firestore_client_factory
        )

    async def reconcile(self) -> OpenAICostReconciliationResult:
        return await asyncio.to_thread(self.reconcile_sync)

    def reconcile_sync(self) -> OpenAICostReconciliationResult:
        if not self._settings.openai_cost_reconciliation_enabled:
            return OpenAICostReconciliationResult("disabled")
        if not _is_openai_project_id(self._settings.openai_project_id):
            raise RetryableWorkerError(
                "OpenAI Costs API reconciliation requires OPENAI_PROJECT_ID to be the dedicated proj_... API ID."
            )
        settlement_delay_hours = self._settings.openai_cost_allocation_settlement_delay_hours
        if (
            isinstance(settlement_delay_hours, bool)
            or not isinstance(settlement_delay_hours, int)
            or not 0 <= settlement_delay_hours <= 168
        ):
            raise RetryableWorkerError(
                "OPENAI_COST_ALLOCATION_SETTLEMENT_DELAY_HOURS must be a whole number from 0 through 168."
            )
        api_key = os.getenv("OPENAI_ADMIN_API_KEY", "").strip()
        if not api_key:
            raise RetryableWorkerError(
                "OpenAI daily cost reconciliation is enabled but OPENAI_ADMIN_API_KEY is not configured."
            )

        now = self._now().astimezone(timezone.utc)
        end = datetime.combine(now.date(), datetime_time.min, tzinfo=timezone.utc)
        lookback_days = max(1, min(180, self._settings.openai_cost_reconciliation_lookback_days))
        start = end - timedelta(days=lookback_days)
        client = self._http_client or httpx.Client(timeout=httpx.Timeout(30.0, connect=5.0))
        owns_client = self._http_client is None
        try:
            buckets = self._fetch_buckets(client, api_key, start, end)
        finally:
            if owns_client:
                client.close()
        firestore = self._firestore_client_factory()
        provider_totals = Decimal("0")
        agents_api_totals = Decimal("0")
        local_totals = Decimal("0")
        days_written = 0
        for bucket_start, rows in sorted(buckets.items()):
            day_start = datetime.fromtimestamp(bucket_start, tz=timezone.utc)
            day_end = day_start + timedelta(days=1)
            usage_by_subject, local_nanos, source_status = self._billing_usage_for_day(
                firestore, day_start, day_end
            )
            local_usd = Decimal(local_nanos) / Decimal("1000000000")
            total_usd = sum((row["amount_usd"] for row in rows), Decimal("0"))
            agents_usd = sum(
                (row["amount_usd"] for row in rows if row.get("api_source") == "agents_api"),
                Decimal("0"),
            )
            allocation_source_status = source_status
            if self._settings.openai_cost_allocation_enabled and source_status == "ready":
                if local_nanos > 0 and total_usd == 0:
                    # An empty/zero provider bucket cannot safely prove that
                    # previously assessed usage cost nothing. Costs may arrive
                    # after the first daily import, so defer instead of refunding.
                    allocation_source_status = "provider_costs_missing_for_activity"
            provider_cost_mature = day_end + timedelta(
                hours=self._settings.openai_cost_allocation_settlement_delay_hours
            ) <= now
            provider_totals += total_usd
            agents_api_totals += agents_usd
            local_totals += local_usd
            project_scope = self._settings.openai_project_id
            project_hash = hashlib.sha256(project_scope.encode("utf-8")).hexdigest()[:12]
            document_id = f"{day_start:%Y-%m-%d}_{project_hash}"
            allocations: dict[str, int] = {}
            allocation_notes: dict[str, int] = {}
            allocation_evidence: dict[str, list[dict[str, Any]]] = {}
            container_allocation_summary: dict[str, Any] = {}
            unallocated_nanos = 0
            if (
                self._settings.openai_cost_allocation_enabled
                and allocation_source_status == "ready"
            ):
                (
                    allocations,
                    allocation_notes,
                    unallocated_nanos,
                    allocation_evidence,
                    container_allocation_summary,
                ) = self._allocate_cost_rows_detailed(rows, usage_by_subject)
            adjustment_summary = {
                "status": "disabled",
                "adjusted_customers": 0,
                "charged_nanos": 0,
                "debt_added_nanos": 0,
                "debt_reduced_nanos": 0,
                "refunded_nanos": 0,
            }
            if self._settings.openai_cost_allocation_enabled:
                if allocation_source_status != "ready":
                    adjustment_summary["status"] = allocation_source_status
                elif unallocated_nanos:
                    # Do not settle only the attributable slice: doing so can
                    # refund customers for costs hidden in the unallocated slice.
                    adjustment_summary["status"] = "manual_review_unallocated_cost"
                elif not allocations and total_usd > 0:
                    adjustment_summary["status"] = "manual_review_no_customer_weight"
                    unallocated_nanos = usd_to_nanos(total_usd)
                elif not provider_cost_mature:
                    # Persist the current allocation preview, but do not apply
                    # wallet deltas until the provider bucket has aged through
                    # the configured reporting grace period.
                    adjustment_summary["status"] = "awaiting_provider_cost_maturity"
                else:
                    charged = debt = debt_reduced = refunded = adjusted = 0
                    for subject_id, user_data in usage_by_subject.items():
                        if subject_id not in allocations and not user_data.get("provisional_assessed_cost_nanos"):
                            continue
                        result = self._wallet_adjustment_service.apply_sync(
                            billing_subject_id=subject_id,
                            owner_uid=str(user_data["owner_uid"]),
                            project_id=self._settings.openai_project_id,
                            day_start=day_start,
                            provider_actual_cost_nanos=allocations.get(subject_id, 0),
                            provisional_assessed_cost_nanos=int(
                                user_data.get("provisional_assessed_cost_nanos", 0)
                            ),
                            allocation_evidence=allocation_evidence.get(subject_id, []),
                        )
                        charged += result.charged_nanos
                        debt += result.debt_added_nanos
                        debt_reduced += result.debt_reduced_nanos
                        refunded += result.refunded_nanos
                        adjusted += int(result.status == "adjusted")
                    adjustment_summary.update(
                        {
                            "status": "applied",
                            "adjusted_customers": adjusted,
                            "charged_nanos": charged,
                            "debt_added_nanos": debt,
                            "debt_reduced_nanos": debt_reduced,
                            "refunded_nanos": refunded,
                        }
                    )
            payload = {
                "schema_version": 1,
                "document_id": document_id,
                "provider": "openai",
                "source": "organization_costs_api",
                "bucket_width": "1d",
                "bucket_start": day_start,
                "bucket_end": day_end,
                "project_scope": project_scope,
                "project_name": self._settings.openai_project_name or None,
                "project_filter_applied": True,
                "currency": "USD",
                "provider_actual_cost_usd": float(total_usd),
                "agents_api_actual_cost_usd": float(agents_usd),
                "local_model_estimate_usd": float(local_usd),
                "agents_api_minus_local_model_estimate_usd": float(agents_usd - local_usd),
                "local_estimate_basis": "per_turn_token_rate_card; excludes aggregate cache-write and non-model OpenAI resources",
                "customer_attribution_status": (
                    adjustment_summary["status"]
                    if self._settings.openai_cost_allocation_enabled
                    else "aggregate_only_allocation_disabled"
                ),
                "wallet_adjustment_status": adjustment_summary["status"],
                "allocation_policy_version": POLICY_VERSION if self._settings.openai_cost_allocation_enabled else None,
                "allocation_source_status": allocation_source_status,
                "provider_cost_maturity_status": (
                    "not_applicable"
                    if not self._settings.openai_cost_allocation_enabled
                    else "mature"
                    if provider_cost_mature
                    else "awaiting_provider_cost_maturity"
                ),
                "allocated_customer_count": len(allocations),
                "allocated_cost_nanos": sum(allocations.values()),
                "unallocated_cost_nanos": unallocated_nanos,
                "allocation_fallbacks": allocation_notes,
                "container_allocation": container_allocation_summary,
                "wallet_adjustments": adjustment_summary,
                "line_items": [
                    {
                        "project_id": row.get("project_id"),
                        "api_source": row.get("api_source"),
                        "line_item": row.get("line_item"),
                        "amount_usd": float(row["amount_usd"]),
                        "quantity": row.get("quantity"),
                        "quantity_unit": row.get("quantity_unit"),
                        "provider_quantity_unit": row.get("provider_quantity_unit"),
                    }
                    for row in rows
                ],
                "reconciled_at": now,
                "reconciliation_window_days": lookback_days,
            }
            firestore.collection(self._settings.openai_costs_collection).document(document_id).set(
                payload,
                merge=True,
            )
            days_written += 1
        return OpenAICostReconciliationResult(
            status="completed",
            reconciled_days=days_written,
            provider_cost_usd=float(provider_totals),
            agents_api_cost_usd=float(agents_api_totals),
            local_estimate_usd=float(local_totals),
        )

    def _fetch_buckets(self, client: httpx.Client, api_key: str, start: datetime, end: datetime) -> dict[int, list[dict[str, Any]]]:
        headers = {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}
        params: dict[str, Any] = {
            "start_time": int(start.timestamp()),
            "end_time": int(end.timestamp()),
            "bucket_width": "1d",
            "limit": 180,
            "group_by": ["project_id", "api_source", "line_item"],
        }
        params["project_ids"] = [self._settings.openai_project_id]
        results: dict[int, list[dict[str, Any]]] = defaultdict(list)
        while True:
            try:
                response = client.get(OPENAI_COSTS_URL, headers=headers, params=params)
            except httpx.RequestError as exc:
                raise RetryableWorkerError(f"OpenAI Costs API request failed: {type(exc).__name__}.") from exc
            if response.status_code >= 500 or response.status_code == 429:
                raise RetryableWorkerError(f"OpenAI Costs API returned retryable HTTP {response.status_code}.")
            if response.status_code != 200:
                raise RetryableWorkerError(f"OpenAI Costs API returned HTTP {response.status_code}.")
            try:
                payload = response.json()
            except ValueError as exc:
                raise RetryableWorkerError("OpenAI Costs API returned invalid JSON.") from exc
            if (
                not isinstance(payload, dict)
                or payload.get("object") != "page"
                or not isinstance(payload.get("data"), list)
                or not isinstance(payload.get("has_more"), bool)
            ):
                raise RetryableWorkerError("OpenAI Costs API response is missing its bucket data.")
            for bucket in payload["data"]:
                if not isinstance(bucket, dict) or not isinstance(bucket.get("results"), list):
                    raise RetryableWorkerError("OpenAI Costs API returned a malformed cost bucket.")
                bucket_start = _as_int(bucket.get("start_time"))
                bucket_end = _as_int(bucket.get("end_time"))
                if (
                    bucket_start is None
                    or bucket_end is None
                    or bucket_start < int(start.timestamp())
                    or bucket_start >= int(end.timestamp())
                    or bucket_end <= bucket_start
                    or bucket_end > int(end.timestamp())
                ):
                    raise RetryableWorkerError("OpenAI Costs API returned a bucket with invalid timestamps.")
                results.setdefault(bucket_start, [])
                for row in bucket["results"]:
                    normalized = _normalize_cost_row(row)
                    if normalized["project_id"] != self._settings.openai_project_id:
                        raise RetryableWorkerError(
                            "OpenAI Costs API returned a row outside the configured project filter."
                        )
                    results[bucket_start].append(normalized)
            if not payload["has_more"]:
                break
            page = payload.get("next_page")
            if not page:
                raise RetryableWorkerError("OpenAI Costs API indicated another page but omitted next_page.")
            params["page"] = str(page)
        return dict(results)

    def _billing_usage_for_day(
        self, client: Any, start: datetime, end: datetime
    ) -> tuple[dict[str, dict[str, Any]], int, str]:
        """Read one day's turn usage and assessed reservations for allocation."""
        users: dict[str, dict[str, Any]] = {}
        ledger_turn_count = 0
        unattributed_turn_count = 0
        local_estimate_nanos = 0
        ledger_collection = client.collection(self._settings.billing_ledger_collection)
        for snapshot in _stream_date_range(ledger_collection, "created_at", start, end):
            ledger = snapshot.to_dict() or {}
            created_at = _as_utc_or_none(ledger.get("created_at"))
            if created_at is None or not start <= created_at < end:
                continue
            ledger_turn_count += 1
            subject_id = str(ledger.get("billing_subject_id") or "").strip()
            owner_uid = str(ledger.get("uid") or "").strip()
            estimate = _as_nonnegative_int(ledger.get("estimated_cost_usd_nanos"))
            local_estimate_nanos += estimate
            if not subject_id or not owner_uid:
                unattributed_turn_count += 1
                continue
            customer = users.setdefault(subject_id, _new_usage_customer(owner_uid))
            if customer["owner_uid"] != owner_uid:
                raise RetryableWorkerError(
                    f"Multiple wallet owners map to billing subject {subject_id}."
                )
            customer["ledger_turn_count"] += 1
            session_id = str(ledger.get("session_id") or "").strip()
            if session_id:
                customer["ledger_session_ids"].add(session_id)
                session_metadata_fields = (
                    "openai_environment_type",
                    "openai_container_size",
                    "openai_session_created_at",
                    "openai_session_attribution_version",
                )
                if any(field in ledger for field in session_metadata_fields):
                    session_metadata = {
                        field: ledger.get(field) for field in session_metadata_fields
                    }
                    previous_metadata = customer["ledger_session_attribution"].get(
                        session_id
                    )
                    if previous_metadata is not None and previous_metadata != session_metadata:
                        raise RetryableWorkerError(
                            "Turn ledgers contain conflicting environment attribution for one session."
                        )
                    customer["ledger_session_attribution"][session_id] = session_metadata
            else:
                customer["container_session_attribution_complete"] = False
            usage = ledger.get("usage") if isinstance(ledger.get("usage"), dict) else {}
            token_data = ledger.get("billable_tokens")
            if not isinstance(token_data, dict):
                token_data = usage.get("billable_tokens") if isinstance(usage.get("billable_tokens"), dict) else {}
            cached = _as_nonnegative_int(token_data.get("input_cached"))
            uncached = _as_nonnegative_int(token_data.get("input_uncached"))
            cache_write = _as_nonnegative_int(token_data.get("input_cache_write"))
            output = _as_nonnegative_int(token_data.get("output_including_reasoning"))
            model = str(ledger.get("pricing_model") or usage.get("pricing_model") or "unknown").strip()
            model_data = customer["models"].setdefault(
                model,
                {
                    "input_uncached_tokens": 0,
                    "input_cached_tokens": 0,
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "estimated_cost_nanos": 0,
                },
            )
            model_data["input_uncached_tokens"] += uncached
            model_data["input_cached_tokens"] += cached
            model_data["input_tokens"] += uncached + cached + cache_write
            model_data["output_tokens"] += output
            model_data["estimated_cost_nanos"] += estimate
            resource_usage = usage.get("provider_resource_usage")
            if isinstance(resource_usage, dict):
                customer["web_search_calls"] += _as_nonnegative_int(resource_usage.get("web_search_calls"))
                customer["provider_resource_usage_attribution_complete"] = (
                    customer["provider_resource_usage_attribution_complete"]
                    and resource_usage.get("attribution_complete") is True
                )
            else:
                customer["provider_resource_usage_attribution_complete"] = False

        if not self._settings.openai_cost_allocation_enabled:
            return users, local_estimate_nanos, "allocation_disabled"

        self._collect_hosted_container_sessions(client, start, end, users)

        reservations = client.collection(self._settings.billing_reservations_collection)
        reservation_turn_count = 0
        pending = False
        pending_provider_usage = False
        manual_review_provider_usage = False
        missing_assessment = False
        for snapshot in _stream_date_range(reservations, "created_at", start, end):
            reservation = snapshot.to_dict() or {}
            created_at = _as_utc_or_none(reservation.get("created_at"))
            if created_at is None or not start <= created_at < end:
                continue
            reservation_turn_count += 1
            subject_id = str(reservation.get("billing_subject_id") or "").strip()
            owner_uid = str(reservation.get("owner_uid") or "").strip()
            if not subject_id or not owner_uid or subject_id not in users:
                missing_assessment = True
                continue
            customer = users[subject_id]
            if customer["owner_uid"] != owner_uid:
                raise RetryableWorkerError(
                    f"Reservation owner does not match turn ledger for billing subject {subject_id}."
                )
            customer["reservation_turn_count"] += 1
            state = str(reservation.get("status") or "")
            if state in {"reserved", "completed_pending", "batch_pending", "late_batch_pending", "late_usage_pending"}:
                pending = True
            usage_status = reservation.get("usage_reconciliation_status")
            if usage_status == "pending":
                pending_provider_usage = True
            elif usage_status == "manual_review":
                manual_review_provider_usage = True
            assessed = reservation.get("billing_assessed_cost_nanos")
            if assessed is None and state in {
                "settled", "settled_shortfall", "late_settled", "late_settled_shortfall",
            }:
                assessed = reservation.get("estimated_cost_nanos")
            if assessed is None:
                missing_assessment = True
            else:
                customer["provisional_assessed_cost_nanos"] += _as_nonnegative_int(assessed)

        if ledger_turn_count != reservation_turn_count or unattributed_turn_count:
            return users, local_estimate_nanos, "manual_review_missing_billing_records"
        if any(
            customer["ledger_turn_count"] != customer["reservation_turn_count"]
            for customer in users.values()
        ) or missing_assessment:
            return users, local_estimate_nanos, "manual_review_missing_billing_records"
        if pending:
            return users, local_estimate_nanos, "pending_customer_settlement"
        if pending_provider_usage:
            return users, local_estimate_nanos, "pending_provider_usage"
        if manual_review_provider_usage:
            return users, local_estimate_nanos, "manual_review_provider_usage"
        return users, local_estimate_nanos, "ready"

    def _collect_hosted_container_sessions(
        self,
        client: Any,
        start: datetime,
        end: datetime,
        users: dict[str, dict[str, Any]],
    ) -> None:
        """Build unique per-customer session weights for the UTC cost day.

        A session is in the daily policy denominator if middleware created it
        during the day or a turn in it was active during the day. Provider
        container duration remains preserved separately as invoice evidence.
        """
        session_owners: dict[str, tuple[str, str]] = {}
        for subject_id, customer in users.items():
            for session_id, metadata in customer["ledger_session_attribution"].items():
                normalized = _normalize_session_attribution(session_id, metadata)
                if normalized is None:
                    continue
                environment_type, size = normalized
                current_owner = (subject_id, size)
                previous_owner = session_owners.get(session_id)
                if previous_owner is not None and previous_owner != current_owner:
                    raise RetryableWorkerError(
                        "An OpenAI hosted session is linked to conflicting customers or container sizes."
                    )
                session_owners[session_id] = current_owner
                if environment_type == "openai_hosted":
                    customer["hosted_container_sessions_by_size"][size].add(session_id)

        # Turn ledgers now carry immutable environment metadata, so a later
        # update to last_message_at cannot erase historical session activity.
        # Query mutable activity timestamps only as a compatibility fallback
        # for older ledgers without that snapshot. Session-creation records
        # remain queryable for sessions created without any turns.
        needs_legacy_activity_lookup = any(
            session_id not in session_owners
            for customer in users.values()
            for session_id in customer["ledger_session_ids"]
        )
        collection = client.collection(self._settings.threads_collection)
        candidate_threads: dict[str, dict[str, Any]] = {}
        fields = ["openai_session_created_at"]
        if needs_legacy_activity_lookup:
            fields.append("last_message_at")
        for field in fields:
            for snapshot in _stream_date_range(collection, field, start, end):
                thread = snapshot.to_dict() or {}
                activity_time = _as_utc_or_none(thread.get(field))
                if activity_time is None or not start <= activity_time < end:
                    continue
                thread_id = str(
                    thread.get("thread_id") or getattr(snapshot, "id", "") or ""
                ).strip()
                if not thread_id:
                    thread_id = f"anonymous-{len(candidate_threads)}"
                candidate_threads[thread_id] = thread

        for thread in candidate_threads.values():
            session_id = str(thread.get("session_id") or "").strip()
            owner_uid = str(thread.get("uid") or "").strip()
            subject_id = str(thread.get("billing_subject_id") or owner_uid).strip()
            if not owner_uid or not subject_id:
                sentinel = users.setdefault(
                    "__unattributed_hosted_session__", _new_usage_customer("")
                )
                sentinel["container_session_attribution_complete"] = False
                continue
            customer = users.setdefault(subject_id, _new_usage_customer(owner_uid))
            if customer["owner_uid"] != owner_uid:
                raise RetryableWorkerError(
                    f"Multiple wallet owners map to billing subject {subject_id}."
                )

            normalized = _normalize_session_attribution(session_id, thread)
            if normalized is None:
                customer["container_session_attribution_complete"] = False
                continue
            environment_type, size = normalized
            current_owner = (subject_id, size)
            previous_owner = session_owners.get(session_id)
            if previous_owner is not None and previous_owner != current_owner:
                raise RetryableWorkerError(
                    "An OpenAI hosted session is linked to conflicting customers or container sizes."
                )
            session_owners[session_id] = current_owner
            if environment_type == "openai_hosted":
                customer["hosted_container_sessions_by_size"][size].add(session_id)

        for subject_id, customer in users.items():
            for session_id in customer["ledger_session_ids"]:
                if session_owners.get(session_id, (None, None))[0] != subject_id:
                    customer["container_session_attribution_complete"] = False

    @staticmethod
    def _allocate_cost_rows(
        rows: list[dict[str, Any]], users: dict[str, dict[str, Any]]
    ) -> tuple[dict[str, int], dict[str, int], int]:
        """Compatibility wrapper returning allocation totals and fallback counts."""
        allocations, fallback_counts, unallocated, _, _ = (
            OpenAICostReconciliationService._allocate_cost_rows_detailed(rows, users)
        )
        return allocations, fallback_counts, unallocated

    @staticmethod
    def _allocate_cost_rows_detailed(
        rows: list[dict[str, Any]], users: dict[str, dict[str, Any]]
    ) -> tuple[
        dict[str, int],
        dict[str, int],
        int,
        dict[str, list[dict[str, Any]]],
        dict[str, Any],
    ]:
        allocations: dict[str, int] = defaultdict(int)
        fallback_counts: dict[str, int] = defaultdict(int)
        allocation_evidence: dict[str, list[dict[str, Any]]] = defaultdict(list)
        unallocated_nanos = 0
        container_summary: dict[str, Any] = {
            "basis": "unique_openai_hosted_sessions_by_container_size",
            "sessions_in_denominator_by_size": {},
            "customers_in_denominator_by_size": {},
            "line_items": [],
        }
        resource_usage_complete = all(
            user.get("provider_resource_usage_attribution_complete", True) is True
            for user in users.values()
        )
        local_web_search_calls = sum(
            item_weight(user, "web_search_calls") for user in users.values()
        )
        web_search_rows_by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            if cost_line_item_weight_kind(str(row.get("line_item") or "")) == "web_search_calls":
                web_search_rows_by_source[str(row.get("api_source") or "")].append(row)
        web_search_source_status: dict[str, str | None] = {}
        for source, source_rows in web_search_rows_by_source.items():
            if source != "agents_api":
                web_search_source_status[source] = "web_search_non_agents_api_source"
                continue
            if not resource_usage_complete:
                web_search_source_status[source] = "web_search_usage_attribution_incomplete"
                continue
            quantities = [_provider_call_quantity(row) for row in source_rows]
            if any(quantity is None for quantity in quantities):
                web_search_source_status[source] = "web_search_provider_quantity_invalid"
            elif sum(quantity for quantity in quantities if quantity is not None) != local_web_search_calls:
                web_search_source_status[source] = "web_search_quantity_mismatch"
            else:
                web_search_source_status[source] = None
        model_names = {
            model
            for user in users.values()
            for model in user.get("models", {})
            if model != "unknown"
        }
        for row in rows:
            amount_nanos = usd_to_nanos(row["amount_usd"])
            if amount_nanos == 0:
                continue
            line_item = str(row.get("line_item") or "")
            kind = cost_line_item_weight_kind(line_item)
            model = line_item_model(line_item, model_names)
            source = str(row.get("api_source") or "")
            container_size = (
                container_size_for_line_item(line_item)
                if kind == "hosted_container_sessions"
                else None
            )
            container_line_evidence: dict[str, Any] | None = None
            if kind == "hosted_container_sessions":
                container_line_evidence = {
                    "line_item": line_item,
                    "container_size": container_size,
                    "provider_amount_nanos": amount_nanos,
                    "provider_quantity": row.get("quantity"),
                    "provider_quantity_unit": row.get("provider_quantity_unit"),
                    "allocation_status": "manual_review",
                    "session_weight_total": 0,
                    "customer_count": 0,
                }
            if source != "agents_api":
                # Project costs without explicit Agents API attribution may
                # include activity outside this middleware. Never spread that
                # cost across middleware customers by token-share fallback.
                unallocated_nanos += amount_nanos
                fallback_counts["non_agents_api_cost_source"] += 1
                if container_line_evidence is not None:
                    container_line_evidence["allocation_reason"] = "non_agents_api_cost_source"
                    container_summary["line_items"].append(container_line_evidence)
                continue
            if kind == "unrecognized_provider_cost":
                unallocated_nanos += amount_nanos
                fallback_counts["unrecognized_provider_cost_line_item"] += 1
                continue
            if kind in {
                "input_uncached_tokens",
                "input_cached_tokens",
                "output_tokens",
                "cache_write_proxy_tokens",
            } and model is None:
                # A model-specific provider row without a matching local model
                # must not borrow weights from customers using other models.
                unallocated_nanos += amount_nanos
                fallback_counts["provider_model_unmatched"] += 1
                continue
            if kind == "web_search_calls":
                reason = web_search_source_status.get(source, "web_search_non_agents_api_source")
                if reason is not None:
                    unallocated_nanos += amount_nanos
                    fallback_counts[reason] += 1
                    continue
            elif kind == "hosted_container_sessions":
                if container_size is None:
                    unallocated_nanos += amount_nanos
                    fallback_counts["container_size_unrecognized"] += 1
                    container_line_evidence["allocation_reason"] = "container_size_unrecognized"
                    container_summary["line_items"].append(container_line_evidence)
                    continue
                if not all(
                    user.get("container_session_attribution_complete", True) is True
                    for user in users.values()
                ):
                    unallocated_nanos += amount_nanos
                    fallback_counts["container_session_attribution_incomplete"] += 1
                    container_line_evidence["allocation_reason"] = "container_session_attribution_incomplete"
                    container_summary["line_items"].append(container_line_evidence)
                    continue
            weights: dict[str, int] = {}
            for subject_id, user in users.items():
                weight = (
                    item_weight(user, kind)
                    if kind == "web_search_calls"
                    else item_weight(user, kind, container_size=container_size)
                    if kind == "hosted_container_sessions"
                    else model_item_weight(user, model, kind)
                )
                if weight > 0:
                    weights[subject_id] = weight
            if not weights and kind in {"web_search_calls", "hosted_container_sessions"}:
                unallocated_nanos += amount_nanos
                fallback_counts[f"{kind}_missing_customer_weight"] += 1
                if container_line_evidence is not None:
                    container_line_evidence["allocation_reason"] = "hosted_container_sessions_missing_customer_weight"
                    container_summary["line_items"].append(container_line_evidence)
                continue
            if not weights and kind not in {"model_cost_fallback", "hosted_container_sessions"}:
                fallback_kind = "model_cost_fallback"
                weights = {
                    subject_id: weight
                    for subject_id, user in users.items()
                    if (weight := model_item_weight(user, model, fallback_kind)) > 0
                }
                if weights:
                    fallback_counts[f"{kind}_to_{fallback_kind}"] += 1
            total_weight = sum(weights.values())
            if kind == "hosted_container_sessions" and container_line_evidence is not None:
                container_line_evidence["session_weight_total"] = total_weight
                container_line_evidence["customer_count"] = len(weights)
            if not weights:
                unallocated_nanos += amount_nanos
                fallback_counts["unallocated_no_customer_weight"] += 1
                if container_line_evidence is not None:
                    container_line_evidence["allocation_reason"] = "unallocated_no_customer_weight"
                    container_summary["line_items"].append(container_line_evidence)
                continue
            row_allocations = allocate_integer(amount_nanos, weights)
            for subject_id, amount in row_allocations.items():
                allocations[subject_id] += amount
                allocation_evidence[subject_id].append(
                    {
                        "line_item": line_item,
                        "weight_kind": kind,
                        "allocation_basis": (
                            "unique_openai_hosted_sessions_by_container_size"
                            if kind == "hosted_container_sessions"
                            else "completed_web_search_calls"
                            if kind == "web_search_calls"
                            else "model_token_or_cost_weight"
                        ),
                        "container_size": container_size,
                        "customer_weight": weights[subject_id],
                        "customer_weight_total": total_weight,
                        "provider_amount_nanos": amount_nanos,
                        "allocated_cost_nanos": amount,
                        "provider_quantity": row.get("quantity"),
                        "quantity_unit": row.get("quantity_unit"),
                        "provider_quantity_unit": row.get("provider_quantity_unit"),
                    }
                )
            if container_line_evidence is not None:
                container_line_evidence["allocation_status"] = "allocated"
                container_summary["sessions_in_denominator_by_size"][container_size] = max(
                    container_summary["sessions_in_denominator_by_size"].get(container_size, 0),
                    total_weight,
                )
                container_summary["customers_in_denominator_by_size"][container_size] = max(
                    container_summary["customers_in_denominator_by_size"].get(container_size, 0),
                    len(weights),
                )
                container_summary["line_items"].append(container_line_evidence)
        return (
            dict(allocations),
            dict(fallback_counts),
            unallocated_nanos,
            dict(allocation_evidence),
            container_summary,
        )


def _normalize_cost_row(row: Any) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise RetryableWorkerError("OpenAI Costs API returned a malformed cost row.")
    amount = row.get("amount")
    raw_value = amount.get("value") if isinstance(amount, dict) else None
    currency = str(amount.get("currency") or "usd").lower() if isinstance(amount, dict) else "usd"
    if currency != "usd":
        raise RetryableWorkerError(f"OpenAI Costs API returned unsupported currency {currency!r}.")
    try:
        amount_usd = Decimal(str(raw_value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise RetryableWorkerError("OpenAI Costs API returned a cost row without a valid amount.") from exc
    if not amount_usd.is_finite() or amount_usd < 0:
        raise RetryableWorkerError("OpenAI Costs API returned a non-finite or negative cost amount.")
    project_id = row.get("project_id")
    if not isinstance(project_id, str) or not project_id.strip():
        raise RetryableWorkerError("OpenAI Costs API returned a cost row without a project ID.")
    line_item = row.get("line_item")
    if line_item is not None and not isinstance(line_item, str):
        raise RetryableWorkerError("OpenAI Costs API returned a cost row with an invalid line item.")
    provider_quantity_unit = row.get("quantity_unit")
    if provider_quantity_unit is not None and not isinstance(provider_quantity_unit, str):
        raise RetryableWorkerError("OpenAI Costs API returned a cost row with an invalid quantity unit.")
    # The Costs API can label web-search request quantities as "tokens" even
    # though the line item and quantity represent search calls. Keep the API's
    # value for auditability, but persist the semantic unit for consumers.
    quantity_unit = (
        "calls"
        if cost_line_item_weight_kind(line_item) == "web_search_calls"
        else provider_quantity_unit
    )
    return {
        "amount_usd": amount_usd,
        "project_id": project_id,
        "api_source": row.get("api_source"),
        "line_item": line_item,
        "quantity": row.get("quantity"),
        "quantity_unit": quantity_unit,
        "provider_quantity_unit": provider_quantity_unit,
    }


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stream_date_range(collection: Any, field: str, start: datetime, end: datetime):
    try:
        from google.cloud.firestore_v1.base_query import FieldFilter
    except (ImportError, ModuleNotFoundError):
        # Keep the query bounded even in environments using an older client.
        return collection.where(field, ">=", start).where(field, "<", end).stream()
    try:
        query = collection.where(filter=FieldFilter(field, ">=", start))
        query = query.where(filter=FieldFilter(field, "<", end))
    except TypeError:
        # Some Firestore-compatible clients only support the positional form.
        # Never fall back to collection.stream(): these collections can grow
        # without bound, and a full scan can make daily reconciliation costly.
        query = collection.where(field, ">=", start).where(field, "<", end)
    return query.stream()


def _as_utc_or_none(value: Any) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _as_nonnegative_int(value: Any) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else 0


def _provider_call_quantity(row: dict[str, Any]) -> int | None:
    if row.get("quantity_unit") != "calls":
        return None
    try:
        quantity = Decimal(str(row.get("quantity")))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not quantity.is_finite() or quantity < 0:
        return None
    integral = quantity.to_integral_value()
    return int(integral) if quantity == integral else None


def _is_openai_project_id(value: str) -> bool:
    return bool(value and value.startswith("proj_") and value[5:].isalnum())


def _new_usage_customer(owner_uid: str) -> dict[str, Any]:
    return {
        "owner_uid": owner_uid,
        "models": {},
        "web_search_calls": 0,
        "provider_resource_usage_attribution_complete": True,
        "container_session_attribution_complete": True,
        "hosted_container_sessions_by_size": {
            "small": set(),
            "medium": set(),
            "large": set(),
        },
        "ledger_session_ids": set(),
        "ledger_session_attribution": {},
        "ledger_turn_count": 0,
        "reservation_turn_count": 0,
        "provisional_assessed_cost_nanos": 0,
    }


def _normalize_session_attribution(
    session_id: str, value: Any
) -> tuple[str, str] | None:
    if not isinstance(value, dict) or not session_id:
        return None
    version = value.get("openai_session_attribution_version")
    if type(version) is not int or version != 1:
        return None
    if _as_utc_or_none(value.get("openai_session_created_at")) is None:
        return None
    environment_type = str(value.get("openai_environment_type") or "").strip()
    if environment_type == "none":
        return environment_type, "none"
    if environment_type != "openai_hosted":
        return None
    size = str(value.get("openai_container_size") or "").strip().lower()
    if size not in {"small", "medium", "large"}:
        return None
    return environment_type, size
