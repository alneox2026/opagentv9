"""Normalize privacy-safe built-in OpenAI tool usage counters."""

from __future__ import annotations

from typing import Any


RESOURCE_STATUS_KEYS = ("completed", "failed", "incomplete", "unknown")


def normalize_provider_resource_usage(value: Any) -> dict[str, Any]:
    """Keep the allowlisted counters and attribution-quality marker only."""
    source = value if isinstance(value, dict) else {}
    web_search_status_counts = _status_counts(source.get("web_search_call_status_counts"))
    container_status_counts = _status_counts(source.get("container_tool_call_status_counts"))
    missing_item_id_count = _nonnegative_int(source.get("missing_item_id_count"))
    unresolved_item_count = _nonnegative_int(source.get("unresolved_item_count"))
    attribution_complete = source.get("attribution_complete") is True
    if (
        missing_item_id_count
        or unresolved_item_count
        or web_search_status_counts["failed"]
        or web_search_status_counts["incomplete"]
        or web_search_status_counts["unknown"]
        or container_status_counts["unknown"]
    ):
        attribution_complete = False
    return {
        "web_search_calls": _nonnegative_int(source.get("web_search_calls")),
        "web_search_attempts": _nonnegative_int(source.get("web_search_attempts")),
        "container_tool_calls": _nonnegative_int(source.get("container_tool_calls")),
        "web_search_call_status_counts": web_search_status_counts,
        "container_tool_call_status_counts": container_status_counts,
        "missing_item_id_count": missing_item_id_count,
        "unresolved_item_count": unresolved_item_count,
        # Older records do not prove that tool events were fully observed.
        "attribution_complete": attribution_complete,
    }


def _status_counts(value: Any) -> dict[str, int]:
    source = value if isinstance(value, dict) else {}
    return {status: _nonnegative_int(source.get(status)) for status in RESOURCE_STATUS_KEYS}


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return 0
    return value
