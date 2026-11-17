from decimal import Decimal

from common.openai_cost_allocation import (
    POLICY_VERSION,
    allocate_integer,
    cost_line_item_weight_kind,
)
from services.agent_persistence_worker_v3.app.services.openai_cost_reconciliation import (
    OpenAICostReconciliationService,
)


def test_largest_remainder_allocation_is_exact_and_stable():
    weights = {"customer-b": 1, "customer-a": 1, "customer-c": 1}

    result = allocate_integer(5, weights)

    assert result == {"customer-a": 2, "customer-b": 2, "customer-c": 1}
    assert sum(result.values()) == 5


def test_policy_classifies_provider_line_items():
    assert POLICY_VERSION == "openai-provider-cost-allocation-v5"
    assert cost_line_item_weight_kind("gpt-6-luna, cache writes") == "cache_write_proxy_tokens"
    assert cost_line_item_weight_kind("gpt-6-luna cached input") == "input_cached_tokens"
    assert cost_line_item_weight_kind("gpt-6-luna output") == "output_tokens"
    assert cost_line_item_weight_kind("web search tool calls") == "web_search_calls"
    assert cost_line_item_weight_kind("container usage, 4gb") == "hosted_container_sessions"
    assert cost_line_item_weight_kind("other provider resource") == "unrecognized_provider_cost"


def test_known_provider_line_items_allocate_and_unknown_items_remain_unallocated():
    users = {
        "customer-a": {
            "web_search_calls": 3,
            "container_session_attribution_complete": True,
            "hosted_container_sessions_by_size": {"medium": {"session-a"}},
            "provider_resource_usage_attribution_complete": True,
            "models": {
                "gpt-6-luna": {
                    "input_uncached_tokens": 100,
                    "input_cached_tokens": 50,
                    "input_tokens": 150,
                    "output_tokens": 10,
                    "estimated_cost_nanos": 1_000,
                }
            },
        },
        "customer-b": {
            "web_search_calls": 1,
            "container_session_attribution_complete": True,
            "hosted_container_sessions_by_size": {
                "medium": {"session-b1", "session-b2", "session-b3"}
            },
            "provider_resource_usage_attribution_complete": True,
            "models": {
                "gpt-6-luna": {
                    "input_uncached_tokens": 300,
                    "input_cached_tokens": 150,
                    "input_tokens": 450,
                    "output_tokens": 30,
                    "estimated_cost_nanos": 3_000,
                }
            },
        },
    }
    rows = [
        {"amount_usd": Decimal("0.000004"), "api_source": "agents_api", "line_item": "gpt-6-luna input tokens"},
        {"amount_usd": Decimal("0.000002"), "api_source": "agents_api", "line_item": "gpt-6-luna cached input"},
        {"amount_usd": Decimal("0.000001"), "api_source": "agents_api", "line_item": "gpt-6-luna cache writes"},
        {"amount_usd": Decimal("0.000003"), "api_source": "agents_api", "line_item": "web search tool calls", "quantity": 4, "quantity_unit": "calls"},
        {
            "amount_usd": Decimal("0.000004"),
            "api_source": "agents_api",
            "line_item": "container usage, 4gb",
            "quantity": Decimal("5"),
            "quantity_unit": "duration_minutes",
            "provider_quantity_unit": "duration_minutes",
        },
        {"amount_usd": Decimal("0.000002"), "api_source": "agents_api", "line_item": "other resource"},
    ]

    shares, fallbacks, unallocated = OpenAICostReconciliationService._allocate_cost_rows(
        rows, users
    )

    assert shares == {"customer-a": 5_000, "customer-b": 9_000}
    assert sum(shares.values()) == 14_000
    assert fallbacks == {"unrecognized_provider_cost_line_item": 1}
    assert unallocated == 2_000


def test_tool_costs_remain_unallocated_when_attribution_is_missing():
    users = {
        "customer-a": {
            "web_search_calls": 0,
            "models": {
                "gpt-6-luna": {"estimated_cost_nanos": 2},
            },
        },
        "customer-b": {
            "web_search_calls": 0,
            "models": {
                "gpt-6-luna": {"estimated_cost_nanos": 6},
            },
        },
    }
    rows = [{
        "amount_usd": Decimal("0.00000008"),
        "api_source": "agents_api",
        "line_item": "web search tool calls",
        "quantity": 1,
        "quantity_unit": "calls",
    }]

    shares, fallbacks, unallocated = OpenAICostReconciliationService._allocate_cost_rows(
        rows, users
    )

    assert shares == {}
    assert fallbacks == {"web_search_quantity_mismatch": 1}
    assert unallocated == 80
