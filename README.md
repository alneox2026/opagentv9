# OpenAI Managed Agents Billing Middleware Template

This template exposes an OpenAI **Agents API** saved agent through a Firebase-authenticated Cloud Run gateway, streams its output to the app, and charges reported model-token usage through the existing central billing flow. It uses raw HTTP against the Agents API; it does not use the Agents SDK.

The template is a new sibling of the existing middleware templates. It follows the managed-agent gateway/persistence-worker boundary and retains Terraform-configured Firestore collection names so the same wallet and billing records can be shared.

> **V4 wallet contract:** requests use the $0.05 minimum-balance gate without holds; the worker batches turn-level usage estimates and records any unfunded amount as shared wallet `usage_debt_nanos`. When final provider usage arrives, it applies only the difference from the amount already assessed. The matching v4 centralized billing service must repay shared debt before adding top-up remainder to available balance. This template change does not deploy either service.

## Request flow

1. The gateway authenticates the Firebase user and derives the billing subject, request ID, and idempotent turn ID on the server.
2. For a follow-up, it validates ownership of the existing thread and resolves the provider session from server-owned storage; this step must not create a provider session.
3. The gateway checks that the active wallet's current `available_credit_nanos` is at least the configured minimum balance (default $0.05). If the check fails, it returns the billing error before creating a provider session or submitting input to the Agents API. This is an eligibility check only: the gateway does not calculate token costs, place a monetary hold, or debit the wallet.
4. Only after the balance check succeeds, a new conversation creates its OpenAI-hosted session from the saved Agent ID. The OpenAI session ID is held in the server-owned thread record; clients must not choose it. The gateway then subscribes to the session event stream and submits the message with an `Idempotency-Key`.
5. The gateway streams text to the client and waits for the root turn to complete, fail, or cancel. Session-idle or stream-close alone is not considered success.
6. After the response completes, the gateway publishes raw per-turn usage reports to Pub/Sub. The persistence worker aggregates and prices available turn reports, writes the immutable per-turn ledger and queues a batch entry. A separate worker reconciliation debits grouped turns once per batch; pricing and settlement stay outside the response-stream path. Missing reports remain on an independent provider-usage retry schedule.

## Billing preflight invariant — keep this order

Treat this ordering as a hard billing and side-effect boundary:

- Authenticate and validate existing thread/session ownership using server-owned state only.
- Check that the current available balance is at least the configured floor (default $0.05).
  If the check fails (including HTTP 402), return without creating a provider-side session
  or sending agent/model input.
- Only after the balance check succeeds may the gateway create a provider-side session or submit
  agent/model input. The gateway writes no billing marker; the worker creates the lifecycle record
  atomically with the durable turn ledger after completion.
- Keep buffered and streaming paths aligned; test that zero-credit requests return 402 with no
  provider-session or agent calls.

Purely local ID generation may happen before the balance check only when it has no provider-side side effects.

Use the streaming route:

```http
POST /v1/agents/openai_managed_agent/chat/stream
Authorization: Bearer <Firebase ID token>
Content-Type: application/json

{"message":"Hello","client_turn_id":"a-unique-client-turn-id"}
```

For a follow-up, send the returned `thread_id` and (optionally) the returned `session_id`. The gateway checks thread ownership and resolves the provider session from Firestore. Never accept a client-provided billing subject, wallet amount, provider Agent ID, or usage report as authoritative.

## Agent and model configuration

The provided OpenAI Agent ID is configured in both `config/agents.dev.yaml` and `config/agents.prod.yaml`:

```yaml
remote_agent_id: agent_bc86941978354b21a682cde68398daca57998fc854d74ce3a1
model: gpt-6-luna
```

`openai_managed_agent` is this middleware's local route name, while `MaxBaris` is the saved Agent's dashboard display name. They do not need to match: the middleware addresses the saved Agent by `remote_agent_id`. The Agent's tools, instructions, reasoning, and multi-agent settings remain managed in the OpenAI dashboard and are not duplicated here.

The registry's `model` must match the model on the saved Agent and must have a current explicit rate card in `config/billing.prod.yaml`. The gateway checks the model returned while creating a session and fails closed on a mismatch. Update the model and catalog together if the saved Agent uses a different model. Agent ID is configuration, not a secret.

## Batched customer debits and usage estimates

The customer-facing wallet debit is asynchronous and batched. Before each provider call, the gateway performs a bounded read of the active wallet and checks only that `available_credit_nanos` meets the configured minimum (default $0.05). It does not write a marker, calculate usage, reserve funds, or debit the wallet. After the turn, the worker atomically writes the immutable ledger and a per-turn lifecycle marker; the legacy `billing_reservations_*` collection name is retained for compatibility, but new documents are not holds. Each priced turn is added to the session batch. A batch becomes due on the first of: five minutes of session inactivity, five-minute maximum age, 20 priced turns, or USD 1.00 of provisional estimated usage. These are configurable Terraform values (`billing_batch_session_idle_seconds`, `billing_batch_max_age_seconds`, `billing_batch_max_turns`, and `billing_batch_max_exposure_nanos`). The five-minute worker job processes due batches as well as provider-usage retries.

Keep `billing_reconciliation_enabled = true` whenever `billing_enforcement_enabled = true`. Terraform enables the five-minute worker job by default and rejects a billing-enforced deployment with reconciliation disabled; without that job, batches would remain pending and delayed-usage recovery would stop.

When a priced turn joins a batch, its token-rate estimate is added to `pending_batch_usage_nanos`; this is a provisional estimate, not a hold or confirmed debt. The gateway does not calculate usage costs, subtract pending estimates, write billing state, or debit the wallet: it performs only the configured floor comparison against the wallet's current `available_credit_nanos`. If OpenAI reports some root or subagent turn usage while another turn is missing, the worker queues the known turn-level estimate as provisional usage and keeps retrying the missing report independently. With no usable turn-level usage, it keeps the marker pending without inventing a zero charge. The $0.05 floor is an eligibility threshold, not a spend cap: concurrent requests and usage accumulated before batch close can exceed the visible balance. Unresolved usage by itself does not block a later question.

Batch settlement uses one idempotent wallet transaction per group, updates the customer billing-period aggregate once, and marks each lifecycle record. At close, the worker clears that batch's provisional amount from `pending_batch_usage_nanos`, debits as much estimated usage as the wallet can cover, and adds any unpaid remainder to the shared `usage_debt_nanos` ledger (also recording `lifetime_usage_debt_added_nanos`). The worker retains the per-turn ledger and reservation marker while either settlement or usage recovery is active. The centralized billing service's atomic top-up flow repays known debt first and credits only the remainder; this middleware does not retry per-turn shortfall collection. The per-turn ledgers and wallet transaction retain the audit trail. Batching reduces wallet/transaction writes; it intentionally does not eliminate per-turn transcript, idempotency, or audit writes.

If batching is disabled, direct settlement applies the same available-balance-first, debt-for-the-remainder rule. Turns without a usable estimate wait for provider usage and then use the same debt ledger whether they are placed in a late batch or settled directly.

Debt repayment depends on the shared billing API using the matching v4 debt-aware credit operation: wallet credits must reduce `usage_debt_nanos` first and add only the net remainder to `available_credit_nanos`. Deploy and verify that central billing contract before relying on top-ups to recover middleware usage debt.

The local model charge is still a **versioned token-rate estimate**, not exact provider pass-through. The worker prices reported `input_tokens` (splitting cached input where reported) and `output_tokens` (including reasoning) using the versioned local catalog. It aggregates root-agent and observed subagent turns. The response stream is not held open for usage recovery, token aggregation, or pricing.

The Agents API reports best-effort usage; it can be null or revised after a turn, and does not expose a separate cache-write-token count. After the response, the worker makes up to three bounded, read-only lookup attempts when stream events omitted usage, with each attempt limited to three pages of recent session turns. The middleware therefore:

- Records a per-turn token-rate estimate and `pricing_warnings` in the ledger rather than calling it an exact invoice total.
- Prices unreported cache writes as zero (not as a guessed surcharge), so estimates can understate model cost when a model charges for cache writes.
- Uses the short-context rate card as an explicit estimate because Agents API reports turn totals rather than per-model-call context tiers; a warning is added when aggregate input crosses the catalog threshold.
- Keeps the fast request path provisional. The optional daily OpenAI Costs API job re-fetches recent completed UTC days and stores provider aggregates, line items, local estimate comparisons, allocations, and variances in `openai_provider_costs_*`. The API provides project/day/line-item aggregates, not Firebase customer IDs, so customer charges use the explicit allocation policy below.
- When `openai_cost_allocation_enabled` is true, the worker allocates each project/day line item among customers, compares the share with their already assessed per-turn estimate, and applies only the difference. The integer-nanos largest-remainder method ensures customer shares sum to each provider line item exactly. Wallet updates use idempotent transaction records and the shared usage-debt fields; positive differences use available credit then debt, and downward corrections reduce debt or restore credit.
- Captures unique web-search and hosted-container command invocations, their terminal status counts, and whether the event stream supplied complete item attribution. Container command counts are diagnostic only; container charges use the session allocation policy below. It does not retain provider traces or tool arguments.
- Missing Agents API usage stays pending without delaying the streamed response. When some per-turn token reports are available, the worker batches their cost as a provisional lower-bound estimate. It retries missing reports independently of batch settlement using exponential backoff (defaults: 5 minutes initially, capped at 6 hours) for up to 7 days. When complete provider usage arrives, an open batch is updated in place; a settled turn receives only the idempotent cost difference. Positive differences use available credit first and add any remainder to shared wallet debt. Downward corrections reduce outstanding debt and refund any already-paid portion to the wallet. If no usable estimate exists, the worker waits for provider usage rather than guessing zero. After the retry window, unresolved usage moves to `usage_recovery_manual_review`; the account can still make requests whenever its available balance meets the minimum. Configure the policy with `billing_usage_reconciliation_initial_backoff_seconds`, `billing_usage_reconciliation_max_backoff_seconds`, and `billing_usage_reconciliation_retention_seconds`.
- Provider-usage retries query only due lifecycle markers and run with bounded concurrency (defaults: up to 32 lookups, 8 at once). Work beyond the per-pass limit is deferred without incrementing its retry count. Completed-but-not-enqueued settlement markers also have a next-attempt timestamp, so unchanged pending records are not repeatedly scanned on each worker run. Terraform provisions the Firestore indexes for these due-time queries.

### OpenAI provider-cost allocation policy

Policy version `openai-provider-cost-allocation-v5` is enabled with `openai_cost_allocation_enabled`. The middleware uses a dedicated OpenAI project for the Costs API query, then allocates explicitly Agents API-attributed line items across Firebase billing subjects using these weights:

| Provider line item | Customer allocation weight |
| --- | --- |
| Model input, cached input, and output tokens | Matching model and token-category counts recorded in turn ledgers |
| Aggregate cache-write charges | Matching model's total reported input-token share; Agents API does not expose per-turn cache-write counts |
| Web-search charges | Count of distinct completed web-search tool items; the Agents API provider quantity must match the combined customer count before allocation |
| Hosted-container or sandbox charges | Equal share per unique middleware-created OpenAI-hosted session of the matching size that was created or had a turn during that UTC day |
| Unknown provider line items or unmatched model names | Left unallocated until an explicit, validated allocation rule is added |

This is a disclosed no-markup allocation of the dedicated project's actual daily costs. It is not provider-exact per-user metering: cache writes use a token-share proxy, and hosted-container costs use an equal-per-session policy because the Costs API does not identify Firebase users or expose per-session duration. A session counts once per UTC day if the middleware created it that day or a turn in it was active that day; repeated turns in the same session do not increase its weight. Turns persist the server-owned environment snapshot in the immutable billing ledger, so later changes to a thread's `last_message_at` cannot erase historical activity when the lookback window is reprocessed. Session-creation records also cover sessions with no turns. This policy does not measure container minutes or charge time for a session that has no creation or turn activity in that UTC day. Costs are split only among sessions whose recorded size matches the provider line item (for example, `4gb` maps to `medium`). The API-reported duration quantity and unit remain in the daily cost record and are not represented as per-customer measured minutes. The gateway writes session owner, OpenAI session ID, creation time, environment type, and effective container size into the existing server-owned thread document before submitting the first turn. Provider-reported size is recorded when present; otherwise the explicit size sent in the create-session request is the recorded basis.

Use the configured OpenAI project exclusively for this middleware. The Costs API has no session-to-user field, so dashboard or other-application activity in that project cannot be separated from middleware customers. Every line item without explicit `agents_api` source attribution is left unallocated; it is never spread across middleware customers by model-token or cost fallback. Unknown provider line items and model-specific rows whose model cannot be matched to local usage are also left unallocated until an explicit allocation rule is added. Hosted-container rows are left unallocated if the line item has an unknown size, no matching session weight exists, a ledger session cannot be matched to its owner record, or session attribution is incomplete. Container charges are never shifted to command-call or model-cost weights. If any positive cost remains unallocated, the worker records `manual_review_unallocated_cost` and skips all wallet adjustments for that day so customers are not refunded or charged based on a partial project total. If the provider bucket is zero while the middleware recorded positive estimated usage, the worker defers wallet adjustment with `provider_costs_missing_for_activity` instead of treating delayed provider data as a confirmed zero and issuing a premature refund. Wallet adjustments also wait until a completed daily bucket is at least `openai_cost_allocation_settlement_delay_hours` old (24 hours by default), giving delayed provider reporting time to populate. The lookback still re-fetches and corrects older buckets if the provider later revises them. This grace period reduces early true-ups; it is not a provider finality guarantee. Days with pending turn settlement are deferred. The daily record includes the policy version, matching-size session counts, per-customer weight evidence, fallback counts, unallocated nanos, and wallet adjustment outcome. Keep thread, turn-ledger, and reservation records for the full provider-cost lookback and any manual review.

In persisted `line_items`, `quantity_unit` is the semantic unit used by reporting. Web-search quantities are stored as `calls`, even if the Costs API returns `tokens` for that line item; the original API value is retained in `provider_quantity_unit`. Other line items preserve the provider-reported unit. This is a reporting normalization and does not change cost totals or allocation weights.

### Scale boundary

Cloud Run autoscaling handles concurrent gateway and worker requests, but the daily provider-cost reconciliation is one scheduled worker request that scans the selected day's ledgers and reservations and applies wallet adjustments customer by customer. This batch path has not been load-tested at million-user scale. Before handling that volume, materialize per-day customer weights and their project-wide denominators (for example, in BigQuery), then enqueue idempotent customer shards and load-test Firestore read/write volume and worker deadlines. The daily allocation job is separate from request authorization and must not be used as a live balance check.

Use the configured OpenAI project exclusively for requests that pass through this middleware. Direct dashboard tests or other applications using that project are included in aggregate provider totals, but they have no Firebase owner record and cannot be separated from middleware sessions by the Costs API. They can therefore contaminate policy-based shares; never use the project for manual or unrelated sessions while automated allocation is enabled.

For a new middleware with an empty reservation collection, no document migration is required; Terraform creates the new due-time indexes before enabling the scheduled reconciler. If the new worker will reuse a populated `billing_reservations_*` collection, backfill `billing_settlement_next_attempt_at` to the current UTC time for existing `completed_pending` documents that lack it before enabling reconciliation. Keep the existing ledger and reservation documents; they are needed to recover pending usage and settlement.

The Cloud Scheduler missing-usage/batch-settlement job defaults to every 5 minutes (`*/5 * * * *`) in this OpenAI-managed template; its schedule is configurable with `billing_reconciliation_schedule`. The optional provider-cost job runs daily at `17 2 * * *` UTC and rechecks the configured lookback window. Do not change the daily job to a five-minute schedule: the provider Costs API currently returns daily buckets.

Check the current [Agents API usage documentation](https://developers.openai.com/api/docs/guides/agents-api/observability) and [OpenAI API pricing](https://developers.openai.com/api/docs/pricing) before production pricing changes. The included `gpt-6-luna` catalog matches the existing OpenAI middleware catalog in this workspace; it is not a substitute for reviewing current rates.

## Credentials

The runtime provider credential is an OpenAI API key authorized for Agents API read/write operations, Responses inference, and the saved Agent. Store it in Google Secret Manager; do not put it in YAML, Terraform variables, command-line arguments, source control, or FlutterFlow. Terraform grants the gateway and persistence-worker service accounts access to the runtime secret. The worker uses the key only for bounded, read-only recovery of missing turn usage; it does not submit agent input.

Daily cost import requires a separate OpenAI organization admin API key. It is injected into the worker only; the public gateway never receives it. The example enables daily import and allocation and names `openai-admin-api-key`, so create that Secret Manager secret and add an organization admin key before applying the example Terraform settings. Do not reuse a normal project API key for this endpoint.

The Cloud Shell deploy script reads these OpenAI cost settings from `infra/terraform/terraform.tfvars`, or from matching environment variables: `openai_cost_reconciliation_enabled`, `openai_cost_allocation_enabled`, `openai_admin_api_key_secret_id`, `openai_admin_api_key_secret_version`, `openai_cost_reconciliation_schedule`, `openai_cost_reconciliation_lookback_days`, `openai_cost_allocation_settlement_delay_hours`, `openai_project_id`, and `openai_project_name`. The hosted container size can be set with `openai_hosted_container_size` or overridden with `OPENAI_HOSTED_CONTAINER_SIZE`. Set `openai_project_name = "ceosystem1"` and replace the empty `openai_project_id` with the actual API ID shown for that project (it starts with `proj_`). The display name is not a valid Costs API filter; deployment fails before Terraform initialization until the API ID is supplied. The lookback setting must be an unquoted whole number from 1 through 180 (for example, `openai_cost_reconciliation_lookback_days = 7`); the settlement delay must be an unquoted integer from 0 through 168 and defaults to 24 hours. Invalid values stop deployment before Terraform initialization. When enabled, the script verifies the admin-key secret exists before continuing. The admin key value itself must remain in Secret Manager.

`openai_session_spend_control_limit_cents` optionally sets a best-effort OpenAI session limit in USD cents. It is disabled at zero by default, applies to a provider session (not a Firebase user wallet), and is supplemental protection—not a final usage or billing source.

Every new session uses an OpenAI-hosted sandbox, so the agent can execute code when needed. `openai_hosted_container_size` selects the size sent explicitly with session creation; it defaults to `medium` (2 vCPU, 4 GB memory) and accepts `small` (1 GB) or `large` (16 GB). The selected and provider-reported size are persisted with the session owner so daily shared container charges can be grouped by matching size. Set it in `infra/terraform/terraform.tfvars` or override it with `OPENAI_HOSTED_CONTAINER_SIZE` for the Cloud Shell deploy script.

The Terraform example references an existing secret:

```hcl
openai_api_key_secret_id      = "openai-api-key"
openai_api_key_secret_version = "1"
```

If the secret does not already exist, create it, then add a version without putting the key in shell history or a command argument:

```bash
gcloud secrets create openai-api-key --replication-policy=automatic
read -s -p "OpenAI API key: " OPENAI_API_KEY
printf '%s' "$OPENAI_API_KEY" | gcloud secrets versions add openai-api-key --data-file=-
unset OPENAI_API_KEY
```

If the secret already exists, skip the create command and add a new version. No API key is needed for unit tests or template construction; a live OpenAI smoke test and deployment require the key.

For local live testing, provide the key to the gateway process as the `OPENAI_API_KEY` environment variable. This template does not load secrets from a `.env` file. Do not paste the key into the agent YAML, Terraform variables, or source code.

## Build, deploy, and test

Use the existing Cloud Shell scripts, after reviewing `config/agents.prod.yaml`, `config/billing.prod.yaml`, Terraform project/region values, and the selected shared billing collection names:

```bash
bash scripts/cloudshell_build_middleware.sh
bash scripts/cloudshell_deploy_middleware.sh
```

The Cloud Shell deploy script limits Terraform to three concurrent operations by default to reduce bursts of Firestore and other Google API requests during infrastructure creation. Override this with `TERRAFORM_PARALLELISM=1` through `10` when needed; use a lower value if Cloud Shell encounters transient connection refusals while creating Firestore indexes. A connection refusal is a transport failure, not evidence that the index definition is invalid. After Cloud Shell connectivity recovers, rerun the deployment script: Terraform will refresh its state and plan the remaining changes. Do not remove successfully created indexes or edit Terraform state to recover from this error.

The sample Terraform settings and Cloud Shell deployment script enable prepaid enforcement and batch settlement for testing. Confirm that the test wallet exists, is server-owned, has enough credit, and that shared collection names point to the intended billing service before running a test. The daily OpenAI cost job additionally needs its admin-key secret. These template edits do not modify or deploy any already-running Cloud Run service.

Run the test suite from this directory:

```bash
python -m pytest -q
```

The tests mock upstream HTTP and Firestore; passing them does not validate the supplied Agent ID, API key permissions, saved Agent's actual model, current OpenAI rates, organization Costs API permissions, or production wallet integration. A fresh middleware should first be tested with a dedicated project, a small number of wallets, known tool usage, and reviewed provider-cost allocation records before increasing traffic.

## Important lifecycle note

Archiving/deleting a local thread updates the middleware's thread lifecycle. The current persistence worker does not delete the corresponding OpenAI-hosted session. OpenAI session deletion/retention should be decided and implemented separately before promising remote data deletion to end users.
