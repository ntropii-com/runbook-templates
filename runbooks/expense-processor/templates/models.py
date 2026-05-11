"""Pydantic models for expense-processor.

The runbook accumulates expense rows submitted via ntro_task_data_ingest
(kind="expense"), proposes a category for each via vendor_map.json,
inserts as PENDING into ``ledgers.expenses`` (the platform subledger
declared in ``ntro.subledger.types.expenses``), then surfaces all
PENDING rows for HITL review before flipping status to APPROVED.

Persistence shape (``ExpenseRow``) lives in ``ntro.subledger.types.expenses``
— that's the platform Row class with the standard column block. This
file owns the runbook-side glue (context, signal payload, correction
shape, summary).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from ntro.events import ObservableResult
from ntro.types.accounting import Period
from ntro.types.currencies import CurrencyCode
from ntro.types.dates import ForgivingDate


class ExpenseSubmissionFields(BaseModel):
    """Typed view over the agent-supplied receipt fields.

    Stage-2 boundary type (per the data lifecycle): ``model_validate`` it
    against ``ExpenseSubmissionPayload.data`` in the activity and read
    typed values directly — no dict-poking, no inline parsers, no
    try/except cascades.

    Mix of forgiving and strict types per N-35: ``ForgivingDate``
    degrades to ``None`` on parse failure (HITL fills the gap);
    ``CurrencyCode`` is **strict** (categorical — a typo means "wrong
    currency", not "missing", so coercion failures raise rather than
    silently degrade). The activity catches the ValidationError and
    surfaces the row to HITL for correction.

    ``extra='ignore'`` so agents can include extra context without
    breaking validation.
    """

    model_config = {"extra": "ignore"}

    vendor: str = ""
    amount: Decimal = Decimal("0")
    currency: CurrencyCode = "GBP"
    date: ForgivingDate = None
    payment_method: str | None = None
    line_items: list[dict[str, Any]] | None = None
    vat_amount: Decimal | None = None
    notes: str | None = None


class ExpenseProcessorContext(BaseModel):
    """Run input. Threaded through every step + activity.

    api-workspace's command-router auto-injects ``tenant_id``,
    ``tenant_slug``, ``entity_id``, ``entity_slug`` from the resolved
    tenant + entity at task creation. The runbook uses ``entity_id``
    (UUID) for subledger writes (canonical, immutable) and the slugs
    for human-readable labels in the UI.
    """

    period: Period | None = None
    tenant_id: UUID
    tenant_slug: str
    entity_id: UUID
    entity_slug: str
    # Number of expense rows to accumulate before advancing to the
    # categorise + review steps. The PoC uses a small fixed batch so
    # smoke tests can drive the whole loop in seconds. Production
    # variant evolves to "wait until close_period signal or month-end".
    max_rows: int = 5
    # Hard upper bound for how long the collect step waits for signals.
    collect_timeout_hours: int = 24
    # GL provider config — injected by CommandRouter from
    # ``tenant.config.gl`` (mirror of how ``tenant.config.ai`` flows
    # to ``ctx.ai`` for the AI capability). Empty / missing means the
    # tenant hasn't connected a GL; the post step skips gracefully.
    # Shape:
    #   { "provider": "apideck",
    #     "options": { "consumer_id": "...", "service_id": "xero", ... } }
    gl: dict[str, Any] | None = None


class ExpenseSubmissionPayload(BaseModel):
    """One row arriving via the ``data_submitted`` signal.

    Field names match api-tenant's DataIngestService._signalPayload —
    snake_case, runbook-side schema. ``data`` is the agent-supplied
    receipt fields; the runbook trusts the agent to extract them
    correctly. (LLM observer flags wrong values during review.)
    """

    event_id: str = Field(..., description="UUID assigned by api-tenant on receipt")
    kind: str | None = Field(default=None, description="Should be 'expense' for this runbook")
    tenant_slug: str
    entity_slug: str
    source_ref: dict[str, Any] | None = None
    data: dict[str, Any] = Field(..., description="Receipt fields the agent extracted")
    received_at: str


class DocumentSubmissionPayload(BaseModel):
    """Document reference arriving via ``document_submitted`` signal."""

    document_ref: str
    tenant_slug: str
    entity_slug: str
    source: str
    filename: str
    content_type: str


class CategoriseInput(BaseModel):
    """Input to ``categorise_and_insert_expense`` activity."""

    payload: ExpenseSubmissionPayload
    tenant_slug: str
    entity_id: UUID
    entity_slug: str
    period: str | None = None
    task_id: UUID


class ExpensePersistedEvent(ObservableResult):
    """Activity return — what the workflow surfaces to the UI / observer.

    Carries the persisted-row identity plus the receipt fields the
    reviewer sees in the DATA_TABLE. Distinct from
    ``ntro.subledger.types.expenses.ExpenseRow`` (the persistence
    shape, with full standard column block + Decimal types) because
    Temporal serialises activity returns as JSON — floats survive the
    round-trip cleanly and the reviewer doesn't need entity_id /
    task_id surfaced in every cell.

    All receipt fields are Optional so the activity can return a
    payload for NEEDS_ATTENTION rows where strict-type validation
    failed and the typed columns are NULL. The ``status`` field
    discriminates: ``"PENDING"`` rows have all required fields
    populated; ``"NEEDS_ATTENTION"`` rows may have NULLs and
    ``validation_errors`` set.
    """

    id: str
    event_id: str
    vendor: str | None = None
    amount_gross: float | None = None
    currency: str | None = None
    expense_date: str | None = None
    payment_method: str | None = None
    line_items: list[dict[str, Any]] | None = None
    vat_amount: float | None = None
    category: str | None = None
    category_source: str | None = None
    confidence: float | None = None
    status: str
    notes: str | None = None
    # N-36: populated when the row landed as NEEDS_ATTENTION; carries
    # one entry per validation invariant that failed. Null on PENDING
    # rows where validation passed.
    validation_errors: list[dict[str, Any]] | None = None

    class ObserverMeta:
        event_type = "expense.row.persisted"


class ExpenseCorrection(BaseModel):
    """One HITL correction returned by the review DATA_TABLE."""

    expense_id: str = Field(..., description="row id from ExpensePersistedEvent.id")
    field: str = Field(..., description="Column being corrected")
    new_value: Any


class CommitInput(BaseModel):
    """Input to ``commit_expenses`` activity."""

    tenant_slug: str
    entity_id: UUID
    entity_slug: str
    period: str | None = None
    task_id: UUID
    expense_ids: list[str]
    corrections: list[ExpenseCorrection] = Field(default_factory=list)
    approved: bool = True


class ExpensePeriodResult(ObservableResult):
    """Workflow output — summary of what was committed."""

    period: str
    entity_slug: str
    rows_total: int
    rows_approved: int
    rows_rejected: int
    # Rows still in NEEDS_ATTENTION at commit time — strict-validation
    # failures the reviewer didn't fix during HITL. They stay
    # NEEDS_ATTENTION on the row; the next operator action (next-period
    # workflow run, manual cleanup, or REJECTED transition) handles them.
    rows_needs_attention: int = 0
    total_amount_by_category: dict[str, float] = Field(default_factory=dict)


class PostToGlInput(BaseModel):
    """Input to ``post_expenses_to_gl`` activity."""

    tenant_slug: str
    entity_id: UUID
    entity_slug: str
    task_id: UUID
    expense_ids: list[str]
    # tenant_config block that ApideckGLProvider reads — only
    # ``gl.options.{consumer_id, service_id}`` flows here. Workspace
    # secrets (api_key, app_id) come from worker pod env, NOT this dict.
    tenant_config: dict[str, Any]


class PostedBillSummary(BaseModel):
    """One row in the post-step's summary table — surfaces the per-bill
    detail an operator needs to reconcile a successful post against the
    GL provider's UI (Xero / QBO / etc).
    """

    row_id: str
    vendor: str | None = None
    amount_gross: float | None = None
    currency: str | None = None
    expense_date: str | None = None
    # Provider-assigned id (Xero bill id, QBO ref, ...). Operator can
    # paste this into the upstream GL's search to find the bill.
    posted_journal_ref: str
    # Idempotency key we threaded through ``external_id`` — also lands
    # on the upstream record's reference field, so operators can search
    # both ways.
    idempotency_key: str


class PostToGlResult(ObservableResult):
    """Result of the GL post step.

    `posted` rows transitioned APPROVED → POSTED with their
    ``posted_journal_ref`` populated. `failed` rows hit a
    non-retryable provider error (validation, idempotency conflict);
    those stay APPROVED on the subledger so an operator can fix them
    on the next run. `skipped` is set when the tenant has no GL
    configured — the runbook degrades gracefully.

    The ``posted_bills`` list carries enough per-bill context to render
    a useful summary on the final screen (one row per Bill landed in
    Xero, with vendor + amount + Xero id). The shorthand counts
    ``posted`` / ``failed`` / ``skipped`` are kept for the observer
    event payload + quick-glance dashboards.
    """

    posted: int
    failed: int
    skipped: int = 0
    # Per-row error context for HITL diagnosis. Each entry:
    # ``{row_id, idempotency_key, error_class, message}``.
    errors: list[dict[str, Any]] = Field(default_factory=list)
    # Map of row_id → provider-assigned id. Kept for backward compat;
    # ``posted_bills`` carries the same data + per-row context.
    posted_journal_refs: dict[str, str] = Field(default_factory=dict)
    # Rich per-bill summary for the final screen. Empty when nothing
    # was posted (skipped or all failed).
    posted_bills: list[PostedBillSummary] = Field(default_factory=list)
    # GL provider that received the posts (``"apideck:xero"``,
    # ``"apideck:quickbooks-online"``, ...). Lets the UI label the
    # summary screen with the right destination.
    gl_provider: str | None = None
    # Subledger-backed rows that were submitted in this run, surfaced on
    # the completed step so operators can reconcile what entered the run
    # versus what reached the GL.
    submitted_expenses: list[dict[str, Any]] = Field(default_factory=list)

    class ObserverMeta:
        event_type = "expense.gl.post.complete"
    corrections_applied: int = 0

    class ObserverMeta:
        event_type = "expense.period.committed"
