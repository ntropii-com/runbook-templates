"""Pydantic models for document-ingest.

Activity-return models inherit from ObservableResult so the worker's
interceptor (post-PoC) automatically emits structured business events.
In PoC, ObservableResult is a no-op marker — typed returns flow through
Temporal's event history.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from ntro.events import ObservableResult


class DocumentIngestContext(BaseModel):
    """Run input — built by the parent (NavMonthlyWorkflow) per expected document."""

    period: str = Field(..., pattern=r"^\d{4}-\d{2}$")
    entity_slug: str
    tenant_slug: str
    source: str = Field(..., description="Source identifier matched by ntro_document_submit")
    schema_slug: str = Field(..., description="Extraction schema identifier (e.g. 'rent-roll-zenko')")
    # Resolved AI provider config injected by CommandRouter; consumed by
    # extract_fields to switch the LLM provider. See spec page "Spec:
    # Private LLM inference for tasks".
    ai: dict | None = None
    # Optional per-field enum constraint map (N-79). Each key is a
    # dotted field path (e.g. ``"line_items.category"``); values are
    # ``[{slug, hint}, ...]``. Forwarded straight through to
    # ``ai.extract``'s ``field_enums`` arg so the runbook owns the
    # taxonomy without touching the shared library.
    field_enums: dict[str, list[dict[str, str]]] | None = None


class DocumentSubmissionPayload(BaseModel):
    """Carried in the document_submitted signal.

    Note: bytes are NOT in the signal — they live in the tenant data
    plane keyed by document_ref. parse_pdf fetches them via
    get_data_plane(tenant_slug). Keeps signals lean and dodges the
    pain of base64-in-Pydantic-bytes-fields.
    """

    document_ref: str = Field(..., description="Stable ID assigned by api-tenant on receipt")
    tenant_slug: str = Field(..., description="Routes parse_pdf to the right tenant data plane")
    entity_slug: str
    source: str
    filename: str
    content_type: str


class RawDocument(ObservableResult):
    """Output of parse_pdf — structured representation for downstream extraction."""

    document_ref: str
    filename: str
    cell_grid: list[list[str]]
    plain_text: str

    class ObserverMeta:
        event_type = "document.parsed"


class ExtractInput(BaseModel):
    raw: RawDocument
    schema_slug: str
    tenant_slug: str
    entity_slug: str
    source_label: str = ""
    # Root workspace task id (child workflow id prefix) — drives reasoning
    # stream fan-out to ui-tenant during extract_fields.
    task_id: str = ""
    # Resolved AI provider config forwarded from DocumentIngestContext.
    # Empty / unset = workspace default.
    ai: dict | None = None
    # N-79: runbook-supplied per-field enum constraints. Forwarded to
    # ``ai.extract``'s ``field_enums`` so categorisation taxonomies live
    # in the runbook, not the library.
    field_enums: dict[str, list[dict[str, str]]] | None = None


class LineItemCorrection(BaseModel):
    """One HITL correction submitted via the review DATA_TABLE."""

    line_item_id: str
    field: str
    old_value: Any
    new_value: Any


class ExtractedPayload(ObservableResult):
    """Output of extract_fields — typed AI extraction result."""

    schema_slug: str
    document_ref: str
    fields: dict[str, Any] = Field(default_factory=dict, description="Top-level fields (e.g. period, totals)")
    line_items: list[dict[str, Any]] = Field(default_factory=list, description="Repeating rows (e.g. one per unit)")
    confidence_scores: dict[str, float] = Field(default_factory=dict)
    summary: str = Field(default="", description="One-line AI summary of the document for the review screen")

    class ObserverMeta:
        event_type = "extraction.completed"

    def with_corrections(self, corrections: list[LineItemCorrection]) -> "ExtractedPayload":
        """Apply HITL corrections to line_items (immutable — returns a new instance)."""
        if not corrections:
            return self
        line_items = [dict(item) for item in self.line_items]
        for c in corrections:
            for item in line_items:
                if item.get("id") == c.line_item_id:
                    item[c.field] = c.new_value
        return self.model_copy(update={"line_items": line_items})

    def average_confidence(self) -> float:
        if not self.confidence_scores:
            return 0.0
        return sum(self.confidence_scores.values()) / len(self.confidence_scores)


class CommitInput(BaseModel):
    source: str
    period: str
    entity_slug: str
    tenant_slug: str
    extracted: ExtractedPayload
    corrections: list[LineItemCorrection] = Field(default_factory=list)
    # Root task id (parent UUID, child workflow id suffixes stripped) so
    # the commit row carries the workflow run that produced it. Drives
    # cross-task lookups via ntro.workflow.history.find_committed_document.
    task_id: str = ""


class IngestedDocument(ObservableResult):
    """Returned to the parent workflow once HITL-approved + persisted."""

    source: str
    document_ref: str
    extracted_payload: dict[str, Any]
    confidence: float
    corrections_applied: int
    record_id: str

    class ObserverMeta:
        event_type = "document.ingested"
