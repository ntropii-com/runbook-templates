"""Activities for document-ingest.

Each activity is a thin workflow-local wrapper around a shared capability
(parse via files, extract via ai) or tenant-data-plane operation
(commit_extraction). The capability dispatchers do the actual work via
providers — the activities exist mainly to scope step-level retry/timeout
policy and to keep the typing consistent across runbooks.
"""

from __future__ import annotations

from temporalio import activity

from ntro.capabilities import ai, files, storage
from ntro.capabilities.trace import http_reasoning_sink_for_task
from ntro.data import get_data_plane
from ntro.ingest import (
    fetch_submitted_document_by_id,
    insert_extracted_document,
)

from .models import (
    CommitInput,
    DocumentSubmissionPayload,
    ExtractedPayload,
    ExtractInput,
    IngestedDocument,
    RawDocument,
)


@activity.defn(name="document_ingest.parse_pdf")
async def parse_pdf(submitted: DocumentSubmissionPayload) -> RawDocument:
    """Parse the submitted PDF bytes into a structured cell grid + plain text.

    Bytes live in the tenant data plane (api-tenant inserted them on
    upload); the signal carries only the document_ref. We fetch by ref
    here so the signal payload stays small and we don't have to
    base64-shuffle bytes through Pydantic.
    """
    db = await get_data_plane(submitted.tenant_slug)
    doc = await fetch_submitted_document_by_id(db, document_ref=submitted.document_ref)
    if doc is None:
        raise RuntimeError(
            f"document_ref {submitted.document_ref} not found in "
            f"submitted_documents for tenant {submitted.tenant_slug}"
        )
    grid = await files.parse(content=doc.data_bytes, format="pdf")
    return RawDocument(
        document_ref=submitted.document_ref,
        filename=submitted.filename,
        cell_grid=grid.cells,
        plain_text=grid.plain_text,
    )


@activity.defn(name="document_ingest.extract_fields")
async def extract_fields(input: ExtractInput) -> ExtractedPayload:
    """Run AI extraction against the configured schema.

    The schema slug routes to the right prompt template inside the
    capability layer. Provider class (Anthropic vs Ntropii vs ...) is
    chosen by `ai.use_config(input.ai)` from the resolved tenant /
    entity config, falling back to the workspace default.
    """
    # Apply per-tenant AI provider override (set on tenant.config.ai or
    # entity.config.ai; resolved by CommandRouter, propagated through
    # DocumentIngestContext → ExtractInput).
    ai.use_config(input.ai)

    trace_sink = http_reasoning_sink_for_task(
        input.task_id,
        "document_ingest.extract_fields",
        source_label=input.source_label or None,
    )

    result = await ai.extract(
        content=input.raw.plain_text,
        schema_slug=input.schema_slug,
        # Cell-grid context helps the extractor for tabular sources like
        # rent rolls, where row/column structure matters.
        structured_context={"cell_grid": input.raw.cell_grid},
        field_enums=input.field_enums,
        trace=trace_sink,
    )
    return ExtractedPayload(
        schema_slug=input.schema_slug,
        document_ref=input.raw.document_ref,
        fields=result.fields,
        confidence_scores=result.confidence_scores,
        line_items=result.line_items,
        summary=result.summary,
    )


@activity.defn(name="document_ingest.commit_extraction")
async def commit_extraction(input: CommitInput) -> IngestedDocument:
    """Persist the approved extraction (with HITL corrections) to tenant Postgres."""
    db = await get_data_plane(input.tenant_slug)

    # Apply HITL corrections to the extracted payload.
    final_payload = input.extracted.with_corrections(input.corrections)

    record = await insert_extracted_document(
        db,
        entity_slug=input.entity_slug,
        period=input.period,
        source=input.source,
        document_ref=input.extracted.document_ref,
        schema_slug=input.extracted.schema_slug,
        payload=final_payload.model_dump(mode="json"),
        corrections=[c.model_dump(mode="json") for c in input.corrections],
        task_id=str(input.task_id) if input.task_id else None,
    )

    # Audit trail — write the final payload to storage too (cheap, useful for replay).
    audit_path = (
        f"periods/{input.entity_slug}/{input.period}/extractions/"
        f"{input.source}/{record.id}.json"
    )
    await storage.write(
        path=audit_path,
        content=final_payload.model_dump_json(indent=2).encode(),
        content_type="application/json",
    )

    return IngestedDocument(
        source=input.source,
        document_ref=input.extracted.document_ref,
        extracted_payload=final_payload.model_dump(mode="json"),
        confidence=final_payload.average_confidence(),
        corrections_applied=len(input.corrections),
        record_id=record.id,
    )
