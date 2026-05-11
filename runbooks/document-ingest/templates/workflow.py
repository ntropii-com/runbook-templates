"""document-ingest child workflow — signal-driven document receipt + extraction + HITL.

One instance per expected document. Blocks on a `document_submitted` signal
(advertising what file is needed via current_pending_action so agent drivers
can submit via ntro_task_file_submit), then runs parse → extract → review
→ commit.

Each phase is a ``@ui_step``-decorated method on this class — that's
what feeds the UI breadcrumb. Class-definition order is the order the
user sees in the sidebar.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from temporalio import workflow

from ntro.workflow import NtroWorkflow, ui_step

from .activities import (
    commit_extraction,
    extract_fields,
    parse_pdf,
)
from .models import (
    CommitInput,
    DocumentIngestContext,
    DocumentSubmissionPayload,
    ExtractInput,
    IngestedDocument,
)


@workflow.defn
class DocumentIngestWorkflow(NtroWorkflow):
    def __init__(self) -> None:
        super().__init__()
        self._submitted: DocumentSubmissionPayload | None = None

    @workflow.signal
    async def document_submitted(self, payload: dict[str, Any]) -> None:
        """Signal handler — receives the document reference from
        api-tenant after the file has been persisted to the tenant
        data plane. The bytes themselves stay in Postgres; parse_pdf
        fetches them by document_ref + tenant_slug."""
        if isinstance(payload, dict):
            self._submitted = DocumentSubmissionPayload(
                document_ref=payload.get("document_ref", ""),
                tenant_slug=payload.get("tenant_slug", ""),
                entity_slug=payload.get("entity_slug", ""),
                source=payload.get("source", ""),
                filename=payload.get("filename", ""),
                content_type=payload.get("content_type", "application/octet-stream"),
            )
        else:
            self._submitted = payload

    # ── Steps (declaration order = breadcrumb order) ───────────────

    @ui_step(name="await_submission", title="Await submission", icon="Inbox")
    async def _step_await_submission(self, ctx: DocumentIngestContext) -> None:
        await self.await_signal_with_action(
            predicate=lambda: self._submitted is not None,
            action={
                "kind": "submit_file",
                "reason": (
                    f"Workflow needs the {ctx.source} document for entity "
                    f"{ctx.entity_slug} (period {ctx.period})."
                ),
                "args": {
                    "tenant_id_or_slug": ctx.tenant_slug,
                    "entity_id_or_slug": ctx.entity_slug,
                    "source": ctx.source,
                    "schema_slug": ctx.schema_slug,
                    "signal_task_id": workflow.info().workflow_id,
                    "signal_name": "document_submitted",
                },
            },
            display_hint={
                "component": "FILE_UPLOAD",
                "config": {
                    "title": f"Upload {ctx.source}",
                    "instructions": (
                        f"Upload the {ctx.source} document for "
                        f"{ctx.entity_slug} ({ctx.period})."
                    ),
                    "accept": [
                        "application/pdf",
                        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        "application/vnd.ms-excel",
                    ],
                    "max_size_mb": 50,
                    "multiple": False,
                },
            },
            timeout=timedelta(hours=24),
        )

    @ui_step(name="parse_pdf", title="Parse document", icon="FileSearch")
    async def _step_parse_pdf(self, submitted: DocumentSubmissionPayload) -> Any:
        return await workflow.execute_activity(
            parse_pdf,
            submitted,
            start_to_close_timeout=timedelta(minutes=5),
        )

    @ui_step(name="extract_fields", title="Extract fields", icon="Sparkles")
    async def _step_extract_fields(
        self, ctx: DocumentIngestContext, raw: Any
    ) -> Any:
        return await workflow.execute_activity(
            extract_fields,
            ExtractInput(
                raw=raw,
                schema_slug=ctx.schema_slug,
                tenant_slug=ctx.tenant_slug,
                entity_slug=ctx.entity_slug,
                source_label=ctx.source,
                task_id=workflow.info().workflow_id.split(":")[0],
                ai=ctx.ai,
                field_enums=ctx.field_enums,
            ),
            start_to_close_timeout=timedelta(minutes=10),
        )

    @ui_step(name="review_extraction", title="Review extraction", icon="Eye")
    async def _step_review_extraction(
        self, ctx: DocumentIngestContext, extracted: Any
    ) -> dict[str, Any]:
        # Surface the AI-generated summary on the review screen — gives
        # the reviewer a one-liner at the top before they scan the table.
        summary_text = getattr(extracted, "summary", "") or ""
        return await self.wait_for_action(
            payload=extracted.model_dump(mode="json"),
            display_hint={
                "component": "DATA_TABLE",
                "config": {
                    "title": f"Review extracted {ctx.source}",
                    "subtitle": (
                        f"Confirm or correct the extracted line items for "
                        f"{ctx.entity_slug} ({ctx.period})."
                    ),
                    "summary": summary_text,
                    "datasets": [
                        {
                            "key": "line_items",
                            "label": "Line items",
                            "data_key": "line_items",
                        }
                    ],
                    "approve_label": "Approve & continue",
                    "reject_label": "Reject",
                    "allow_corrections": True,
                    "confidence_warning_threshold": 0.7,
                    # Canvas-header CTAs (right-aligned in TCH).
                    # Order = left-to-right rendering — secondary
                    # first so the primary action sits on the right.
                    "actions": [
                        {"kind": "reject", "label": "Revise"},
                        {
                            "kind": "approve",
                            "label": "Confirm extraction",
                            "primary": True,
                            "icon": "ArrowRight",
                        },
                    ],
                },
            },
            reason=f"Extracted {ctx.source} for {ctx.entity_slug} — review and approve.",
        )

    @ui_step(name="commit", title="Commit", icon="CheckCircle2")
    async def _step_commit(
        self,
        ctx: DocumentIngestContext,
        extracted: Any,
        approval: dict[str, Any],
    ) -> IngestedDocument:
        # Root task id = parent's task UUID (this child's workflow_id is
        # `parent:step:slug:source` — split off the suffix). Lets the
        # commit row carry the workflow run that produced it so
        # cross-task lookups (find_committed_document) can scope reads.
        root_task_id = workflow.info().workflow_id.split(":")[0]
        return await workflow.execute_activity(
            commit_extraction,
            CommitInput(
                source=ctx.source,
                period=ctx.period,
                entity_slug=ctx.entity_slug,
                tenant_slug=ctx.tenant_slug,
                extracted=extracted,
                corrections=approval.get("corrections", []) if isinstance(approval, dict) else [],
                task_id=root_task_id,
            ),
            start_to_close_timeout=timedelta(minutes=5),
        )

    # ── Entry point ────────────────────────────────────────────────

    @workflow.run
    async def run(self, ctx: DocumentIngestContext) -> IngestedDocument:
        await self._step_await_submission(ctx)
        assert self._submitted is not None
        submitted = self._submitted

        raw = await self._step_parse_pdf(submitted)
        extracted = await self._step_extract_fields(ctx, raw)
        approval = await self._step_review_extraction(ctx, extracted)
        return await self._step_commit(ctx, extracted, approval)
