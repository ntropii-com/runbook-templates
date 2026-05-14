"""expense-processor workflow.

Always-on inbox workflow for one entity. Accumulates expense rows submitted
via `ntro_task_data_ingest` (kind="expense"), categorises and persists
each as PENDING in the entity's `expenses` table, surfaces the batch for
HITL review, and flips statuses to APPROVED on user approval.

Rows are period-stamped during ingestion (receipt date -> YYYY-MM,
fallback current UTC month). Optional context.period can force a
single batch period for backfills.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from ntro.workflow import runbook

from ntro.ingest import IngestOutcome
from ntro.workflow import NtroWorkflow, ui_step

from .activities import (
    categorise_and_insert_expense,
    commit_expenses,
    ingest_submitted_document,
    post_expenses_to_gl,
)
from .models import (
    CategoriseInput,
    CommitInput,
    ExpenseCorrection,
    ExpensePeriodResult,
    ExpensePersistedEvent,
    ExpenseProcessorContext,
    ExpenseSubmissionPayload,
    DocumentSubmissionPayload,
    PostToGlInput,
    PostToGlResult,
)


@runbook.defn
class ExpenseProcessorWorkflow(NtroWorkflow):
    def __init__(self) -> None:
        super().__init__()
        self._submitted_events: list[ExpenseSubmissionPayload] = []
        self._submitted_documents: list[DocumentSubmissionPayload] = []
        self._row_corrections: dict[tuple[str, str], Any] = {}
        self._rejected_row_ids: set[str] = set()

    # ── Signal handlers ────────────────────────────────────────────

    @runbook.signal
    async def data_submitted(self, payload: dict[str, Any]) -> None:
        """One expense row arriving via api-tenant after persistence
        to submitted_records. Just append; the main loop processes
        once at least one accepted row is available."""
        if not isinstance(payload, dict):
            return
        self._submitted_events.append(
            ExpenseSubmissionPayload(
                event_id=payload.get("event_id", ""),
                kind=payload.get("kind"),
                tenant_slug=payload.get("tenant_slug", ""),
                entity_slug=payload.get("entity_slug", ""),
                source_ref=payload.get("source_ref"),
                data=payload.get("data", {}),
                received_at=payload.get("received_at", ""),
            )
        )

    @runbook.signal
    async def document_submitted(self, payload: dict[str, Any]) -> None:
        """Document reference from api-tenant file upload path."""
        if not isinstance(payload, dict):
            return
        self._submitted_documents.append(
            DocumentSubmissionPayload(
                document_ref=payload.get("document_ref", ""),
                tenant_slug=payload.get("tenant_slug", ""),
                entity_slug=payload.get("entity_slug", ""),
                source=payload.get("source", ""),
                filename=payload.get("filename", ""),
                content_type=payload.get("content_type", ""),
            )
        )

    def on_row_action(self, action: dict[str, Any]) -> None:
        payload = action.get("payload") if isinstance(action, dict) else None
        if not isinstance(payload, dict):
            return
        op = payload.get("action")
        row_id = payload.get("row_id")
        if not isinstance(row_id, str) or not row_id:
            return
        if op == "reject_row":
            self._rejected_row_ids.add(row_id)
            if isinstance(self._pending_payload, dict):
                rows = self._pending_payload.get("line_items")
                if isinstance(rows, list):
                    for row in rows:
                        if isinstance(row, dict) and row.get("id") == row_id:
                            row["status"] = "REJECTED"
            return
        if op == "edit_cell":
            field = payload.get("field")
            if not isinstance(field, str) or not field:
                return
            self._row_corrections[(row_id, field)] = payload.get("value")
            if isinstance(self._pending_payload, dict):
                rows = self._pending_payload.get("line_items")
                if isinstance(rows, list):
                    for row in rows:
                        if isinstance(row, dict) and row.get("id") == row_id:
                            row[field] = payload.get("value")
                            if row.get("status") == "NEEDS_ATTENTION":
                                row["status"] = "PENDING"
            return

    # ── Steps (declaration order = breadcrumb order) ──────────────

    def _display_period(self, ctx: ExpenseProcessorContext) -> str:
        return str(ctx.period) if ctx.period else "derived from receipt date"

    @ui_step(name="collect", title="Collect expense rows", icon="Inbox")
    async def _step_collect(self, ctx: ExpenseProcessorContext) -> dict[str, Any]:
        period_label = self._display_period(ctx)
        files_processed: list[dict[str, Any]] = []
        target_rows = 1
        while len(self._submitted_events) < target_rows:
            progress = len(self._submitted_events)
            if files_processed:
                latest = files_processed[-1]
                latest_name = str(latest.get("filename") or latest.get("document_ref") or "uploaded file")
                latest_rows = int(latest.get("accepted_rows", 0))
                if latest_rows == 0:
                    message = (
                        f"Last upload ({latest_name}) was processed but accepted 0 rows. "
                        "Check required columns (vendor, amount, currency, date) and try again."
                    )
                else:
                    message = f"Last upload ({latest_name}) accepted {latest_rows} row(s)."
                outcome = IngestOutcome(
                    source_ref=latest_name,
                    parsed_rows=latest_rows,
                    accepted_rows=latest_rows,
                    rejected_rows=0,
                    status="empty" if latest_rows == 0 else "success",
                    message=message,
                    progress={
                        "accepted_rows": progress,
                        "target_rows": target_rows,
                    },
                )
            else:
                outcome = IngestOutcome(
                    status="success",
                    message=f"Upload a CSV/XLSX file. Current progress: {progress}/{target_rows} accepted rows.",
                    progress={
                        "accepted_rows": progress,
                        "target_rows": target_rows,
                    },
                )
            self.set_user_feedback(kind="ingest", payload=outcome)

            await self.await_signal_with_action(
                predicate=lambda: (
                    len(self._submitted_events) >= target_rows
                    or len(self._submitted_documents) > 0
                ),
                action={
                    "kind": "submit_file",
                    "reason": (
                        f"Collecting expense rows for {ctx.entity_slug} ({period_label}). "
                        f"Upload a CSV/XLSX file (or submit rows via "
                        f'ntro_task_data_ingest kind="expense"). Advances after '
                        "at least 1 accepted row."
                    ),
                    "args": {
                        "source": "expense-csv",
                        "tenant_id_or_slug": ctx.tenant_slug,
                        "entity_id_or_slug": ctx.entity_slug,
                        "expected_kind": "expense",  # MCP path remains supported.
                        "signal_task_id": runbook.info().workflow_id,
                        "signal_name": "document_submitted",
                    },
                },
                display_hint={
                    "component": "FILE_UPLOAD",
                    "config": {
                        "title": f"Upload expense rows for {period_label}",
                        "instructions": (
                            f"Upload a CSV or Excel file of expenses. The platform stores canonical ingest events "
                            "and advances after at least 1 accepted row. "
                            f'Agents can still submit rows via ntro_task_data_ingest kind="expense".'
                        ),
                        "accept": [
                            "text/csv",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            "application/vnd.ms-excel",
                        ],
                        "max_size_mb": 25,
                        "multiple": False,
                    },
                },
                timeout=timedelta(hours=ctx.collect_timeout_hours),
            )
            if self._submitted_documents:
                root_task_id = runbook.info().workflow_id.split(":")[0]
                pending_docs = self._submitted_documents[:]
                self._submitted_documents.clear()
                for doc in pending_docs:
                    rows = await runbook.execute_activity(
                        ingest_submitted_document,
                        args=[doc.model_dump(mode="json"), root_task_id],
                        start_to_close_timeout=timedelta(minutes=2),
                    )
                    files_processed.append(
                        {
                            "document_ref": doc.document_ref,
                            "filename": doc.filename,
                            "content_type": doc.content_type,
                            "source": doc.source,
                            "accepted_rows": len(rows),
                        }
                    )
                    # target_rows is the minimum to advance the step
                    # (the outer await loop exits once we have ≥1), NOT
                    # a cap on rows accepted per file. Take everything
                    # the activity returned — earlier code truncated on
                    # the first row of a multi-row file.
                    for payload in rows:
                        self._submitted_events.append(ExpenseSubmissionPayload.model_validate(payload))
        file_rows = sum(int(f.get("accepted_rows", 0)) for f in files_processed)
        total_rows = len(self._submitted_events)
        direct_rows = max(total_rows - file_rows, 0)
        return {
            "summary": (
                f"Collected {total_rows} expense row(s) for {period_label} "
                f"({file_rows} from file upload, {direct_rows} direct data submissions)."
            ),
            "period": period_label,
            "target_rows": target_rows,
            "collected_rows": total_rows,
            "uploaded_files": files_processed,
            "direct_data_submission_rows": direct_rows,
            "closed_by_signal": False,
        }

    async def _categorise_submitted(
        self, ctx: ExpenseProcessorContext
    ) -> list[ExpensePersistedEvent]:
        """Per-row vendor lookup → category proposal → INSERT (PENDING
        or NEEDS_ATTENTION). NOT a ``@ui_step`` — this is mechanical
        post-submission processing, not a user-facing milestone. Runs
        as the tail of the ``collect`` step so the breadcrumb sequence
        stays honest about what the user actually does
        (Collect → Review → Commit).

        Sequential keeps activity event ordering predictable in the
        workflow history; parallelism via asyncio.gather would be a
        noticeable win only at hundreds-of-rows scale.
        """
        from uuid import UUID
        root_task_id = UUID(runbook.info().workflow_id.split(":")[0])
        rows: list[ExpensePersistedEvent] = []
        for ev in self._submitted_events:
            row = await runbook.execute_activity(
                categorise_and_insert_expense,
                CategoriseInput(
                    payload=ev,
                    tenant_slug=ctx.tenant_slug,
                    entity_id=ctx.entity_id,
                    entity_slug=ctx.entity_slug,
                    period=str(ctx.period) if ctx.period else None,
                    task_id=root_task_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
            )
            rows.append(row)
        return rows

    @ui_step(name="review", title="Review expenses", icon="Eye")
    async def _step_review(
        self,
        ctx: ExpenseProcessorContext,
        rows: list[ExpensePersistedEvent],
    ) -> dict[str, Any]:
        # DATA_TABLE config built from the SDK's review-config helper —
        # column set + actions stay consistent across subledger types.
        # We override the actions list to keep the existing
        # "Reject batch" / "Approve & post" pair the runbook expects.
        from ntro.subledger import review
        from ntro.subledger.types.expenses import ExpenseRow

        config = review.data_table_config(
            ExpenseRow,
            title=f"Review expenses — {ctx.entity_slug} / {self._display_period(ctx)}",
            actions=[
                {"kind": "reject", "label": "Reject batch"},
                {
                    "kind": "approve",
                    "label": "Approve & post",
                    "primary": True,
                    "icon": "ArrowRight",
                },
            ],
        )
        config["subtitle"] = (
            "Confirm vendor, amount and category for each row. "
            "Reject all to discard the batch."
        )
        config["datasets"] = [
            {
                "key": "line_items",
                "label": "Line items",
                "data_key": "line_items",
                "columns": config.get("columns", []),
            }
        ]
        config.pop("columns", None)
        config["allow_corrections"] = True

        line_items = [r.model_dump(mode="json") for r in rows]
        return await self.wait_for_action(
            payload={
                "line_items": line_items,
                "summary": (
                    f"{len(rows)} expense row(s) awaiting approval for "
                    f"{ctx.entity_slug} / {self._display_period(ctx)}."
                ),
            },
            display_hint={
                "component": "DATA_TABLE",
                "config": config,
            },
            reason=(
                f"Review {len(rows)} expense row(s) for {self._display_period(ctx)}. "
                f"Edit category or amount inline; approve to post."
            ),
        )

    @ui_step(name="commit", title="Commit", icon="CheckCircle2")
    async def _step_commit(
        self,
        ctx: ExpenseProcessorContext,
        rows: list[ExpensePersistedEvent],
        approval: dict[str, Any],
    ) -> ExpensePeriodResult:
        from uuid import UUID

        approved = approval.get("kind", "approve") != "reject"
        raw_corrections = approval.get("corrections", []) if isinstance(approval, dict) else []
        corrections = [
            ExpenseCorrection(
                expense_id=c.get("line_item_id") or c.get("expense_id") or c.get("id"),
                field=c.get("field"),
                new_value=c.get("new_value"),
            )
            for c in raw_corrections
            if (c.get("line_item_id") or c.get("expense_id") or c.get("id"))
            and c.get("field")
        ]
        for (expense_id, field), value in self._row_corrections.items():
            corrections.append(
                ExpenseCorrection(
                    expense_id=expense_id,
                    field=field,
                    new_value=value,
                )
            )
        selected_rows = [r.id for r in rows if r.id not in self._rejected_row_ids]
        root_task_id = UUID(runbook.info().workflow_id.split(":")[0])
        return await runbook.execute_activity(
            commit_expenses,
            CommitInput(
                tenant_slug=ctx.tenant_slug,
                entity_id=ctx.entity_id,
                entity_slug=ctx.entity_slug,
                period=str(ctx.period) if ctx.period else None,
                task_id=root_task_id,
                expense_ids=selected_rows,
                corrections=corrections,
                approved=approved,
            ),
            start_to_close_timeout=timedelta(seconds=30),
        )

    @ui_step(name="post_to_gl", title="Post to GL", icon="ArrowRight")
    async def _step_post_to_gl(
        self,
        ctx: ExpenseProcessorContext,
        commit_result: ExpensePeriodResult,
        rows: list[ExpensePersistedEvent],
    ) -> PostToGlResult:
        """Post APPROVED rows to the tenant's GL.

        Skips gracefully when:
        - No rows were APPROVED (commit step rejected the batch).
        - The tenant hasn't connected a GL (``ctx.gl`` empty/missing).

        In both cases the rows stay APPROVED on the subledger; the
        next operator action (next-period workflow run, manual ntro
        ops, or a fresh GL connection) handles them.
        """
        from uuid import UUID

        if commit_result.rows_approved == 0:
            workflow.logger.info(
                "expense_processor.post_to_gl: no APPROVED rows in this batch, skipping"
            )
            return PostToGlResult(
                posted=0,
                failed=0,
                skipped=commit_result.rows_total,
                submitted_expenses=[
                    {
                        "id": r.id,
                        "vendor": r.vendor,
                        "amount_gross": r.amount_gross,
                        "currency": r.currency,
                        "expense_date": r.expense_date,
                        "category": r.category,
                        "status": r.status,
                    }
                    for r in rows
                ],
            )

        if not ctx.gl:
            # Tenant hasn't connected a GL — degrade gracefully. Rows
            # stay APPROVED on the subledger; operator can post later.
            workflow.logger.info(
                "expense_processor.post_to_gl: tenant.config.gl not configured, skipping"
            )
            return PostToGlResult(
                posted=0,
                failed=0,
                skipped=commit_result.rows_approved,
                submitted_expenses=[
                    {
                        "id": r.id,
                        "vendor": r.vendor,
                        "amount_gross": r.amount_gross,
                        "currency": r.currency,
                        "expense_date": r.expense_date,
                        "category": r.category,
                        "status": r.status,
                    }
                    for r in rows
                ],
            )

        root_task_id = UUID(runbook.info().workflow_id.split(":")[0])
        result = await runbook.execute_activity(
            post_expenses_to_gl,
            PostToGlInput(
                tenant_slug=ctx.tenant_slug,
                entity_id=ctx.entity_id,
                entity_slug=ctx.entity_slug,
                task_id=root_task_id,
                expense_ids=[r.id for r in rows if r.id not in self._rejected_row_ids],
                # Slim tenant_config carrying ONLY the gl block.
                # The activity uses ``gl.for_entity`` which reads
                # ``tenant_config["gl"]["options"]``; workspace creds
                # come from the worker's env separately (Phase 4c).
                tenant_config={"gl": ctx.gl},
            ),
            start_to_close_timeout=timedelta(minutes=5),
        )
        result.submitted_expenses = [
            {
                "id": r.id,
                "vendor": r.vendor,
                "amount_gross": r.amount_gross,
                "currency": r.currency,
                "expense_date": r.expense_date,
                "category": r.category,
                "status": r.status,
            }
            for r in rows
        ]
        return result

    # ── Entry point ───────────────────────────────────────────────

    @runbook.run
    async def run(self, ctx: ExpenseProcessorContext) -> ExpensePeriodResult:
        await self._step_collect(ctx)
        # Categorise runs as the tail of collect's lifecycle — pure
        # mechanical post-submission processing, not a user-facing
        # milestone, so it doesn't get its own breadcrumb. The user's
        # mental model is "I submit rows → I review them"; categorise
        # is invisible plumbing.
        rows = await self._categorise_submitted(ctx)
        approval = await self._step_review(ctx, rows)
        commit_result = await self._step_commit(ctx, rows, approval)
        # The post-to-GL step is a separate breadcrumb. It either
        # succeeds (rows POSTED), fails per-row (operator-fixable
        # errors captured in the result), or skips (no GL configured /
        # nothing approved). We swallow the result here — the workflow
        # output is the commit result; the post is observed via the
        # step event log + the row-level posted_journal_ref column.
        await self._step_post_to_gl(ctx, commit_result, rows)
        return commit_result
