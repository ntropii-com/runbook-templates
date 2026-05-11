---
id: rb-document-ingest
slug: document-ingest
name: Document Ingest
description: >
  Generic child workflow that ingests one document. Receives bytes via signal,
  parses, AI-extracts using the configured schema, surfaces extracted lines
  for HITL review, then commits to the working set. Returns an
  IngestedDocument to the parent on approval.
classification: DOCUMENT_OPS
tags:
  - Document
  - Extraction
  - Child Workflow
  - HITL
jurisdictions:
  - any
version: 0.1.0
state: DRAFT
author: Ntropii

steps:
  - id: await_submission
    label: Awaiting Document
    component: FILE_UPLOAD
    responsibility: client
    timeout_minutes: 1440
    config:
      accept: ["application/pdf"]
      max_size_mb: 25
      instructions: >
        Submit the document via ntro_document_submit (MCP) or upload manually.
        The source identifier must match this child's configured source.
    description: Blocks until a file matching the configured source is signalled.

  - id: parse_pdf
    label: Parse PDF
    activity: parse_pdf
    component: LOADING
    timeout_minutes: 5
    description: Capability-backed PDF parse → cell grid + plain text.

  - id: extract_fields
    label: AI Extraction
    activity: extract_fields
    component: LOADING
    timeout_minutes: 5
    description: AI extraction via the schema-routed prompt (e.g. rent-roll-zenko).

  - id: review_extraction
    label: Review Extracted Data
    component: DATA_TABLE
    responsibility: accountant
    timeout_minutes: 1440
    config:
      # Default columns are placeholders — Claude adapts these per schema.
      # For Zenko rent rolls, see the demo target section below.
      columns: []
      row_actions: [edit]
      footer_actions: [approve, reject]
    description: HITL review. Accountant edits cells, approves or rejects.

  - id: commit
    label: Commit
    activity: commit_extraction
    component: SUMMARY
    config:
      sections:
        - {key: source, label: Source}
        - {key: line_count, label: Lines, type: number}
        - {key: confidence, label: Confidence, type: badge}
        - {key: corrections_applied, label: Corrections, type: number}
    description: Persist the approved extraction to tenant Postgres, return to parent.

templates:
  - {path: templates/workflow.py, description: DocumentIngestWorkflow with signal-based document receipt}
  - {path: templates/activities.py, description: parse_pdf, extract_fields, commit_extraction}
  - {path: templates/models.py, description: DocumentIngestContext, RawDocument, ExtractedPayload, IngestedDocument}
  - {path: templates/requirements.txt, description: ntro-core, ntro-providers, schema-specific provider deps}

config_schema:
  global:
    source:
      type: string
      description: Stable source identifier matched by ntro_document_submit (e.g. "zenko-rent-roll")
    schema_slug:
      type: string
      description: Extraction schema identifier routing to the right ntro-provider-anthropic prompt (e.g. "rent-roll-zenko")
  steps:
    review_extraction:
      columns:
        type: array
        description: DATA_TABLE column definitions for the extracted lines. Schema-specific — Claude generates from the extraction schema.
        default: []
      validation_rules:
        type: array
        description: Optional client-side validations applied to edited cells before approval (e.g. amount > 0)
        default: []
---

## When to use

`document-ingest` is the standard child workflow for any inbound document that needs:
- Receipt via either MCP signal (`ntro_document_submit`) or tenant UI upload
- Parsing (PDF for now; future: xlsx, docx, eml)
- AI extraction against a named schema
- HITL review with row-level edits
- Persistence to the tenant data plane on approval

Parents dispatch one `document-ingest` child per `expected_documents` entry. Each child is independently durable, signal-addressable, and re-runnable.

## Workflow patterns

The await step uses Temporal signals — the workflow blocks until a `document_submitted` signal arrives:

```python
@workflow.signal
async def document_submitted(self, payload: DocumentSubmissionPayload):
    self._submitted = payload  # resolved by wait_condition

@workflow.run
async def run(self, ctx: DocumentIngestContext) -> IngestedDocument:
    await self.run_step("await_submission", lambda: self.wait_for_signal("document_submitted"))
    raw = await self.run_step("parse_pdf", parse_pdf, self._submitted)
    extracted = await self.run_step("extract_fields", extract_fields, ExtractInput(raw=raw, schema=ctx.schema))
    approved = await self.run_step("review_extraction", lambda: self.wait_for_action(payload=extracted))
    return await self.run_step("commit", commit_extraction, CommitInput(extracted=extracted, corrections=approved.corrections))
```

`wait_for_signal` and `wait_for_action` are SDK helpers in `ntro.workflow`. They block the step (UI shows the corresponding component as active/waiting) until the named signal/action arrives.

## What the workflow uses

**Workflow-local activities** — defined in `templates/activities.py`. Adapted per tenant only when the data-plane schema or commit semantics differ:

| Activity | Used in step | What it does |
|---|---|---|
| `parse_pdf` | `parse_pdf` | Wraps `ntro.capabilities.pdf.parse(bytes)` |
| `extract_fields` | `extract_fields` | Wraps `ntro.capabilities.extraction.extract(content, schema)` |
| `commit_extraction` | `commit` | Persists extracted payload + corrections to tenant Postgres via `ntro.data` |

**Shared capabilities** — provider-backed, dispatched via `ntro.capabilities.*`:

| Capability | Used in | Provider (PoC) |
|---|---|---|
| `ntro.capabilities.pdf.parse(bytes) → CellGrid` | `parse_pdf` | `ntro-provider-local` (pdfplumber) |
| `ntro.capabilities.extraction.extract(content, schema) → ExtractionResult` | `extract_fields` | `ntro-provider-anthropic` (Haiku — schema slug routes to the right prompt) |
| `ntro.capabilities.storage.write(path, bytes)` | `commit_extraction` (audit trail) | `ntro-provider-local` (filesystem) |

**Pure library imports:**

| Import | Used in | Purpose |
|---|---|---|
| `ntro.data.get_data_plane(tenant_slug)` | `commit_extraction` | Raw asyncpg connection |
| `ntro.capabilities.extraction.ExtractionResult` | model typing | Pydantic model used in payloads |

## Adapting this runbook

What Claude changes per schema:

1. **`config_schema.global.schema`** — the schema slug that routes to the right extraction prompt in `ntro-provider-anthropic`. Adding a new schema = adding a new prompt to the provider's prompt library + (optionally) a new Pydantic typed payload model in the provider's models.
2. **`config_schema.steps.review_extraction.columns`** — DATA_TABLE column definitions matching the schema's typed fields. Claude generates these from the extraction schema definition.
3. **`config_schema.steps.review_extraction.validation_rules`** — optional cell-level validations (amount > 0, currency match, etc.).

What stays unchanged:

- 5-step structure (await → parse → extract → review → commit)
- Signal-based receipt
- HITL via DATA_TABLE
- Commit semantics (persist to `submitted_documents` and `extracted_payloads` tables in tenant Postgres)

## Re-run semantics

If a child is re-run (parent re-dispatches), the new run gets a fresh signal handler. The previous extraction (if persisted) remains in `extracted_payloads` for audit. The parent decides which extraction to use (typically the latest). Cleaner detection (incremental re-extraction, partial state) is deferred post-PoC.

## Observability

- Activity returns are typed `ObservableResult` subclasses (`RawDocument`, `ExtractedPayload`, `IngestedDocument`) — emitted as observer events post-PoC; recorded in Temporal event history in PoC
- The `ExtractionResult.confidence_scores` field is the primary signal for runbook quality regression — when observer ships, low-confidence extractions become a Tier 2 analysis trigger
- Live UI updates flow via api-tenant ↔ Temporal as for the parent

## Demo target — Zenko rent roll (March 2026)

For the byng / fund-one-uk / 4-high-court-limited demo, this child is configured as:

- `source: "zenko-rent-roll"`
- `schema: "rent-roll-zenko"` — routes to the `ntro-provider-anthropic` prompt that knows the Zenko statement layout (10 units, 7 × 1-bed + 3 × 2-bed, gross rent / received / arrears columns, management fee + maintenance deductions, net BACS remittance)
- `review_extraction.columns`: see fixture `byng-fund-ops/fund-one-uk/4-high-court-limited/periods/2026-03/expected-extraction.json`

Acceptance: the `review_extraction` DATA_TABLE surfaces 10 rows (one per unit) plus a footer summary (gross rent, deductions, net BACS) for HITL approval. On approval, the returned `IngestedDocument.extracted_payload` matches the expected-extraction fixture (modulo HITL corrections).
