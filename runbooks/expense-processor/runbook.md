---
id: rb-expense-processor
slug: expense-processor
name: Expense Processor
description: >
  Always-on inbox workflow for one entity. Accepts expense rows submitted via
  ntro_task_data_ingest (kind="expense") from a remote agent reading email
  receipts, proposes a category from the bundled vendor map, persists each
  as PENDING in the entity's `expenses` table, and surfaces the batch for
  HITL review before flipping status to APPROVED.
classification: TREASURY
tags:
  - Expense
  - Data Ingest
  - HITL
  - Receipts
jurisdictions:
  - any
version: 0.1.1
state: DRAFT
author: Ntropii

steps:
  - id: collect
    label: Collect expense rows
    component: LOADING
    responsibility: client
    timeout_minutes: 1440
    config:
      instructions: >
        Submit each expense via ntro_task_data_ingest with kind="expense".
        Workflow advances once at least one accepted row is processed.
    description: Blocks on data/file submissions until at least one accepted row is available.

  - id: categorise
    label: Categorise + persist
    activity: expense_processor.categorise_and_insert
    component: LOADING
    timeout_minutes: 5
    description: Per-row vendor → category lookup; INSERT PENDING into the expenses table.

  - id: review
    label: Review expenses
    component: DATA_TABLE
    responsibility: accountant
    timeout_minutes: 1440
    config:
      data_key: line_items
      allow_corrections: true
      actions:
        - { kind: reject, label: Reject batch }
        - { kind: approve, label: Approve & post, primary: true }
    description: HITL — accountant edits category/amount per row, approves the batch.

  - id: commit
    label: Commit
    activity: expense_processor.commit_expenses
    component: SUMMARY
    config:
      sections:
        - { key: rows_approved, label: Approved, type: number }
        - { key: rows_rejected, label: Rejected, type: number }
        - { key: corrections_applied, label: Corrections, type: number }
        - { key: total_amount_by_category, label: By category }
    description: Apply corrections, flip status to APPROVED/REJECTED, return summary.

templates:
  - { path: templates/workflow.py, description: ExpenseProcessorWorkflow with multi-signal accumulation }
  - { path: templates/activities.py, description: categorise_and_insert + commit_expenses }
  - { path: templates/models.py, description: Context + payload + correction + summary models }
  - { path: templates/vendor_map.json, description: Bundled vendor → category lookup table }

config_schema:
  global:
    max_rows:
      type: number
      default: 5
      description: Deprecated. Collect step now advances after >=1 accepted row.
---

# Expense Processor

A canonical row-shaped runbook for fund-ops expense capture. The agent
(Cowork, Copilot Studio, etc.) reads inbound receipts from email, parses
each into a structured row, and submits via `ntro_task_data_ingest` with
`kind="expense"`. The runbook accumulates rows, proposes
a GL category from the bundled vendor map, stages each as `PENDING` for
HITL review, then flips status to `APPROVED` once the accountant signs
off the batch.

## Skill — what the agent should extract per receipt

Tell the LLM to populate the `data` field of each `ntro_task_data_ingest`
call with these fields, in this canonical shape:

```json
{
  "vendor": "Uber",
  "amount": 12.50,
  "currency": "GBP",
  "date": "2026-04-12",
  "payment_method": "personal-card",
  "line_items": [
    {"description": "Trip 19:42 → Liverpool St", "amount": 11.50},
    {"description": "Tip", "amount": 1.00}
  ],
  "vat_amount": 0.00,
  "notes": "Client meeting return leg"
}
```

### Vendor families to recognise

The bundled `vendor_map.json` covers the substring matches the runbook
uses. Anything not in the map lands as `category=null` for the reviewer
to set. Coverage today:

- **Travel/Transport** — Uber, Lyft, Bolt, Addison Lee, taxis, trains
  (LNER, GWR, CrossCountry, SNCF, Trenitalia), TfL/Oyster, parking
  (NCP), fuel (Shell, BP, Esso).
- **Travel/Lodging** — Airbnb, Booking.com, hotels.com, Marriott,
  Hilton, Premier Inn, Travelodge, Ibis, Novotel, Accor.
- **Meals/Client** — generic "restaurant" matches.
- **Meals/Team** — Pret, Leon, Deliveroo, Uber Eats, Just Eat,
  Starbucks, Costa, Caffè Nero.
- **Office Supplies** — Amazon, eBay, Argos, Ryman, Viking Direct.
- **Software** — GitHub, GitLab, Vercel, Linear, Notion, Figma, Slack,
  Anthropic, OpenAI, Datadog, Sentry, Stripe, Google Workspace,
  Microsoft 365, Atlassian, JetBrains.
- **Conferences** — Eventbrite, Tito, generic "summit"/"conference"
  matches.
- **Communications** — BT, Vodafone, EE, O2, Three.
- **Professional Fees** — legal, solicitor, accounting, audit.

### Heuristics — what to skip

- **Refunds** — negative amounts; route through a separate
  `refund-processor` runbook (not yet implemented), not this one.
- **Order confirmations not yet shipped** — wait until the receipt /
  shipped notification arrives.
- **Personal/shared bills** — ask the user to confirm before submitting.
- **VAT/sales tax** — extract as a separate `vat_amount` field where
  the receipt itemises it; reclaim handled at posting, not here.

## Period scope

PoC: advances after the first accepted row so users get immediate feedback
and can review quickly. Rows are stamped to periods from receipt dates
(`YYYY-MM`) unless an optional global `period` override is provided.

## Out of scope (filed separately)

- **LLM categorisation fallback** for vendors not in the map.
- **Xero / accounting-system push** at posting time.
- **Multi-payee** — single payee per workflow run for now.
- **FX conversion** — store native currency only.
- **Auto-pull from email** via `ntro.capabilities.email` — entry path
  is Cowork-driven for the first cut.
