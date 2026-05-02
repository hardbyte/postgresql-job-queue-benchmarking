# Architecture Decision Records

Design decisions for the cross-system benchmark harness itself —
adapter contracts, scenario shapes, output formats, repo layout.
Per-system results live under [`../../results/`](../../results/);
per-system implementation notes live in each `<system>-bench/`.

Format follows [awa's ADR style](https://github.com/hardbyte/awa/tree/main/docs/adr):
short, dated, status-tagged, focused on the *why*.

## Index

- [001 — Unified phase-driven driver](001-unified-phase-driver.md)
  — fold standalone `chaos.py` into the phase DSL so there is one
  entry point and one adapter contract. *Implemented.*

## Adding an ADR

1. Pick the next number.
2. Title `<NNN>-<kebab-slug>.md`.
3. Lead with **Status** (Proposed / Accepted / Implemented / Superseded)
   and a one-line **Context**.
4. Link from this index.
