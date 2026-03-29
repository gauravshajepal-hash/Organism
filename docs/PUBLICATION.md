# Publication Layer

## Goal

The public publication layer turns internal organism state into a static GitHub-facing research surface.

It is designed to be:

- `one way`
- `static`
- `redacted`
- `inspectable`

## Outputs

The exporter writes:

- `docs/data/latest.json`
  - public bundle with stats, discoveries, positive results, negative results, related work
- `docs/data/graph.json`
  - nodes and edges for the interactive graph page
- `docs/papers/chimera-lab-research-synthesis.md`
  - research paper in markdown
- `docs/papers/chimera-lab-research-synthesis.html`
  - browser-friendly paper view

## Public Dashboard

The public dashboard is in:

- `docs/index.html`
- `docs/dashboard.js`
- `docs/style.css`

This page is read-only. It fetches exported JSON and renders:

- project stats
- latest research
- top discoveries
- positive results
- negative results
- related work

## Graph View

The graph view is in:

- `docs/graph.html`
- `docs/graph.js`

It is an Obsidian-like exploration surface:

- click nodes
- inspect details in a side panel
- trace edges between missions, programs, runs, artifacts, discoveries, and related work

## Redaction Policy

Before publication:

- scrub obvious local filesystem paths
- scrub obvious bearer token patterns
- prefer summaries over raw payload dumps

This is not a perfect secrecy system. It is a publication gate for a local-first organism, not a full data-loss-prevention suite.

## Workflow

1. Run research locally.
2. Accumulate artifacts, discoveries, and failures.
3. Export public bundle.
4. Push `docs/` to GitHub.
5. Serve with GitHub Pages or use the repo directly as a public record.

## GitHub Pages

The repository includes:

- `.github/workflows/publish-pages.yml`

That workflow deploys the `docs/` directory directly to GitHub Pages on push or manual dispatch.
