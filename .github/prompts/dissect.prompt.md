---
name: Dissect Kubernetes Flow
description: "Dissect a Kubernetes subsystem end-to-end and generate an onboarding walkthrough in dissection/. Use when you need entry-point tracing, call-flow mapping, related code discovery, and beginner-friendly explanations with verification commands."
argument-hint: "Topic to dissect (e.g. 'PVC binding flow', 'HPA scaling decision path', 'authn/authz request path')"
agent: "Onboarding Mentor"
---
Dissect the user-provided topic end-to-end in this repository.

## Required behavior

1. Delegate execution to the `Onboarding Mentor` agent behavior and keep an onboarding-first explanation style.
2. Identify and explain the true entry point first, then trace the complete flow step-by-step.
3. Include **code source and related code** for every major step:
   - Primary source files that implement the step
   - Related files (interfaces, callers, tests, config, API types)
   - Exact file paths and line references
4. **Resolve interfaces to concrete implementations.** Wherever the flow crosses an interface (Go implicit satisfaction, dependency injection, factory functions), trace it to the concrete struct behind it and cite each hop: interface definition → `var _ Interface = &Struct{}` assertion → factory/constructor → DI/registry wiring → runtime selection. Place this trace inline at the relevant step; never stop at the interface boundary.
5. Use visual flow output (`1 -> 2 -> 3`, ASCII, or Mermaid) before deep dives.
6. Include actionable validation commands (`kubectl`, logs, or other local checks) so a beginner can verify the flow.
7. **Explain for a day-one reader.** Never present a code excerpt without first explaining, in plain language, what it does and what problem it solves. Define every domain term, flag, and magic constant inline the first time it appears (e.g. `doFullSync`, `maxSurge`, `clusterEndpoints`); decode opaque argument lists (iptables flags, bitmasks, stringly-typed keys) argument-by-argument, preferably as a table. Lead dense subsystems with two or three mental models and a short vocabulary table.
8. All narrative and generated content must be in English.

## Output requirements

- Create or update a walkthrough document under `dissection/` only, following `NN-topic-name.md` naming.
- Update `dissection/README.md` scenario table when creating a new walkthrough.
- Never write Markdown outside `dissection/`.
- If a request would write `.md` outside `dissection/`, stop and report the violation explicitly.

## Minimum section template for the generated walkthrough

- `## Big Picture` — one-paragraph summary + Mermaid or ASCII flow diagram
- `## Reading Guide (Beginner)` — trigger, first durable state change, success criterion, most common confusion
- `## Interface Resolution Guide (This Scenario)` — the interface hops this flow crosses and how to resolve them
- `## End-to-End Flow` — `1 -> 2 -> 3` / ASCII / Mermaid overview before deep dives
- `## Step-by-Step Code Trace` — numbered `### [N] Title` subsections
- `## Related Code Map` — table of step → file → key symbol → line
- `## Verify It Yourself` — runnable `kubectl`/`crictl`/log commands
- `## Gotchas` — `> ⚠️` callouts for non-obvious behavior
- `## Related Scenarios` — cross-links to sibling `dissection/` docs

For each step in `## Step-by-Step Code Trace`, include:
- A concept primer in plain language (what this step does and why it exists) **before** any code
- Every domain term, flag, and magic constant in the excerpt defined inline; opaque argument lists decoded argument-by-argument
- Primary source file references (exact `path/file.go:line`)
- Related code references (interfaces, callers, tests, config, API types)
- Concrete implementation behind any interface the step crosses (struct + `var _ ... = &...{}` assertion + factory + wiring)
