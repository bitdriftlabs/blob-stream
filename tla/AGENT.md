# TLA+ Model Guidance

This directory contains a teaching-oriented, direct-TLA+ model for one
blob-stream virtual partition. Treat the model as an executable design contract,
not as a transcription of Rust control flow.

## Before Editing

- Read `README.md`, the comments around the action or invariant you will touch,
  and the relevant product contract in `../DESIGN.md`.
- Preserve the current model boundary unless the task explicitly expands it:
  one virtual partition, bounded brokers/batches/time, and producer lease plus
  Hi-Lo safety before publication or reader behavior.
- Keep documented limitations explicit. The planned stale-writer publication and
  eventually-consistent metadata-loss paths are expected witness behaviors, not
  invariants to disprove.

## Modeling Rules

- Use direct TLA+ actions. Model nondeterminism with alternatives and bounded
  quantifiers; do not encode one preferred execution order.
- Every action must define the next-state value of every variable, either with a
  primed assignment or `UNCHANGED`.
- Keep every TLC state dimension finite. Bound clocks, counters, process
  incarnations, and generated identities in the `.cfg` file.
- Do not add unconditional `UNCHANGED vars` to `Next`; it hides unexpected TLC
  deadlocks. Use a named, guarded quiescent action only for an explicitly
  completed bounded scenario.
- Keep comments liberal. Explain both the TLA+ mechanism and the corresponding
  blob-stream behavior, especially where a compact expression is non-obvious.
- Add `TypeOK` coverage for every new variable and document every new invariant
  in `README.md`.

## Verification

Run these commands from this directory after every model or configuration edit:

```sh
make check
git -C .. diff --check
git -C .. status --short --ignored tla
```

`make check` automatically finds the macOS TLA+ Toolbox JAR. Override it only
when the Toolbox is installed elsewhere:

```sh
make check TLA_TOOLS_JAR="/absolute/path/to/tla2tools.jar"
```

Record the TLC state count, depth, duration, and memory use when changing model
bounds. TLC's `states/` directory is generated runtime output and is ignored.

## Adding Stages

Extend the model in small, independently checked layers. First add the state,
transitions, `TypeOK` clauses, and safety invariants; then run TLC before adding
the next layer. Use separate constrained configurations for intentional loss
witnesses, and document their expected traces rather than treating them as
ordinary passing safety checks.
