# TLA+ Model Guidance

This directory contains a teaching-oriented, direct-TLA+ model for one
blob-stream virtual partition. Treat the model as an executable design contract,
not as a transcription of Rust control flow.

## Before Editing

- Read `README.md`, the comments around the action or invariant you will touch,
  and the relevant product contract in `../docs/design/README.md`.
- Preserve the current model boundary unless the task explicitly expands it:
  one virtual partition, bounded brokers/batches/time, producer lease and
  Hi-Lo safety, and blob-to-metadata-to-acknowledgement publication ordering.
  The model also includes one complete-view reader with a monotonic cursor;
  eventually consistent observation and Fast frontiers remain outside it.
- Keep documented limitations explicit. The stale-writer publication and
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

When changing the stale-writer witness, also run:

```sh
make check-stale-writer-safety
make witness-stale-writer
```

The witness Make target succeeds only after the residual-safety configuration
passes and TLC then fails specifically at `NoStaleWriterPublicationLoss`. Do
not treat an arbitrary nonzero TLC exit as an expected witness.

When changing the eventually consistent metadata witness, also run:

```sh
make check-eventual-metadata-safety
make witness-eventual-metadata
```

This target similarly requires its residual-safety configuration to pass before
TLC fails specifically at `NoEventualMetadataLoss`. The witness must model an
incomplete replica observation and bounded Fast eligibility; do not represent
the loss as deleting durable metadata or as merely dropping a deferred row.

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
witnesses. Prefer phase-gated witness actions over TLC `CONSTRAINT`: a
constraint can remove transitions and hide the causal behavior being documented.
Each witness must have a passing residual-safety configuration and a named,
validated expected-failure configuration with a documented trace.
