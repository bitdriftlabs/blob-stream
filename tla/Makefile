# Run bounded blob-stream TLA+ models with TLC.
#
# First install the Toolbox on macOS:
#   brew install --cask tla+-toolbox
#
# The default searches the standard macOS Toolbox location. Keep the JAR outside
# the repository. This avoids committing a large binary and lets a separately
# downloaded tools JAR override the discovered path when needed.

TLA_TOOLS_JAR ?= $(shell find "/Applications/TLA+ Toolbox.app" -type f -name tla2tools.jar -print -quit 2>/dev/null)
CONFIG ?= BlobStreamPartition.cfg
MODULE ?= BlobStreamPartition
JAVA_FLAGS ?= -XX:+UseParallelGC

.PHONY: check check-stale-writer-safety witness-stale-writer check-eventual-metadata-safety \
	witness-eventual-metadata help

help:
	@echo "Run: make check"
	@echo "Run the stale-writer residual-safety scenario: make check-stale-writer-safety"
	@echo "Print the expected stale-writer loss trace: make witness-stale-writer"
	@echo "Run the eventual-metadata residual-safety scenario: make check-eventual-metadata-safety"
	@echo "Print the expected eventual-metadata loss trace: make witness-eventual-metadata"
	@echo "Override the detected JAR: make check TLA_TOOLS_JAR=/absolute/path/to/tla2tools.jar"

check:
	@test -n "$(TLA_TOOLS_JAR)" || \
		(echo "Could not find tla2tools.jar; run 'make help' for installation guidance."; exit 2)
	@test -f "$(TLA_TOOLS_JAR)" || \
		(echo "TLA_TOOLS_JAR does not name a file: $(TLA_TOOLS_JAR)"; exit 2)
	java $(JAVA_FLAGS) -cp "$(TLA_TOOLS_JAR)" tlc2.TLC -config "$(CONFIG)" "$(MODULE)"

check-stale-writer-safety:
	$(MAKE) --no-print-directory check MODULE=StaleWriterWitness CONFIG=StaleWriterWitness.cfg

# This target has two deliberately different expectations:
#
# 1. The phase-gated scenario must pass every residual safety invariant.
# 2. Negating the known accepted loss must fail specifically at
#    NoStaleWriterPublicationLoss, and TLC must print that counterexample.
#
# TLC output is kept under its ignored states/ directory while the target checks
# the expected error name. This prevents an unrelated parser, deadlock, or
# invariant failure from being counted as the intended witness.
witness-stale-writer: check-stale-writer-safety
	@output="states/stale-writer-witness.out"; \
	if $(MAKE) --no-print-directory check MODULE=StaleWriterWitness CONFIG=StaleWriterWitnessExpectedFailure.cfg > "$$output" 2>&1; then \
		cat "$$output"; \
		rm -f "$$output"; \
		echo "Expected NoStaleWriterPublicationLoss to fail, but TLC passed."; \
		exit 1; \
	fi; \
	cat "$$output"; \
	grep -Fq "Error: Invariant NoStaleWriterPublicationLoss is violated." "$$output" || { \
		rm -f "$$output"; \
		echo "TLC failed, but not at NoStaleWriterPublicationLoss."; \
		exit 1; \
	}; \
	rm -f "$$output"; \
	echo "Observed the expected NoStaleWriterPublicationLoss counterexample."

check-eventual-metadata-safety:
	$(MAKE) --no-print-directory check MODULE=EventualMetadataWitness CONFIG=EventualMetadataWitness.cfg

# This target mirrors witness-stale-writer: safety must pass first, then TLC
# must fail specifically at the named accepted eventually-consistent loss.
witness-eventual-metadata: check-eventual-metadata-safety
	@output="states/eventual-metadata-witness.out"; \
	if $(MAKE) --no-print-directory check MODULE=EventualMetadataWitness CONFIG=EventualMetadataWitnessExpectedFailure.cfg > "$$output" 2>&1; then \
		cat "$$output"; \
		rm -f "$$output"; \
		echo "Expected NoEventualMetadataLoss to fail, but TLC passed."; \
		exit 1; \
	fi; \
	cat "$$output"; \
	grep -Fq "Error: Invariant NoEventualMetadataLoss is violated." "$$output" || { \
		rm -f "$$output"; \
		echo "TLC failed, but not at NoEventualMetadataLoss."; \
		exit 1; \
	}; \
	rm -f "$$output"; \
	echo "Observed the expected NoEventualMetadataLoss counterexample."
