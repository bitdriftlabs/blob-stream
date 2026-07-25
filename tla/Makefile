# Run the small Stage 1 model with TLC.
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

.PHONY: check help

help:
	@echo "Run: make check"
	@echo "Override the detected JAR: make check TLA_TOOLS_JAR=/absolute/path/to/tla2tools.jar"

check:
	@test -n "$(TLA_TOOLS_JAR)" || \
		(echo "Could not find tla2tools.jar; run 'make help' for installation guidance."; exit 2)
	@test -f "$(TLA_TOOLS_JAR)" || \
		(echo "TLA_TOOLS_JAR does not name a file: $(TLA_TOOLS_JAR)"; exit 2)
	java $(JAVA_FLAGS) -cp "$(TLA_TOOLS_JAR)" tlc2.TLC -config "$(CONFIG)" "$(MODULE)"
