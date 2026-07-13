#!/bin/bash

# Find the cargo.toml file
CARGO_TOML="Cargo.toml"
if [ ! -f "$CARGO_TOML" ]; then
  echo "Error: $CARGO_TOML not found"
  exit 1
fi

# Default to cargo path
USE_GIT_PATH=false

# Process command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --git-path)
      USE_GIT_PATH=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--git-path]"
      echo "  --git-path    Use git path format instead of cargo path"
      exit 1
      ;;
  esac
done

# Get absolute paths for repositories
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
SEARCH_DIR="$ROOT_DIR"
while [[ "$SEARCH_DIR" != "/" && ! -d "$SEARCH_DIR/shared-core" ]]; do
  SEARCH_DIR="$(dirname "$SEARCH_DIR")"
done

if [[ ! -d "$SEARCH_DIR/shared-core" ]]; then
  echo "Error: shared-core not found in any parent directory of $ROOT_DIR"
  exit 1
fi

SHARED_CORE_PATH="$SEARCH_DIR/shared-core"

# Create a temporary file
TMP_FILE=$(mktemp)

SHARED_CORE_GIT_URL="https://github.com/bitdriftlabs/shared-core.git"

# Process file line by line
while IFS= read -r line; do
  if echo "$line" | grep -q "git = \"$SHARED_CORE_GIT_URL\""; then
    crate_name=$(echo "$line" | sed -E 's/^([a-zA-Z0-9_-]+)[[:space:]]*=.*/\1/')

    if $USE_GIT_PATH; then
      echo "$line" | sed "s#git = \"$SHARED_CORE_GIT_URL\"#git = \"file://$SHARED_CORE_PATH\"#g" >> "$TMP_FILE"
    else
      echo "$line" | sed "s#git = \"$SHARED_CORE_GIT_URL\"#path = \"$SHARED_CORE_PATH/$crate_name\"#g" >> "$TMP_FILE"
    fi
  else
    echo "$line" >> "$TMP_FILE"
  fi
done < "$CARGO_TOML"

# Replace original file
mv "$TMP_FILE" "$CARGO_TOML"

if $USE_GIT_PATH; then
  echo "All dependencies swapped to local git paths:"
  echo "  - shared-core: file://$SHARED_CORE_PATH"
else
  echo "All shared-core dependencies swapped to local cargo paths"
fi
