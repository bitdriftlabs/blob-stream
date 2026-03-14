#!/bin/bash

set -x
set -e

# Install library compile deps.
if ! [[ -z "$RUNNER_TEMP" ]]; then
  sudo apt-get remove -y --purge man-db
  sudo apt-get update
  sudo apt-get install lld
fi

if ! [[ -z "$DOCKER_COMPOSE_UP" ]]; then
  docker compose up -d
fi
