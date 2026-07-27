#!/usr/bin/env zsh

set -uo pipefail

if (( $# < 3 )) || [[ "$1" == "-h" || "$1" == "--help" ]]; then
  print -u2 -- "Usage: $0 <iterations> -- <cargo nextest arguments>"
  print -u2 -- "Example: $0 100 -- -p blob-stream-integration-tests -E 'test(my_test)'"
  exit 2
fi

iterations="$1"
shift

if [[ ! "$iterations" =~ '^[1-9][0-9]*$' ]]; then
  print -u2 -- "iterations must be a positive integer: $iterations"
  exit 2
fi

if [[ "$1" != "--" ]]; then
  print -u2 -- "expected -- before cargo nextest arguments"
  exit 2
fi
shift

if (( $# == 0 )); then
  print -u2 -- "cargo nextest arguments are required"
  exit 2
fi

script_dir="${0:A:h}"
repo_root="${script_dir:h}"
cpu_count="$(sysctl -n hw.ncpu)"
parallel_runs=$(( iterations < cpu_count ? iterations : cpu_count ))
run_root="${repo_root}/.tmp/deflake-$(date +%Y%m%dT%H%M%S)-$$"
mkdir -p "$run_root"

print -- "Deflaking $iterations iteration(s) with up to $parallel_runs parallel run(s)."
print -- "Logs: ${run_root#$repo_root/}"

typeset -a failed_runs=()
typeset -a pids=()
typeset -a run_numbers=()

run_one() {
  local run_number="$1"
  shift
  local run_dir="$run_root/run-$(printf '%04d' "$run_number")"
  local log_file="$run_dir/test.log"
  local status_file="$run_dir/exit-status"

  mkdir -p "$run_dir"
  {
    print -- "Command: cargo nextest run $*"
    print -- "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    print -- ""
    cd "$repo_root" || exit 1
    cargo nextest run "$@"
  } >"$log_file" 2>&1
  local exit_code=$?
  print -- "$exit_code" >"$status_file"
  exit "$exit_code"
}

next_run=1
while (( next_run <= iterations )); do
  pids=()
  run_numbers=()

  while (( next_run <= iterations && ${#pids} < parallel_runs )); do
    run_one "$next_run" "$@" &
    pids+=("$!")
    run_numbers+=("$next_run")
    (( next_run += 1 ))
  done

  for index in {1..${#pids}}; do
    local_pid="${pids[$index]}"
    run_number="${run_numbers[$index]}"
    run_dir="$run_root/run-$(printf '%04d' "$run_number")"

    if wait "$local_pid"; then
      print -- "PASS run $(printf '%04d' "$run_number"): ${run_dir#$repo_root/}"
    else
      print -u2 -- "FAIL run $(printf '%04d' "$run_number"): ${run_dir#$repo_root/}"
      failed_runs+=("$run_dir")
    fi
  done
done

if (( ${#failed_runs} == 0 )); then
  print -- "PASS: all $iterations iteration(s) completed successfully."
  exit 0
fi

print -u2 -- "FAIL: ${#failed_runs} of $iterations iteration(s) failed."
for run_dir in "${failed_runs[@]}"; do
  print -u2 -- "  ${run_dir#$repo_root/}"
done
exit 1
