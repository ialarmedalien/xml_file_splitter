#!/usr/bin/env bash
set -euo pipefail

# Ensure at least one argument is provided
if [ "$#" -eq 0 ]; then
  echo "Usage: $0 {split} [args...]"
  exit 1
fi

cmd="$1"
shift

case "$cmd" in
  split)
    # Run the xml_file_splitter app
    exec /usr/bin/tini -- xml_file_splitter "$@"
    ;;
  bash)
    exec /usr/bin/tini -- /bin/bash
    ;;
  *)
    echo "Error: unknown command '$cmd'; the only valid command is 'split'" >&2
    exit 1
    ;;
esac
