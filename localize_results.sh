#!/usr/bin/env bash
set -euo pipefail

# Usage: ./process_and_copy.sh /path/to/source /path/to/dest
if [[ $# -ne 2 ]]; then
  echo "Usage: $0 <source_dir> <dest_dir>" >&2
  exit 1
fi

SRC_DIR=$1
DEST_DIR=$2

# Ensure source exists
if [[ ! -d "$SRC_DIR" ]]; then
  echo "Source directory does not exist: $SRC_DIR" >&2
  exit 1
fi

# Create destination if it doesn't exist
mkdir -p "$DEST_DIR"

# Require 'zip'
if ! command -v zip >/dev/null 2>&1; then
  echo "'zip' command not found. Please install it (e.g., 'sudo apt install zip')." >&2
  exit 1
fi

# Size threshold: 5 MB in bytes
THRESHOLD=$((5 * 1024 * 1024))

# A list (array) to collect the filenames to copy
declare -a FILES_TO_COPY=()
declare -a ZIPS_TO_REMOVE=()

# Helper: get file size in bytes (Linux/BSD compatible)
get_size() {
  local f="$1"
  if stat --version >/dev/null 2>&1; then
    # GNU stat (Linux)
    stat -c %s -- "$f"
  else
    # BSD/Mac stat
    stat -f %z -- "$f"
  fi
}

# Iterate over regular files in SRC_DIR (non-recursive), handling spaces safely
while IFS= read -r -d '' f; do
  base="$(basename "$f")"
  size="$(get_size "$f")"

  if (( size > THRESHOLD )); then
    # Compress into c<oldname>.zip placed alongside the original in SRC_DIR
    out_zip="${base}.zip"
    out_path="${SRC_DIR%/}/$out_zip"

    # -j: junk paths (store just the filename), -q: quiet, -m not used (we keep original)
    zip -j -q "$out_path" "$f"
    FILES_TO_COPY+=("$out_path")
    ZIPS_TO_REMOVE+=("$out_path")
  else
    # Keep as-is
    FILES_TO_COPY+=("$f")
  fi
done < <(find "$SRC_DIR" -maxdepth 1 -type f -print0)

# Copy all files in the list to DEST_DIR (preserve times/permissions)
for path in "${FILES_TO_COPY[@]}"; do
  cp -p -- "$path" "$DEST_DIR/"
done

# Remove only the newly-created zips after successful copy
for zip_path in "${ZIPS_TO_REMOVE[@]}"; do
  rm -f "$zip_path"
done

# Optionally print a summary
echo "Processed files:"
for path in "${FILES_TO_COPY[@]}"; do
  echo "  $(basename "$path")"
done
echo "Copied to: $DEST_DIR"
