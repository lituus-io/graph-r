#!/usr/bin/env bash
# Author: spicyzhug@gmail.com
# Copyright: lituus-io, all rights reserved
#
# Clone the sibling repositories pinned in .github/siblings next to this
# checkout (the layout every relative path dependency expects). Existing
# directories are kept, so a local workspace is never touched.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
token="${SIBLING_TOKEN:-}"
auth="${token:+x-access-token:${token}@}"
while read -r name ref; do
  # A Windows checkout may hand the file over with CRLF line endings.
  ref="${ref%$'\r'}"
  [[ -z "${name}" || "${name}" == \#* ]] && continue
  dest="${root}/../${name}"
  [[ -e "${dest}" ]] && continue
  git clone --quiet --depth 1 --branch "${ref}" "https://${auth}github.com/lituus-io/${name}.git" "${dest}"
done < "${root}/.github/siblings"
