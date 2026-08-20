#!/bin/sh
set -eu

cat >&2 <<'EOF'
Official Platform adapter publication is not configured.
[01B] must supply immutable artifact coordinates, manifest, checksums, signature, and provenance.
Private Platform source access, reconstruction, and vendoring are forbidden.
EOF
exit 78
