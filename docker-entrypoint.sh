#!/usr/bin/env bash
set -euo pipefail

source /opt/conda/etc/profile.d/conda.sh
conda activate rdkit

if [ "$#" -eq 0 ]; then
  set -- run
fi

exec python -m npc_labeler.cli "$@"

