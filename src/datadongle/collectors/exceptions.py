from __future__ import annotations

# SchemaDriftError moved to datadongle.core.exceptions when drift detection
# became an engine concern rather than a collector one. Re-exported here so the
# original import path keeps working.
from datadongle.core.exceptions import SchemaDriftError

__all__ = ["SchemaDriftError"]
