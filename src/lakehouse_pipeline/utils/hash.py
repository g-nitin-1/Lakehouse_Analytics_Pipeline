from __future__ import annotations

import hashlib


def composite_key_hash(*parts: object) -> str:
    normalized = "||".join(str(part).strip().lower() for part in parts)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
