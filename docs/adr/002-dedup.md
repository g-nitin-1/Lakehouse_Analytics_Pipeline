# ADR 002: Deterministic Dedup Strategy

## Decision
Use `order_id` as business key and keep latest record by `updated_at` desc.

## Rationale
- Predictable and reproducible tie-break behavior.
- Aligns with common CDC-like correction patterns.
