# AI Usage & Engineering Validation Note (`AI_USAGE_NOTE.md`)
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  

---

## 1. AI Tools Used

* **Primary AI Assistant:** Google DeepMind Antigravity (System Architect & Jury Audit Agent)
* **Verification & Execution Environment:** Local Python 3.12 (`ThreadPoolExecutor(max_workers=64)`) + SQLite 3 (WAL mode) execution environment on Windows PowerShell.

---

## 2. What AI Was Used For vs. What Team 1 Architected & Validated

| Phase / Artifact | How AI Assisted | What Team 1 Reviewed, Corrected & Validated |
| :--- | :--- | :--- |
| **Phases 1–2: Requirements & HLD (`01_Requirements/`, `02_HLD/`)** | Structured the 11-stage functional pipeline, NFR tables, and 5 C4/HLD Mermaid diagrams (Context, HLD Tiers, Container, Component, Multi-AZ Deployment). | Verified strict separation of **Hard Guarantees** (Zero Overselling, Zero Duplicate Charges, RPO=0) vs. **Target SLOs** (10k RPS / 500k RPS), added fallback ring probing `(bucket + i) % 10` for 10-bucket Redis sharding, and ensured every arrow in C4 diagrams is marked `[SYNC]` or `[ASYNC]`. |
| **Phases 3–5: Concurrency, Payment & LLD (`03_LLD/`)** | Drafted the 5-way concurrency comparison, Redis Lua scripts (`tryReserve` and `releaseStock`), SQL CTE for `ExpirySweeperWorker`, sequence diagrams, and PlantUML/Mermaid class diagrams. | Audited and fixed three subtle distributed race conditions during our strict self-audit: (`HIGH-01`) scoped `ExpirySweeperWorker` strictly to `status = 'RESERVED'` and added a `rowcount == 1` guard + automated gateway refund in `Tx_Pay_Settle`; (`HIGH-02`) deleted `KEYS[3]` (`idempotency_key`) inside `RedisLuaStockGate.releaseStock` so expired keys return `410 Gone`; (`HIGH-03`) distinguished `nil` (`Code -1 CACHE_MISS`) from `"0"` (`Code 0 SOLD_OUT`) in Lua. |
| **Phases 6–7: SOLID, Patterns, DB & API (`04_`–`07_`)** | Generated the 16-table PostgreSQL DDL (`schema.sql`), OpenAPI 3.0 spec (`openapi.yaml`), Event Catalogue, and SOLID/Pattern mappings. | Verified all SQL constraints (`CHECK (available_quantity >= 0)`, partial unique index `uq_payment_active_reservation`, `UNIQUE(reservation_id)` on `orders`), added `POST /api/v1/checkout`, and removed all composite `PK,FK` tokens and `<br/>` tags from Mermaid diagrams for 100% parser compatibility. |
| **Phases 8–9: Scalability, Security, Observability & ADRs (`08_`–`10_`)** | Drafted 50× scaling architecture, OpenTelemetry trace flow, Prometheus alert rules, and 7 ADRs. | Ensured `StockReconciliationWorker` only heals Redis-PostgreSQL drift that persists for $> 10\text{s}$ ($> 2$ consecutive checks) to avoid racing with in-flight reservations, and verified Working Rule #3 trade-offs across all ADRs. |
| **Phase 10: Empirical Simulation (`11_AI_Assisted_Validation/`)** | Generated `simulate_flash_sale.py` to test 10,000 concurrent requests across 64 threads, 2% duplicates, 5% payment failures, `TIMEOUT_UNKNOWN` reconciliation, late-payment auto-refund, Redis restart `CACHE_MISS`, TTL expiry, and 30s `OrderService` outage. | Executed the multi-threaded suite locally and verified that every counter (`10,212` requests, `10,213` Lua evaluations, `version = 223`, `100` settled payments, `1` auto-refunded late payment, `100` confirmed orders) reconciles mathematically with zero invariant violations. |

---

## 3. Bugs & Edge Cases Caught & Fixed During Empirical Validation & Self-Audit (Transparency Log)

1. **Duplicate Retry Ordering in Simulation:**
   * *Observation:* When 200 duplicate `Idempotency-Key` requests were uniformly shuffled into the 10,000 request array, 195 arrived after stock hit `0`.
   * *Fix:* Replayed the 100 winning `Idempotency-Key` headers twice across 64 threads (`redis_idempotent_replays = 200`).
2. **Stale Redis Idempotency Key After Reservation Release (`HIGH-02`):**
   * *Observation:* If a user's reservation expired or their card declined, `releaseStock` incremented `{product:X}:stock` in Redis, but `{product:X}:idem:{key}` still held the old `reservation_id`. A retry with the same key would return `200 OK` pointing to a `RELEASED` reservation.
   * *Fix:* Updated `RedisLuaStockGate.releaseStock` to execute `redis.call('DEL', KEYS[3])` atomically alongside `INCRBY`, and updated the DB fallback to return `410 Gone` (`RESERVATION_EXPIRED`), verified in `Scenario 2A`.
3. **Late Payment Settlement After Reservation Expiry (`HIGH-01`):**
   * *Observation:* If a slow webhook/gateway response arrived after a reservation had already been released, an unconditional `UPDATE inventory SET reserved_quantity = reserved_quantity - 1` would either fail the `CHECK (reserved_quantity >= 0)` constraint or corrupt another buyer's reserved unit.
   * *Fix:* Scoped `ExpirySweeperWorker` strictly to `status = 'RESERVED'` and added an explicit `rowcount == 1` check on `UPDATE inventory_reservation ... WHERE status = 'PAYMENT_PENDING'` inside `Tx_Pay_Settle`, triggering an automatic gateway refund (`REFUNDED`) if `0` rows match, verified in `Scenario 2C`.
