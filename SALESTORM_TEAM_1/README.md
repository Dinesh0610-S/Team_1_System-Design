# SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform
**Event:** SALESTORM | SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  

---

## One-Paragraph Executive Design Summary

**SALESTORM** is a design-first, high-concurrency flash-sale e-commerce architecture engineered to absorb **10,000+ simultaneous purchase requests (scaling up to 500,000 req/s at 50× flash peak) competing for 100 units of inventory** with **zero overselling, zero duplicate charges, and zero lost paid orders**. The platform acts as a **4-Stage Traffic Funnel**: (1) an **Edge CDN & Token-Bucket Virtual Waiting Room** absorbs massive read and bot spikes; (2) a fast-path **Redis Single-Threaded Atomic Lua Gate (`RedisLuaStockGate`)** validates idempotency and filters 10,000 buyers down to 100 reservation winners in sub-millisecond memory time (`409 SOLD_OUT` for the remaining 9,900 without touching the database); (3) an authoritative **PostgreSQL 16 ACID Ledger** enforces single-statement conditional updates (`WHERE available_quantity >= 1`), `CHECK (available_quantity >= 0)` conservation constraints, and multi-worker-safe TTL expiry sweeps (`FOR UPDATE SKIP LOCKED`) while `PaymentService` settles tokenized charges idempotently; and (4) a **Transactional Outbox + 3-State Circuit Breaker + Apache Kafka Saga** decouples payment settlement from downstream `OrderService` creation, empirically proven by our live simulation to survive a **30-second `OrderService` outage**, **5% payment failure rate**, and **2% duplicate request retries** with 100% order confirmation and zero invariant violations.

---

## Complete Repository Folder Index

```text
SALESTORM_TEAM_1/
├── 01_Requirements/
│   └── requirements.md                  # 11-stage functional pipeline, measurable NFRs, Hard Guarantees vs Target SLOs, Canonical Glossary
├── 02_HLD/
│   └── hld_architecture.md              # C4 Context, Container, Component & Multi-AZ Deployment Mermaid diagrams, bottlenecks & 50x scaling
├── 03_LLD/
│   ├── inventory_reservation.md         # 5-way concurrency comparison, Hybrid Lua+SQL gate, TTL ExpirySweeper, induction proof, last-unit race
│   ├── payment_order.md                 # 5 payment/order flows, TIMEOUT_UNKNOWN reconciliation, 30s OrderService outage recovery, Order State Machine
│   ├── low_level_design.md              # Layered responsibility matrix, Mermaid Class Diagram, 3 Sequence Diagrams, 3 State Diagrams
│   └── class_diagram.puml               # Formal PlantUML version of the complete domain Class Diagram
├── 04_Database/
│   ├── database_design.md               # Mermaid ER Diagram for all 16 core tables, partial B-Tree indexing & HOT update optimization
│   └── schema.sql                       # Executable PostgreSQL 16 DDL with PK/FK, CHECK (available >= 0), conservation constraints & audit fields
├── 05_API/
│   ├── openapi.yaml                     # OpenAPI 3.0.3 REST specification with mandatory Idempotency-Key header, JWT Auth & ErrorModel
│   └── event_catalogue.md               # 5 Synchronous Commands & 11 Asynchronous Domain Events with Kafka topics & idempotent consumers
├── 06_SOLID/
│   └── solid_principles.md              # Mapping of SRP, OCP, LSP, ISP, and DIP directly to Phase 5 classes with architectural trade-offs
├── 07_Design_Patterns/
│   └── design_patterns.md               # 8 Patterns (Strategy, Factory, State, Observer, Adapter, Facade, Repository, Circuit Breaker) + Extensibility Proofs
├── 08_Scalability_Reliability/
│   └── scalability_reliability.md       # Horizontal auto-scaling (10k -> 500k RPS), caching, sharding, and outage playbooks (Redis, DB, Gateway, Order)
├── 09_Security_Observability/
│   └── security_observability.md        # 7-layer security, PCI-DSS tokenization, Prometheus metrics, JSON logs, OpenTelemetry traces & P0/P1 alerts
├── 10_ADR/
│   └── architecture_decision_records.md # 7 formal ADRs (SQL vs NoSQL, Sync vs Async, Concurrency, Caching, Outbox/Saga vs 2PC, Kafka, Waiting Room)
├── 11_AI_Assisted_Validation/
│   ├── simulate_flash_sale.py           # Executable Python 3 + SQLite WAL simulation (10k buyers, 100 units, 95% pay ok, 5% fail, 2% dup, 30s outage)
│   ├── validation_report.md             # Unmodified terminal output capture & mathematical reconciliation of all simulation counters
│   └── AI_USAGE_NOTE.md                 # Transparency log of AI tools used, human design reviews, and simulation bug caught & fixed
├── 12_Presentation/
│   ├── pitch_5min.md                    # Exact 5-minute (300s) timed speaker script (30s/30s/60s/60s/45s/45s/30s) & final jury walkthrough answer
│   └── jury_qa_and_traces.md            # Complete Q&A defense guide + step-by-step traces for all 7 mandatory hackathon demonstration scenarios
└── README.md                            # Root blueprint index and one-paragraph executive design summary
```

---

## Quick Verification: How to Run the Phase 10 Concurrency & Outage Simulation

No external dependencies are required (uses Python 3 standard library `sqlite3` in WAL mode + `ThreadPoolExecutor(max_workers=64)` + `asyncio`):

```powershell
python "SALESTORM_TEAM_1\11_AI_Assisted_Validation\simulate_flash_sale.py"
```

### Verified Invariants Asserted on Every Run:
1. **Zero Overselling:** `sold_quantity == 100`, `available_quantity == 0`, `reserved_quantity == 0` (`available + reserved + sold == 100` across all 39 mid-flight snapshots).
2. **Never Negative Stock & Safe Cache-Miss Recovery:** Enforced by Redis Lua gate (`Code -1 CACHE_MISS` single-flight DB seed on Redis restart) + SQL `CHECK (available_quantity >= 0)`.
3. **Zero Duplicate Reservations or Charges:** `200` concurrent duplicate `/api/v1/reservations` calls and `20` duplicate `/api/v1/payments` calls return `200 OK` idempotent replays (`gateway_net_settled_charges == 100`), while expired keys return `410 Gone`.
4. **TTL, Failed-Payment & Late-Payment Reclamation:** `5` expired reservations and `5` failed payments (`5%`) atomically restore stock to `available_quantity`, `1` `TIMEOUT_UNKNOWN` (`202`) payment is reconciled by `PaymentReconciliationWorker`, and `1` late payment arriving after expiry is caught by our `rowcount == 1` guard and automatically refunded (`REFUNDED`).
5. **30-Second `OrderService` Outage Recovery:** Circuit Breaker trips `OPEN`, buffers all `100` `OrderRequested` events in `outbox_event`, probes `HALF_OPEN` $\rightarrow$ `CLOSED` on recovery, and confirms all `100` orders.

---

## Quick Navigation Links

* **Phase 1 — Requirements:** [`01_Requirements/requirements.md`](./01_Requirements/requirements.md)
* **Phase 2 — High-Level Design:** [`02_HLD/hld_architecture.md`](./02_HLD/hld_architecture.md)
* **Phase 3 — Concurrency & Inventory LLD:** [`03_LLD/inventory_reservation.md`](./03_LLD/inventory_reservation.md)
* **Phase 4 — Payment & Order LLD:** [`03_LLD/payment_order.md`](./03_LLD/payment_order.md)
* **Phase 5 — Class, Sequence & State Diagrams:** [`03_LLD/low_level_design.md`](./03_LLD/low_level_design.md) | [`03_LLD/class_diagram.puml`](./03_LLD/class_diagram.puml)
* **Phase 6 — SOLID & Design Patterns:** [`06_SOLID/solid_principles.md`](./06_SOLID/solid_principles.md) | [`07_Design_Patterns/design_patterns.md`](./07_Design_Patterns/design_patterns.md)
* **Phase 7 — Database DDL & OpenAPI 3.0:** [`04_Database/database_design.md`](./04_Database/database_design.md) | [`04_Database/schema.sql`](./04_Database/schema.sql) | [`05_API/openapi.yaml`](./05_API/openapi.yaml) | [`05_API/event_catalogue.md`](./05_API/event_catalogue.md)
* **Phase 8 — Scalability, Security & Observability:** [`08_Scalability_Reliability/scalability_reliability.md`](./08_Scalability_Reliability/scalability_reliability.md) | [`09_Security_Observability/security_observability.md`](./09_Security_Observability/security_observability.md)
* **Phase 9 — Architecture Decision Records:** [`10_ADR/architecture_decision_records.md`](./10_ADR/architecture_decision_records.md)
* **Phase 10 — Empirical Validation:** [`11_AI_Assisted_Validation/simulate_flash_sale.py`](./11_AI_Assisted_Validation/simulate_flash_sale.py) | [`11_AI_Assisted_Validation/validation_report.md`](./11_AI_Assisted_Validation/validation_report.md) | [`11_AI_Assisted_Validation/AI_USAGE_NOTE.md`](./11_AI_Assisted_Validation/AI_USAGE_NOTE.md)
* **Phase 11 — Presentation & Jury Defense:** [`12_Presentation/pitch_5min.md`](./12_Presentation/pitch_5min.md) | [`12_Presentation/jury_qa_and_traces.md`](./12_Presentation/jury_qa_and_traces.md)
