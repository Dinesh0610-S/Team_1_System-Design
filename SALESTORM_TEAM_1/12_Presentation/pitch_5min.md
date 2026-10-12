# Phase 11A: 5-Minute Jury Pitch Script & Final Walkthrough
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1 (3 Speakers: **Student A [Architect]**, **Student B [Concurrency & Resilience Lead]**, **Student C [LLD, DB & Validation Lead]**)  

---

## 1. Exact 5-Minute (300-Second) Pitch Outline & Speaker Script

| Segment | Time | Speaker | Visual Artifact Shown | Key Message |
| :--- | :---: | :--- | :--- | :--- |
| **1. The Problem** | `0:00 – 0:30` (**30s**) | Student A | Title Slide / Funnel Diagram | 10,000 concurrent buyers for 100 units breaks naive databases via TOCTOU overselling, row-lock exhaustion, and lost paid orders during downstream crashes. |
| **2. Requirements & Invariants** | `0:30 – 1:00` (**30s**) | Student A | `01_Requirements/requirements.md` | Hard Guarantees (`sold + reserved <= 100`, 0 duplicate charges, 0 lost paid orders, RPO=0) vs. Target SLOs (10k RPS normal, 500k RPS flash peak, `p99 < 150ms`). |
| **3. High-Level Design (HLD)** | `1:00 – 2:00` (**60s**) | Student A | `02_HLD/hld_architecture.md` (C4 Container) | 4-Stage Traffic Funnel: Edge Virtual Waiting Room $\rightarrow$ Redis Atomic Lua Gate $\rightarrow$ PostgreSQL ACID Ledger $\rightarrow$ Transactional Outbox + Kafka. |
| **4. Critical Design: Concurrency & Inventory** | `2:00 – 3:00` (**60s**) | Student B | `03_LLD/inventory_reservation.md` | Two-Layer Hybrid Gate (Redis Lua + PostgreSQL `WHERE available >= 1`), Two-Layer Idempotency, and `FOR UPDATE SKIP LOCKED` TTL Expiry Sweeper. |
| **5. Payment & Order Outage Recovery** | `3:00 – 3:45` (**45s**) | Student B | `03_LLD/payment_order.md` | `PAYMENT_PENDING` grace lock, `TIMEOUT_UNKNOWN` reconciliation, and how the Local ACID Outbox + Circuit Breaker survives a 30s `OrderService` outage. |
| **6. LLD, SOLID & Design Patterns** | `3:45 – 4:30` (**45s**) | Student C | `03_LLD/low_level_design.md` & `06_SOLID/` | Class diagram (10 `IOrderState` classes), Strategy/Adapter/Factory zero-touch extensibility, and 16-table PostgreSQL DDL with `CHECK` constraints. |
| **7. 50× Scalability & Live Validation** | `4:30 – 5:00` (**30s**) | Student C | `11_AI_Assisted_Validation/validation_report.md` | 10-Bucket Redis sharding (with ring fallback) + Edge Sold-Out short-circuit at 500k RPS, backed by real 64-thread simulation proof (`10,212` requests, `100` sold, `0` oversells). |

---

### Full Word-for-Word Speaker Script (Timed to 300 Seconds)

#### [0:00 – 0:30] Segment 1: The Problem (30 Seconds — Student A)
> "Good morning judges. We are Team 1 presenting **SALESTORM**. Imagine a flash sale where **10,000 customers click 'Buy Now' at the exact same millisecond for just 100 units** of `PRODUCT_X`. In a traditional e-commerce architecture, three catastrophic failures happen simultaneously: first, read-then-write race conditions oversell the 100 units; second, holding database locks across a 2-second external payment call starves the connection pool; and third, if `OrderService` crashes for 30 seconds right after a customer's card is charged, paid orders vanish. SALESTORM is architected from first principles to make all three failures structurally impossible."

#### [0:30 – 1:00] Segment 2: Requirements & Hard Guarantees vs. Targets (30 Seconds — Student A)
> "In Phase 1, we mapped the full 11-stage customer pipeline from Discovery to Live Shipment Tracking and separated **Hard Guarantees** from **Target SLOs**. Our Hard Guarantees are non-negotiable invariants that hold 100% of the time: **never oversell (`available + reserved + sold = 100` and `available >= 0`)**, **never double-charge or create duplicate orders**, **never lose a paid order (`RPO = 0` via synchronous PostgreSQL standby)**, and **zero raw card storage (PCI-DSS tokenization)**. Our Target SLOs sustain **10,000 req/s normal traffic**, **500,000 req/s flash peak**, and **p99 reservation latency under 150ms**."

#### [1:00 – 2:00] Segment 3: High-Level Design — The 4-Stage Traffic Funnel (60 Seconds — Student A)
> "Looking at our C4 Container Diagram in Phase 2, our core architectural insight is a **4-Stage Traffic Funnel** that sheds load at the cheapest layer first:
> * **Stage 1 (Edge):** Cloudflare CDN serves 99% of catalog reads, while a **Token-Bucket Virtual Waiting Room** absorbs bursts up to 500,000 req/s and admits a controlled flow to our API Gateway.
> * **Stage 2 (Fast-Path Gate):** `ReservationService` runs an **atomic single-threaded Lua script in Redis**. In under `0.1 milliseconds`, Redis checks the `Idempotency-Key`, distinguishes cache misses (`-1`) from `0`, and decrements stock from 100 to 0. **9,900 losing requests are rejected with `409 SOLD_OUT` in RAM without ever touching PostgreSQL!**
> * **Stage 3 (Authoritative Ledger):** Only the **100 Redis winners** open a PostgreSQL transaction to lock a 10-minute reservation (`RESERVED`) and proceed to `PaymentService`.
> * **Stage 4 (Async Order Pipeline):** When payment succeeds, `PaymentService` commits the payment, marks stock `SOLD`, and writes an `OrderRequested` event to the `outbox_event` table in **one local ACID transaction**, which `OutboxPublisherWorker` streams via **Apache Kafka** to `OrderService`, `FulfilmentService`, `ShipmentService`, and `NotificationService`."

#### [2:00 – 3:00] Segment 4: Critical Design — Concurrency & Inventory Control (60 Seconds — Student B)
> "In Phase 3, we evaluated five concurrency strategies. We rejected Optimistic Locking because 10,000 threads reading `version = 1` triggers a 99.9% retry storm, and we rejected Pessimistic `SELECT ... FOR UPDATE` as the front gate because queuing 10,000 requests on one row lock exhausts DB connections.
> Instead, our **Two-Layer Hybrid Gate** pairs the Redis Lua script with a single-statement PostgreSQL conditional update:
> `UPDATE inventory SET available_quantity = available_quantity - 1, reserved_quantity = reserved_quantity + 1, version = version + 1 WHERE product_id = 'PRODUCT_X' AND available_quantity >= 1`.
> Even if Redis failed over, PostgreSQL's row-level write lock and `CHECK (available_quantity >= 0)` constraint guarantee zero overselling.
> Every reservation has a 10-minute TTL. Our `ExpirySweeperWorker` reclaims abandoned carts using `FOR UPDATE SKIP LOCKED` scoped strictly to `WHERE status = 'RESERVED'` (leaving in-flight `PAYMENT_PENDING` rows to `PaymentReconciliationWorker`), and our Redis release script atomically increments stock while deleting the expired `Idempotency-Key` so stale retries receive `410 Gone`."

#### [3:00 – 3:45] Segment 5: Payment Reliability & 30s OrderService Outage Recovery (45 Seconds — Student B)
> "In Phase 4, when a winner calls `POST /api/v1/payments`, we first transition their reservation to `PAYMENT_PENDING` with a 60-second grace extension so the expiry sweeper cannot steal their stock mid-charge.
> * If the gateway **declines the card (5%)**, we atomically transition `PAYMENT_PENDING -> PAYMENT_FAILED -> RELEASED` and increment `available_quantity + 1` in PostgreSQL and Redis so the next buyer in line gets the unit.
> * If the gateway **times out at 3 seconds**, we mark the payment `TIMEOUT_UNKNOWN` (`202 Accepted`) and hold the reservation until `PaymentReconciliationWorker` queries the gateway—preventing phantom oversells. And if a late payment ever arrives after expiry, our `rowcount == 1` settlement guard triggers an automatic gateway refund (`REFUNDED`).
> * Finally, when **`OrderService` crashes for 30 seconds**, `POST /api/v1/payments` still succeeds 100% because `OrderRequested` is saved in the local `outbox_event` table. `OutboxPublisherWorker` trips its **Circuit Breaker `OPEN`** after 5 failures, waits with exponential backoff, probes `HALF_OPEN` when `OrderService` recovers, and drains all 100 orders idempotently via `UNIQUE(reservation_id)`."

#### [3:45 – 4:30] Segment 6: Low-Level Design, SOLID & Extensibility (45 Seconds — Student C)
> "In Phases 5, 6, and 7, we translated this into clean OOP and our 16-table PostgreSQL DDL.
> * We applied **SRP** and **DIP** so `ReservationOrchestrator` and `PaymentSagaOrchestrator` depend solely on interfaces (`IStockGate`, `IInventoryRepository`, `IPaymentGatewayAdapter`).
> * Using the **State Pattern (`IOrderState` across all 10 state classes)**, `OrderStateMachine` strictly enforces `CREATED -> PAYMENT_PENDING -> CONFIRMED -> PROCESSING -> SHIPPED -> OUT_FOR_DELIVERY -> DELIVERED` and rejects illegal jumps with `422`.
> * Using **Strategy, Factory, and Adapter patterns**, adding a new payment provider like PayPal or a new carrier like BlueDart requires adding **one class** and **zero changes** to core orchestrators."

#### [4:30 – 5:00] Segment 7: 50× Scalability & Empirical Validation (30 Seconds — Student C)
> "To scale 50× to **500,000 req/s**, we shard the 100 units across **10 Redis buckets (`10 × 10 units`)** with fallback ring probing `(bucket + i) % 10` for 1M Lua ops/sec and broadcast a 1-second **Edge Sold-Out Short-Circuit flag** to the API Gateway the instant all buckets hit zero.
> Finally, in Phase 10, we executed our 64-thread Python + SQLite WAL simulation against **10,212 requests** (10,000 buyers, 200 duplicates, 5% payment failures, `TIMEOUT_UNKNOWN` reconciliation, late-payment auto-refund, and a full `OrderService` outage): **all 100 units sold, 0 oversold, 200 idempotent replays caught, 5 failed payments reclaimed and re-sold, and all 100 orders reached `CONFIRMED` after outage recovery.** Thank you—we are ready for your questions!"

---

## 2. Scripted Answer to the Final Mandatory Jury Question

> **Jury Question:** *"100 units, 10,000 Buy Now clicks: walk through exactly what happens from request arrival to final confirmed orders."*

### Team 1 Step-by-Step Scripted Response:

1. **$t = 0\text{ ms}$ — Arrival at the Edge (`10,000 Requests`):**
   * 10,000 authenticated customers click **"Buy Now"** (`POST /api/v1/reservations`), each carrying a JWT and a client-generated UUIDv4 `Idempotency-Key` header (plus ~200 network-retry duplicate requests carrying the same `Idempotency-Key`).
   * Cloudflare WAF, the Virtual Waiting Room, and the API Gateway validate the JWT and `Idempotency-Key` header (`400 Bad Request` if missing) and forward the requests to stateless `ReservationService` pods.
2. **$t = 0.1\text{ ms} \text{ to } 10\text{ ms}$ — Fast-Path Atomic Contention Gate (`RedisLuaStockGate`):**
   * Each request executes our atomic Lua script on Redis (`EVALSHA`). Because Redis is single-threaded per shard, all 10,200 calls are serialized in memory without lock thrashing:
     * **The 200 Duplicate Retries** hit `GET {product:X}:idem:{key}` in the Lua script and immediately return `Code 2 (IDEMPOTENT_REPLAY)` $\rightarrow$ `200 OK` with zero stock change.
     * **The First 100 Distinct Requests** see `stock >= 1`, decrement `stock` from `100` down to `0`, record their idempotency key, and return `Code 1 (RESERVED_WINNER)`.
     * **The Remaining 9,900 Requests** see `stock == 0`, return `Code 0 (SOLD_OUT)` in `< 1 ms`, and receive **`409 Conflict (SOLD_OUT)`** without ever touching PostgreSQL! Furthermore, the API Gateway sets a 1-second `edge:sold_out:PRODUCT_X` flag to short-circuit subsequent floods.
3. **$t = 10\text{ ms} \text{ to } 50\text{ ms}$ — Authoritative PostgreSQL Reservation (`100 Winners Only`):**
   * Only the 100 Redis winners open a PostgreSQL `READ COMMITTED` transaction and execute:
     ```sql
     UPDATE inventory
     SET available_quantity = available_quantity - 1,
         reserved_quantity  = reserved_quantity + 1,
         version            = version + 1
     WHERE product_id = 'PRODUCT_X' AND available_quantity >= 1;
     ```
   * Even if two threads raced at the DB level on the 100th unit, PostgreSQL row-level locking and `CHECK (available_quantity >= 0)` ensure `rowcount == 1` for the winner and `rowcount == 0` for the loser.
   * Each of the 100 winners inserts an `inventory_reservation` row (`status = 'RESERVED'`, `expires_at = NOW() + 10 minutes`, `UNIQUE(idempotency_key)`) and receives **`201 Created`**. Ledger state is now: **`available = 0, reserved = 100, sold = 0`**.
4. **$t = 1\text{ s} \text{ to } 5\text{ s}$ — Idempotent Payment Execution (`95% Success, 5% Failure`):**
   * The 100 reservation holders call `POST /api/v1/payments` with a payment `Idempotency-Key` and tokenized card (`pm_tok_*`).
   * `PaymentSagaOrchestrator` first locks each reservation in `PAYMENT_PENDING` (`WHERE status = 'RESERVED' AND expires_at > NOW()`) and extends `expires_at` by a 60s grace window so `ExpirySweeperWorker` cannot expire a reservation while the gateway call is in flight.
   * `PaymentService` calls the External Payment Gateway via `CircuitBreakerProxy` (passing deterministic `transaction_reference = tx_ref_{reservation_id}`):
     * **95 Winners Succeed (`200 OK`):** For each of the 95 successes, `PaymentService` commits **one local ACID transaction** updating `payment.status = 'SUCCESS'`, `inventory_reservation.status = 'SOLD'`, `inventory` (`reserved - 1, sold + 1`), and inserting an `OrderRequested` event into `outbox_event`.
     * **5 Winners Fail Payment (`402 Payment Required`):** For the 5 declined cards (or any user whose 10-minute TTL expires via `ExpirySweeperWorker`), `PaymentService` atomically updates `payment.status = 'FAILED'`, transitions `inventory_reservation.status = 'RELEASED'`, restores `inventory` (`reserved - 1, available + 1`), and executes Redis Lua `INCRBY +1` (guarded by `SETNX release:{reservation_id}`).
5. **$t = 5\text{ s} \text{ to } 8\text{ s}$ — Wave 2 Re-Reservation of the 5 Released Units:**
   * The 5 restored units (`available = 5, reserved = 0, sold = 95`) are immediately claimed by 5 waiting buyers, who reserve and pay successfully.
   * Final Inventory Ledger reaches its immutable terminal state: **`available = 0, reserved = 0, sold = 100` (`total_stock = 100`)**.
6. **$t = 5\text{ s} \text{ to } 35\text{ s}$ — Surviving the 30-Second `OrderService` Outage & Final Order Confirmation:**
   * Suppose `OrderService` crashes for 30 seconds during the sale. Because all 100 paid purchases wrote their `OrderRequested` events into the local `outbox_event` table inside the payment transaction, **zero paid purchases are lost**.
   * `OutboxPublisherWorker` detects 5 consecutive failures calling `OrderService`, trips its `CircuitBreakerProxy` to **`OPEN`**, and schedules retries with exponential backoff + jitter.
   * At $t = 35\text{ s}$, `OrderService` recovers. The Circuit Breaker enters **`HALF_OPEN`**, succeeds on its probe request, resets to **`CLOSED`**, and drains all 100 `OrderRequested` events into Kafka / `OrderService`.
   * `OrderService` inserts each order using `INSERT INTO orders ... ON CONFLICT (reservation_id) DO NOTHING`, transitions `CREATED` $\rightarrow$ `CONFIRMED` via `OrderStateMachine`, and emits `OrderConfirmed` to `FulfilmentService` and `NotificationService`.
   * **Final Result:** **100 units sold, 100 unique gateway charges, 100 `CONFIRMED` orders, 0 oversold, 0 duplicate orders, and 0 lost payments.**
