# Phase 11B: Jury Q&A Defense Sheet & 7 Demonstration Traces
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  

---

## Part 1: Answers to Every "Jury Design Question"

### Q1: "Where is the exact point of contention in your system, and how do you prevent it from becoming a bottleneck?"
* **Answer:** Contention occurs on the single hot inventory counter for `PRODUCT_X`. We control it at **two exact choke points**:
  1. **In-Memory Fast Gate (`RedisLuaStockGate`):** A single-threaded atomic Lua script on Redis evaluates `Idempotency-Key` and decrements `{product:PRODUCT_X}:stock` in $\sim 0.05\text{ ms}$. Out of 10,000 requests, **9,900 are rejected in RAM** (`409 SOLD_OUT`) and never touch PostgreSQL.
  2. **Authoritative Disk Gate (`PostgresInventoryRepository`):** Only the **100 Redis winners** execute `UPDATE inventory SET available_quantity = available_quantity - 1, reserved_quantity = reserved_quantity + 1 WHERE product_id = 'PRODUCT_X' AND available_quantity >= 1`.
* **At 50× Traffic (500,000 req/s):** We eliminate single-shard Redis contention by **Token-Bucket Sharding** the 100 units across 10 Redis shards (`10 buckets × 10 units` with fallback ring probing `(bucket + i) % 10` so zero stock is stranded) and broadcasting a 1-second `edge:sold_out:PRODUCT_X` flag to the API Gateway so post-sellout traffic is rejected in local pod memory with zero network hops.

---

### Q2: "What happens if Redis crashes or gets out of sync with PostgreSQL? Can your system oversell?"
* **Answer:** **No, it can never oversell, and it never falsely rejects stock on restart.** PostgreSQL is the authoritative source of truth and enforces `WHERE available_quantity >= 1` on every `UPDATE` plus a storage-engine `CHECK (available_quantity >= 0)` constraint.
  * **If Redis has a higher number than PostgreSQL (e.g., Redis says 5, DB says 0):** Up to 5 requests pass Redis, hit PostgreSQL, get `rowcount == 0` on `WHERE available_quantity >= 1`, roll back, and receive `409 SOLD_OUT`. **Zero overselling.**
  * **If Redis restarts with an empty key (`GET` returns `nil`):** Our Lua script explicitly checks `if not raw_stock then return {-1, "CACHE_MISS"}` (rather than coercing `nil` to `0`) and triggers a single-flight lock (`SETNX {product:X}:init_lock`) to seed Redis from PostgreSQL `available_quantity`.
  * **If Redis crashes after decrementing before PostgreSQL commits (e.g., Redis says 0, DB says 1):** The system fails *safe* (briefly under-allocating by 1 unit) until `StockReconciliationWorker` (running every 5 seconds and healing only drift that persists across $> 2$ consecutive checks / $> 10\text{s}$ so in-flight transactions are never overwritten) heals the drift.
  * **If the entire Redis cluster is unreachable:** `ReservationOrchestrator` fails open to PostgreSQL conditional updates while the API Gateway tightens rate limits to PostgreSQL's safe write envelope (`~5,000 RPS`).

---

### Q3: "Why did you choose Atomic Conditional SQL Updates (`WHERE available_quantity >= 1`) instead of Optimistic Locking (`WHERE version = :v`) or Pessimistic Locking (`SELECT ... FOR UPDATE`)?"
* **Answer:**
  * **Why Optimistic Locking fails in flash sales:** If 10,000 requests read `available = 100, version = 1` at the same millisecond and try `UPDATE ... WHERE version = 1`, **1 succeeds and 9,999 fail** with `StaleStateException` even though 99 units are still available! Those 9,999 threads must retry in a loop, creating an $O(N^2)$ retry storm.
  * **Why Pessimistic `SELECT ... FOR UPDATE` is slower:** It requires two SQL statements (a `SELECT ... FOR UPDATE` round-trip from DB to app, followed by an `UPDATE` round-trip), doubling lock-hold duration.
  * **Why Single-Statement Conditional `UPDATE` wins:** `UPDATE inventory SET available = available - 1, reserved = reserved + 1, version = version + 1 WHERE product_id = 'PRODUCT_X' AND available >= 1` executes entirely inside the DB engine in **one round-trip**. Under `READ COMMITTED`, PostgreSQL locks the row only for the microsecond of the `UPDATE`, re-evaluates `WHERE available >= 1` against the latest committed tuple, and increments `version` for auditability.

---

### Q4: "What happens if a user's reservation TTL expires at the exact millisecond their payment is being processed by Stripe/Razorpay?"
* **Answer:** We solved this race condition in Phase 4 with **Three Defense Layers (`HIGH-01`)**:
  1. **`PAYMENT_PENDING` Grace Lock (`Tx_Pay_Init`):** Before calling the external payment gateway, `PaymentSagaOrchestrator` executes:
     ```sql
     UPDATE inventory_reservation
     SET status = 'PAYMENT_PENDING',
         expires_at = GREATEST(expires_at, CURRENT_TIMESTAMP + INTERVAL '60 seconds')
     WHERE reservation_id = $1 AND status = 'RESERVED' AND expires_at > CURRENT_TIMESTAMP;
     ```
     If `ExpirySweeperWorker` already expired the reservation 1 millisecond earlier (`status = 'RELEASED'`), this `UPDATE` returns `rowcount == 0`, and `PaymentService` aborts with `410 Gone` **before calling the payment gateway** (so the customer is never charged).
  2. **Strict Separation of Sweeper Scope:** `ExpirySweeperWorker` only sweeps `WHERE status = 'RESERVED' AND expires_at < NOW()`. It **never** touches `PAYMENT_PENDING` rows; expired `PAYMENT_PENDING` rows are handled exclusively by `PaymentReconciliationWorker`, which queries the payment gateway before releasing stock.
  3. **Settlement Rowcount Guard & Automated Refund:** In `Tx_Pay_Settle`, we assert `rowcount == 1` on `UPDATE inventory_reservation SET status = 'CONFIRMED' WHERE reservation_id = $1 AND status = 'PAYMENT_PENDING'` before decrementing `reserved_quantity`. If a late gateway webhook ever arrives after a reservation was already released (`rowcount == 0`), `Tx_Pay_Settle` rolls back the inventory mutation and issues an **immediate automated Gateway Refund (`REFUNDED`)**.

---

### Q5: "What happens if the External Payment Gateway times out after 3 seconds? Do you release the stock or retry?"
* **Answer:** **Neither immediately!** If an HTTP call to Stripe/Razorpay times out at `3,000 ms`, we do not know whether the customer's card was charged at `2,950 ms`.
  * If we released the stock immediately, and the charge actually succeeded at the bank, a 101st customer would buy the released unit—causing an **oversell** or an unfulfilled paid customer.
  * Instead, we mark the payment `TIMEOUT_UNKNOWN`, return `202 Accepted (Verifying Payment)`, keep the reservation locked in `PAYMENT_PENDING` (extending `expires_at` by 5 minutes), and let `PaymentReconciliationWorker` poll the gateway's status API (`GET /v1/charges?ref=tx_ref_{reservation_id}`).
  * If the gateway confirms `CHARGED`, we settle `PAYMENT_PENDING -> CONFIRMED -> SOLD` and emit `OrderRequested`. If the gateway confirms `NOT_FOUND / FAILED`, we transition `PAYMENT_PENDING -> PAYMENT_FAILED -> RELEASED` and restore the stock.

---

### Q6: "Why not use Two-Phase Commit (2PC) between PaymentService and OrderService?"
* **Answer:** Two-Phase Commit (2PC/XA) is a blocking protocol: if `OrderService` goes down for 30 seconds (the mandatory hackathon scenario), 2PC blocks `PaymentService` from committing payments and holds database row locks open across services, taking down the entire checkout pipeline.
  * By using the **Transactional Outbox Pattern**, `PaymentService` commits `payment = 'SUCCESS'`, `inventory = 'SOLD'`, and `outbox_event = 'OrderRequested'` in **one local ACID transaction** inside PostgreSQL.
  * Even when `OrderService` is completely dead for 30 seconds, the customer's payment succeeds immediately (`200 OK`), and `OutboxPublisherWorker` delivers the `OrderRequested` event as soon as `OrderService` recovers.

---

### Q7: "What if `ExpirySweeperWorker` runs on 5 Kubernetes pods simultaneously? Won't all 5 pods see the same expired reservation and increment `available_quantity` by +5?"
* **Answer:** **No.** Our `ExpirySweeperWorker` uses two defense mechanisms in SQL and one in Redis:
  1. `SELECT ... FROM inventory_reservation WHERE status = 'RESERVED' AND expires_at < NOW() FOR UPDATE SKIP LOCKED` ensures each worker pod locks a disjoint set of expired rows.
  2. The state update transitions `RESERVED -> TIMEOUT -> RELEASED` (`release_reason = 'TTL_EXPIRED'`). Only the worker that transitions the row gets `rowcount == 1` and increments `available_quantity + 1`.
  3. In Redis, `releaseStock` Lua checks `SETNX release:{reservation_id}` so Redis is never incremented twice, and executes `DEL {product:X}:idem:{idempotency_key}` (`HIGH-02`) so a stale client retry receives `410 Gone` rather than replaying a dead reservation.

---

## Part 2: The 7 Mandatory End-to-End Demonstration Traces

### Trace 1: Happy-Path Purchase (`Success`)
```text
[1] Client -> POST /api/v1/reservations (Idempotency-Key: idem-res-01, product_id: PRODUCT_X, qty: 1)
[2] RedisLuaStockGate: GET {product:X}:idem:idem-res-01 (MISS) -> DECR {product:X}:stock (100 -> 99) -> SET idem key -> Return {1, res_01} (0.4ms)
[3] PostgreSQL Tx_Reserve:
      UPDATE inventory SET available=99, reserved=1, version=2 WHERE product_id='PRODUCT_X' AND available >= 1 (rowcount=1)
      INSERT INTO inventory_reservation (res_01, status='RESERVED', expires_at=NOW()+10m)
      COMMIT -> Return 201 Created
[4] Client -> POST /api/v1/payments (Idempotency-Key: idem-pay-01, reservation_id: res_01, token: pm_tok_visa)
[5] PostgreSQL Tx_Pay_Init:
      UPDATE inventory_reservation SET status='PAYMENT_PENDING', expires_at=NOW()+60s WHERE reservation_id='res_01' AND status='RESERVED'
      INSERT INTO payment (pay_01, status='PENDING', tx_ref='tx_ref_res_01')
      COMMIT
[6] External Gateway Call (via CircuitBreakerProxy[CLOSED]): POST /charges (Idempotency-Key: tx_ref_res_01) -> 200 OK (prov_tx_id: ch_991)
[7] PostgreSQL Tx_Pay_Settle (Single Atomic Local Transaction):
      UPDATE inventory_reservation SET status='CONFIRMED' WHERE reservation_id='res_01' AND status='PAYMENT_PENDING' (assert rowcount==1)
      UPDATE inventory_reservation SET status='SOLD' WHERE reservation_id='res_01' AND status='CONFIRMED'
      UPDATE payment SET status='SUCCESS', provider_tx_id='ch_991'
      UPDATE inventory SET reserved=0, sold=1, version=3 (99 + 0 + 1 == 100)
      INSERT INTO outbox_event (evt_01, event_type='OrderRequested', status='PENDING')
      COMMIT -> Return 200 OK {"payment_status": "SUCCESS", "order_status": "CONFIRMING"}
[8] OutboxPublisherWorker -> OrderService:
      OrderStateMachine validates CREATED -> CONFIRMED
      INSERT INTO orders (ord_01, reservation_id='res_01', status='CONFIRMED') ON CONFLICT DO NOTHING
      UPDATE outbox_event SET status='PUBLISHED'
      Emit OrderConfirmed -> FulfilmentService (PROCESSING) -> ShipmentService (SHIPPED) -> NotificationService (Email Sent)
```

---

### Trace 2: Failed Payment (`5% Card Decline -> Stock Released`)
```text
[1] Client holds active reservation res_02 (Inventory: available=98, reserved=1, sold=1)
[2] Client -> POST /api/v1/payments (Idempotency-Key: idem-pay-02, reservation_id: res_02)
[3] PostgreSQL Tx_Pay_Init: Locks res_02 -> 'PAYMENT_PENDING', inserts payment pay_02 ('PENDING')
[4] External Gateway Call: Returns 402 Payment Required (error: INSUFFICIENT_FUNDS)
[5] PostgreSQL Tx_Pay_Fail (Compensating Local Transaction):
      UPDATE payment SET status='FAILED', failure_reason='INSUFFICIENT_FUNDS'
      UPDATE inventory_reservation SET status='PAYMENT_FAILED' WHERE reservation_id='res_02' AND status='PAYMENT_PENDING'
      UPDATE inventory_reservation SET status='RELEASED', release_reason='PAYMENT_FAILED' WHERE reservation_id='res_02' AND status='PAYMENT_FAILED'
      UPDATE inventory SET reserved=reserved-1 (0), available=available+1 (99), version=version+1
      INSERT INTO outbox_event (event_type='PaymentFailed')
      COMMIT
[6] RedisLuaStockGate.releaseStock: SETNX release:res_02 (1) -> INCRBY {product:X}:stock +1 (98 -> 99) -> DEL {product:X}:idem:idem-res-02
[7] Return 402 Payment Required {"payment_status": "FAILED", "stock_released": true}
[8] Next waiting customer in flash sale immediately reserves the restored unit!
```

---

### Trace 3: Duplicate Request Retry (`2% Network Retry on /reservations and /payments`)
```text
--- Case 3A: Duplicate Reservation Retry ---
[1] Client sends POST /api/v1/reservations (Idempotency-Key: idem-res-01) -> Wins res_01 (201 Created), stock decremented once (100 -> 99).
[2] Packet drops on mobile network; Client retries POST /api/v1/reservations with SAME Header (Idempotency-Key: idem-res-01).
[3] RedisLuaStockGate: GET {product:X}:idem:idem-res-01 -> HIT ("res_01") -> Returns Code 2 (IDEMPOTENT_REPLAY) in 0.1ms.
[4] ReservationService fetches res_01 and returns 200 OK {"reservation_id": "res_01", "idempotent_replay": true}.
[5] Result: Zero additional stock decremented (available stays 99). (If res_01 had already expired/released, returns 410 Gone).

--- Case 3B: 50 Concurrent Payment Retries with Same Key ---
[1] 50 threads send POST /api/v1/payments (Idempotency-Key: idem-pay-01, reservation_id: res_01) at the same millisecond.
[2] Thread 1 acquires row lock on res_01 (WHERE status='RESERVED'), sets 'PAYMENT_PENDING', inserts pay_01 (UNIQUE idempotency_key).
[3] Threads 2..50 either hit UNIQUE(idempotency_key) or see res_01.status != 'RESERVED' -> Roll back and return pay_01 receipt (200 OK).
[4] Result: Gateway charged exactly once; 1 order created.
```

---

### Trace 4: Payment Succeeds, Then `OrderService` Crashes for 30 Seconds (`Outage Recovery`)
```text
[1] t = 0.0s: OrderService pods crash (30-second outage begins).
[2] t = 1.0s: 100 customers complete POST /api/v1/payments.
      All 100 payments succeed at gateway and commit locally in PostgreSQL:
      payment='SUCCESS', inventory_reservation='SOLD', inventory.sold_quantity=100, outbox_event='PENDING' (100 rows).
      All 100 customers receive 200 OK {"payment_status": "SUCCESS", "order_status": "CONFIRMING"}.
[3] t = 1.5s: OutboxPublisherWorker polls first 5 events and calls OrderService -> 5 consecutive 503 Connection Refused errors.
[4] t = 1.6s: CircuitBreakerProxy[OrderService] trips CLOSED -> OPEN (cooldown = 10s).
      Remaining 95 events fast-fail in <0.01ms without hanging worker threads; next_retry_at updated with exponential backoff + jitter.
[5] t = 12.0s & 22.0s: Cooldown expires -> Circuit enters HALF_OPEN -> 1 probe call fails -> Circuit re-opens for another 10s.
[6] t = 30.0s: OrderService pods recover!
[7] t = 32.0s: Cooldown expires -> Circuit enters HALF_OPEN -> Probe call for Event #1 succeeds (201 Created)!
      Circuit transitions HALF_OPEN -> CLOSED.
[8] t = 32.1s - 33.5s: OutboxPublisherWorker drains all 99 remaining events.
      OrderService executes INSERT INTO orders ... ON CONFLICT (reservation_id) DO NOTHING.
      All 100 outbox_event rows marked 'PUBLISHED'; all 100 orders reach status = 'CONFIRMED'.
```

---

### Trace 5: Zero Inventory (`Sold-Out Fast Rejection`)
```text
[1] All 100 units are reserved/sold (Redis {product:X}:stock == 0; PostgreSQL available_quantity == 0).
[2] Customer #101 .. #10,000 send POST /api/v1/reservations.
[3] RedisLuaStockGate evaluates tonumber(redis.call('GET', KEYS[1])) >= 1 -> FALSE (0 < 1).
[4] Lua immediately returns {0, "SOLD_OUT"} in 0.05ms.
[5] API Gateway caches edge:sold_out:PRODUCT_X = true for 1 second.
[6] Customer receives 409 Conflict {"error_code": "SOLD_OUT", "message": "Product PRODUCT_X is out of stock"} in < 10ms p99.
[7] PostgreSQL write load from Customers #101..#10,000 = 0 queries!
```

---

### Trace 6: 50× Traffic Spike (`500,000 req/s Flash Peak`)
```text
[1] 500,000 req/s hit Cloudflare Anycast Edge at t = 0.00s.
[2] Layer 1 (CDN Edge): Serves 400,000 GET /api/v1/products read requests directly from edge cache (0 origin load).
[3] Layer 2 (Virtual Waiting Room): Receives 100,000 POST /api/v1/reservations attempts/sec; Token-Bucket admits 20,000 req/s with signed HMAC tokens and queues the rest fairly (HTTP 202 Queue Position).
[4] Layer 3 (Sharded Redis Lua Gate): The 100 units are split across 10 Redis Cluster shards ({product:PRODUCT_X:b0}:stock .. b9, 10 units each) with ring fallback probing (bucket + i) % 10.
      20,000 req/s distribute evenly (2,000 ops/s per shard). All 100 units reserve in < 5ms.
[5] Layer 4 (Edge Sold-Out Short-Circuit): At t = 5ms, all 10 buckets hit 0 -> StockDepleted event sets edge:sold_out:PRODUCT_X = true at CDN & API Gateway.
      Remaining 499,900 requests receive instant 409 SOLD_OUT at the edge with 0% DB CPU spike.
```

---

### Trace 7: Database Primary Failover & Payment Gateway Outage
```text
--- Part 7A: PostgreSQL Primary Crash Mid-Sale ---
[1] AZ-1 PostgreSQL Primary crashes at t = 3.0s.
[2] Because synchronous_commit = on to AZ-2 Synchronous Standby, 100% of committed reservations and payments are already on disk in AZ-2 (RPO = 0).
[3] Patroni promotes AZ-2 Standby to Primary in < 20s (RTO <= 30s) and updates PgBouncer routing.
[4] In-flight client requests that got a socket reset retry with the exact same Idempotency-Key and complete without duplicate reservations or charges.

--- Part 7B: External Payment Gateway Outage ---
[1] Primary Gateway (Stripe) suffers a brownout (5 consecutive 503/Timeouts).
[2] CircuitBreakerProxy[Stripe] trips CLOSED -> OPEN.
[3] For in-flight timeouts, PaymentService sets status='TIMEOUT_UNKNOWN', extends reservation TTL by +5m, and queues for PaymentReconciliationWorker.
[4] For new /api/v1/payments requests while Stripe circuit is OPEN, PaymentGatewayFactory dynamically routes charges to Secondary Gateway (RazorpayPaymentAdapter), maintaining 99.99% checkout availability!
```
