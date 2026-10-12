# Phase 9: Architecture Decision Records (ADRs)
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  

---

## ADR Index

| ADR ID | Title | Status | Core Domain Impacted |
| :--- | :--- | :---: | :--- |
| **[ADR-001](#adr-001-postgresql-relational-aciddb-vs-nosql-for-core-financial--inventory-ledger)** | **SQL (PostgreSQL 16) vs. NoSQL (DynamoDB / MongoDB) for Core Ledger** | Accepted | `inventory`, `inventory_reservation`, `payment`, `orders`, `outbox_event` |
| **[ADR-002](#adr-002-synchronous-reservation--payment-with-asynchronous-order-creation)** | **Sync Reservation/Payment vs. Async Order Creation Pipeline** | Accepted | `ReservationService`, `PaymentService`, `OrderService` |
| **[ADR-003](#adr-003-two-layer-hybrid-concurrency-control-redis-atomic-lua--postgresql-conditional-update)** | **Hybrid Redis Atomic Lua Gate + PostgreSQL Conditional Update vs. Optimistic / Pessimistic Locking** | Accepted | `RedisLuaStockGate`, `PostgresInventoryRepository` |
| **[ADR-004](#adr-004-multi-tier-cache-strategy-cache-aside-catalog--write-through-lua-gate--edge-sold-out-flag)** | **Multi-Tier Cache Strategy (Cache-Aside + Atomic Lua Gate + Edge Sold-Out Short-Circuit)** | Accepted | CDN, API Gateway, Redis Cluster 7, `StockReconciliationWorker` |
| **[ADR-005](#adr-005-transactional-outbox--saga-pattern-vs-distributed-two-phase-commit-2pcxa)** | **Transactional Outbox + Saga Pattern vs. Distributed Two-Phase Commit (2PC/XA)** | Accepted | `PaymentSagaOrchestrator`, `OutboxPublisherWorker`, `OrderService` |
| **[ADR-006](#adr-006-apache-kafka-log-stream-vs-rabbitmq-traditional-amqp-broker)** | **Apache Kafka vs. RabbitMQ for Domain Event Streaming** | Accepted | `order.events`, `payment.events`, `inventory.events`, `order.dlq` |
| **[ADR-007](#adr-007-token-bucket-virtual-waiting-room-at-the-edge-for-50x-traffic-spikes)** | **Edge Token-Bucket Virtual Waiting Room vs. Direct Backend Auto-Scaling at 500k RPS** | Accepted | Cloudflare Edge / Virtual Waiting Room, API Gateway |

---

## ADR-001: PostgreSQL (Relational ACID DB) vs. NoSQL for Core Financial & Inventory Ledger

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  SALESTORM must process 10,000+ concurrent purchase attempts for 100 units while guaranteeing **zero overselling** (`available_quantity >= 0`), **zero duplicate charges**, and **atomic multi-table settlement** (updating `payment`, `inventory_reservation`, `inventory`, and inserting `outbox_event` in a single atomic commit).
* **Decision (The Choice):**
  Use **PostgreSQL 16** (with synchronous standby replication for `RPO = 0` and hash partitioning by `product_id`) as the authoritative system of record for all 16 core tables (`inventory`, `inventory_reservation`, `payment`, `orders`, `outbox_event`, etc.).
* **Alternatives Rejected:**
  1. **MongoDB (Document NoSQL):** While MongoDB 4.0+ supports multi-document transactions, they carry higher performance overhead under high write contention and lack native declarative `CHECK (available_quantity + reserved_quantity + sold_quantity = total_stock)` constraints and `FOR UPDATE SKIP LOCKED` partial-index semantics.
  2. **Amazon DynamoDB / Cassandra (Wide-Column / Key-Value NoSQL):** DynamoDB `TransactWriteItems` is limited to 100 items, costs $2\times$ WCUs, throttles single hot partitions (`1,000 WCU/sec` per partition limit on a single `PRODUCT_X` key), and lacks `FOR UPDATE SKIP LOCKED` for multi-worker outbox/expiry sweeps.
* **Why It Fits SALESTORM:**
  1. Native engine-level `CHECK` constraints make negative inventory (`available_quantity < 0`) physically impossible to commit.
  2. Single-statement conditional updates (`UPDATE inventory ... WHERE available_quantity >= 1`) execute in sub-millisecond time under `READ COMMITTED` isolation without multi-round-trip locks.
  3. Supports atomic 4-table transactions (`Tx_Pay_Settle`: `payment` + `inventory_reservation` + `inventory` + `outbox_event`) and `FOR UPDATE SKIP LOCKED` for lock-free worker queues.
* **Consequences (What It Sacrifices):**
  * Scaling a single unpartitioned PostgreSQL instance beyond ~20,000 write TPS on a single row hits lock contention—which we mitigate by placing the **Redis Atomic Lua Gate** in front of PostgreSQL so only the 100 reservation winners ever write to PostgreSQL.

---

## ADR-002: Synchronous Reservation & Payment with Asynchronous Order Creation

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  During a flash sale, customers expect immediate confirmation of whether they secured one of the 100 units (`201 Created` vs `409 SOLD_OUT`) and whether their payment succeeded (`200 OK` vs `402 Declined`). However, the hackathon brief mandates that **`OrderService` can go down for 30 seconds mid-sale**, and no paid purchase may be lost.
* **Decision (The Choice):**
  Use a **Hybrid Sync/Async Pipeline**:
  * **Synchronous:** `POST /api/v1/reservations` (stock hold) and `POST /api/v1/payments` (gateway charge + local ACID Outbox write).
  * **Asynchronous:** Order creation (`OrderService`), warehouse allocation (`FulfilmentService`), carrier booking (`ShipmentService`), and customer alerts (`NotificationService`) driven via `OutboxPublisherWorker` and Apache Kafka.
* **Alternatives Rejected:**
  1. **Fully Synchronous End-to-End Checkout (`Reserve -> Pay -> CreateOrder -> BookShipment -> SendEmail` in one HTTP call):** Coupling `/pay` synchronously to `OrderService` means a 30-second `OrderService` outage fails HTTP `/pay` requests *after* the customer's credit card was already charged!
  2. **Fully Asynchronous Reservation (`POST /reserve` returns `202 Accepted` and queues all 10,000 clicks into Kafka):** Forces 10,000 users to poll a status endpoint repeatedly (multiplying read traffic by $10\times$) just to discover `SOLD_OUT`.
* **Why It Fits SALESTORM:**
  Customers get instant, deterministic answers on stock reservation (`< 80 ms`) and payment capture, while everything downstream of payment capture is 100% insulated from service outages via the Transactional Outbox.
* **Consequences (What It Sacrifices):**
  * Immediately after `POST /payments` returns `200 OK`, the response shows `"order_status": "CONFIRMING"` for a few milliseconds (or up to ~35 seconds if `OrderService` is experiencing a 30s outage) until the async order record is finalized.

---

## ADR-003: Two-Layer Hybrid Concurrency Control (Redis Atomic Lua + PostgreSQL Conditional Update)

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  10,000 concurrent requests (scaling to 500,000 req/s at 50× flash peak) compete for `100` units of `PRODUCT_X`. We must prevent overselling while maintaining low p99 latency.
* **Decision (The Choice):**
  Combine **Layer 1: Redis Single-Threaded Atomic Lua Script (`RedisLuaStockGate`)** to check idempotency and decrement in-memory stock (`> 0`), with **Layer 2: PostgreSQL Conditional Atomic Update (`WHERE available_quantity >= 1`)** executed **only by the 100 Redis winners**.
* **Alternatives Rejected:**
  1. **Application Optimistic Locking (`SELECT version` $\rightarrow$ `UPDATE ... WHERE version = v`):** Causes a 99.9% abort-and-retry storm when 10,000 threads read the same `version` number simultaneously.
  2. **Database Pessimistic Locking (`SELECT ... FOR UPDATE` on all 10,000 requests):** Queues 10,000 transactions on a single row lock, exhausting connection pools and inflating tail latency to seconds.
  3. **Redis-Only Stock Management (No DB Conditional Check):** Risks overselling if a Redis master node crashes before replicating recent decrements to its replica.
* **Why It Fits SALESTORM:**
  Redis sheds 99% of excess traffic (`9,900 SOLD_OUT` responses) in `< 1 ms` in RAM. PostgreSQL processes only the ~100 winning writes and enforces the final `CHECK (available_quantity >= 0)` guarantee on disk.
* **Consequences (What It Sacrifices):**
  * Requires compensating Redis `INCRBY` (guarded by `SETNX release:{reservation_id}`) whenever a PostgreSQL transaction rolls back, a payment fails (5%), or a reservation expires (`ExpirySweeperWorker`), plus a periodic `StockReconciliationWorker`.

---

## ADR-004: Multi-Tier Cache Strategy (Cache-Aside Catalog + Write-Through Lua Gate + Edge Sold-Out Flag)

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  A single caching pattern cannot serve both static catalog metadata (read 500,000 times/sec) and rapidly mutating flash-sale stock (100 units depleted in milliseconds).
* **Decision (The Choice):**
  Adopt a **Three-Tier Domain-Specific Cache Strategy**:
  1. **Catalog & Deals (`product`, `sale_deal`):** Cache-Aside in Redis (`TTL = 300s`) + Cloudflare CDN (`TTL = 60s`) with event-driven invalidation on admin updates.
  2. **Inventory Counter (`{product:PRODUCT_X}:stock`):** Pre-warmed Write-Through Atomic Lua Gate in Redis pinned with `noeviction`.
  3. **Edge Sold-Out Short-Circuit (`edge:sold_out:PRODUCT_X`):** 1-second local in-memory boolean flag broadcast to API Gateway / CDN Edge once Redis stock hits `0`.
* **Alternatives Rejected:**
  1. **Write-Behind (Write-Back) Caching for Inventory:** Asynchronously flushing stock decrements from Redis to PostgreSQL in batches violates our **Zero Overselling** and **`RPO = 0`** hard guarantees if a Redis node crashes before flushing.
  2. **Short-TTL Cache-Aside for Inventory Availability:** Even a 100ms stale cache window allows 5,000 requests to bypass the gate thinking stock is available.
* **Why It Fits SALESTORM:**
  Protects PostgreSQL from both read floods and sold-out write floods while keeping the reservation ledger 100% strongly consistent.
* **Consequences (What It Sacrifices):**
  * If a reservation expires 10 minutes into the sale and restores 1 unit of stock, the 1-second `edge:sold_out:PRODUCT_X` flag takes up to `1,000 ms` to clear at the edge so new buyers can claim the released unit.

---

## ADR-005: Transactional Outbox + Saga Pattern vs. Distributed Two-Phase Commit (2PC/XA)

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  Completing a purchase requires updating `PaymentService` (`payment`), `InventoryService` (`inventory`, `inventory_reservation`), and `OrderService` (`orders`), plus calling an external Payment Gateway that does not support XA/2PC transactions.
* **Decision (The Choice):**
  Use the **Saga Pattern (Orchestrated Payment + Choreographed Order/Fulfilment)** coupled with the **Transactional Outbox Pattern** (`outbox_event` table committed in the same local ACID transaction as `payment` and `inventory`).
* **Alternatives Rejected:**
  1. **Distributed Two-Phase Commit (2PC / XA Transactions):** Requires all participating services (`PaymentService`, `InventoryService`, `OrderService`) to be online simultaneously to commit. If `OrderService` goes down for 30 seconds (mandatory hackathon test), 2PC blocks 100% of payments and holds database locks across nodes.
  2. **Best-Effort Dual Write (`db.commit()` then `kafka.publish()`):** If the pod crashes or loses network connectivity 1 millisecond after `db.commit()`, the Kafka event is lost forever and the customer is charged with no order created.
* **Why It Fits SALESTORM:**
  Guarantees atomic durability of `payment = 'SUCCESS'` and `outbox_event = 'OrderRequested'` inside a single local transaction. When `OrderService` goes down for 30 seconds, `OutboxPublisherWorker` trips its Circuit Breaker `OPEN`, waits safely with exponential backoff, and drains the backlog automatically upon recovery.
* **Consequences (What It Sacrifices):**
  * Requires `OutboxPublisherWorker` infrastructure, an `outbox_event` table, idempotent consumers (`UNIQUE(reservation_id)` in `orders`), and a compensating refund workflow (`DeadLetterHandler`) if an order permanently fails after 10 retries.

---

## ADR-006: Apache Kafka (Log Stream) vs. RabbitMQ (Traditional AMQP Broker)

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  Domain events (`OrderRequested`, `OrderConfirmed`, `PaymentFailed`, `ReservationReleased`) are consumed by multiple independent downstream services (`OrderService`, `FulfilmentService`, `ShipmentService`, `NotificationService`, `AuditLogSubscriber`, and analytics).
* **Decision (The Choice):**
  Use **Apache Kafka** (configured with `replication.factor = 3`, `min.insync.replicas = 2`, `acks = all`, partitioned by `aggregate_id`) as the primary production message broker.
* **Alternatives Rejected:**
  1. **RabbitMQ (AMQP Queue Broker):** Deletes messages as soon as a consumer acknowledges them (preventing historical replay for audit/reconciliation), requires complex fanout exchange bindings for multi-service consumption, and degrades in performance when millions of messages accumulate in disk queues during downstream outages.
  2. **Redis Pub/Sub:** Fire-and-forget in-memory broadcast; if `OrderService` is down for 30 seconds, all messages published during those 30 seconds are permanently lost!
* **Why It Fits SALESTORM:**
  1. **Partition-Ordered Delivery:** Partitioning `order.events` by `reservation_id` / `order_id` guarantees that state events for the same order are processed strictly in order.
  2. **Multi-Consumer Fanout & Replayability:** `OrderService`, `NotificationService`, and `AuditLogSubscriber` read from the same log using independent Consumer Groups at their own pace, and offsets can be rewound to replay events after an incident.
* **Consequences (What It Sacrifices):**
  * Higher operational footprint than Redis Pub/Sub or a single RabbitMQ node, and per-message retry backoff requires either an Outbox table scheduler (`next_retry_at`) or dedicated retry/DLQ topics (`order.dlq`).

---

## ADR-007: Token-Bucket Virtual Waiting Room at the Edge for 50× Traffic Spikes

* **Date:** 2026-10-12
* **Status:** Accepted
* **Context:**
  While normal peak traffic is `10,000 req/s`, a viral flash sale can spike $50\times$ to `500,000 req/s`. Provisioning backend Kubernetes clusters and load balancers to keep 500,000 concurrent TLS connections open year-round is prohibitively expensive when only 100 items are being sold.
* **Decision (The Choice):**
  Deploy an **Edge Virtual Waiting Room (VWR)** on Cloudflare Workers / Edge KV using a **Token-Bucket Admission Controller** that admits at most `10,000–20,000 req/s` (via signed short-lived HMAC admission cookies) into the origin API Gateway.
* **Alternatives Rejected:**
  1. **Hard HTTP 429 Drops at the Load Balancer without a Waiting Room:** Drops legitimate early customers randomly during TCP congestion and triggers aggressive client retry loops that worsen the DDoS effect.
  2. **Over-Provisioning Origin Clusters by 50× for a 10-Second Sale:** Wastes 98% of cloud compute budget for an event that sells out in under 1 second.
* **Why It Fits SALESTORM:**
  Absorbs the 500,000 req/s spike across hundreds of global Anycast edge PoPs, stops bot floods before they enter the cloud VPC, and guarantees predictable, bounded load on `ReservationService` and Redis.
* **Consequences (What It Sacrifices):**
  * Users arriving after the first 10,000 admission tokens see a waiting-room screen for 1–2 seconds before receiving either an admission slot or the edge-cached `SOLD_OUT` notification.
