# Phase 7A: Database Architecture & Entity-Relationship Design
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  

---

## 1. Database Design Philosophy: Invariants Enforced at the Storage Engine

In SALESTORM, application code is **never the sole guardian of data correctness**. Even if an application pod had a bug, the PostgreSQL 16 database schema guarantees that **overselling, negative stock, duplicate reservations, duplicate charges, and duplicate orders are physically impossible to commit** via strict database-level constraints:

1. **Stock Conservation Invariant (`inventory` table):**
   * `CHECK (available_quantity >= 0)`
   * `CHECK (reserved_quantity >= 0)`
   * `CHECK (sold_quantity >= 0)`
   * `CHECK (available_quantity + reserved_quantity + sold_quantity = total_stock)`
2. **Idempotency & Deduplication Constraints:**
   * `inventory_reservation`: `UNIQUE (idempotency_key)`
   * `payment`: `UNIQUE (idempotency_key)`, `UNIQUE (transaction_reference)`, and partial unique index `UNIQUE (reservation_id) WHERE status IN ('PENDING', 'SUCCESS', 'RECONCILED_SUCCESS')`
   * `orders`: `UNIQUE (reservation_id)` and `UNIQUE (payment_id)`
   * `notification`: `UNIQUE (event_id, channel)` (idempotent notification delivery)

---

## 2. Complete Entity-Relationship (ER) Diagram (`Mermaid`)

Covers all **16 canonical tables** from Phase 1 across Customer, Catalog, Promotions, Cart, Inventory, Reservation, Payment, Order, Fulfilment/Shipment, Notification, Outbox, and Audit domains. Every key modifier uses single-token `PK` / `FK` syntax so it renders cleanly across all Mermaid versions.

```mermaid
erDiagram
    CUSTOMER ||--o{ CART : "owns"
    CUSTOMER ||--o{ INVENTORY_RESERVATION : "places"
    CUSTOMER ||--o{ PAYMENT : "initiates"
    CUSTOMER ||--o{ ORDERS : "places"
    CUSTOMER ||--o{ NOTIFICATION : "receives"

    CATEGORY ||--o{ PRODUCT : "categorizes"
    PRODUCT ||--|| INVENTORY : "has_stock_ledger"
    PRODUCT ||--o{ SALE_DEAL : "featured_in"
    PRODUCT ||--o{ CART_ITEM : "added_to"
    PRODUCT ||--o{ INVENTORY_RESERVATION : "reserved_in"
    PRODUCT ||--o{ ORDER_ITEM : "purchased_in"

    CART ||--o{ CART_ITEM : "contains"
    COUPON ||--o{ CART : "applied_to"
    COUPON ||--o{ ORDERS : "redeemed_in"
    SALE_DEAL ||--o{ INVENTORY_RESERVATION : "applies_deal_price"

    INVENTORY_RESERVATION ||--o{ PAYMENT : "settled_by"
    INVENTORY_RESERVATION ||--o| ORDERS : "converts_to"
    PAYMENT ||--o| ORDERS : "funds"

    ORDERS ||--|{ ORDER_ITEM : "contains"
    ORDERS ||--o| SHIPMENT : "fulfilled_via"
    ORDERS ||--o{ NOTIFICATION : "triggers"

    CUSTOMER {
        varchar customer_id PK "e.g., cust_00101 or UUID"
        varchar email "UNIQUE"
        varchar full_name
        varchar phone_number
        varchar tier_level
        boolean is_active
        timestamptz created_at
        timestamptz updated_at
    }

    CATEGORY {
        uuid category_id PK
        uuid parent_category_id FK
        varchar name "UNIQUE"
        varchar slug "UNIQUE"
        timestamptz created_at
    }

    PRODUCT {
        varchar product_id PK "e.g., PRODUCT_X"
        uuid category_id FK
        varchar sku "UNIQUE"
        varchar name
        text description
        numeric base_price
        boolean is_flash_sale_eligible
        boolean is_active
        timestamptz created_at
        timestamptz updated_at
    }

    SALE_DEAL {
        uuid sale_id PK
        varchar product_id FK
        varchar campaign_name
        numeric sale_price
        integer max_qty_per_customer
        integer allocated_stock
        timestamptz starts_at
        timestamptz ends_at
        varchar status
        timestamptz created_at
    }

    COUPON {
        uuid coupon_id PK
        varchar code "UNIQUE"
        varchar discount_type "PERCENTAGE | FLAT"
        numeric discount_value
        numeric min_order_amount
        numeric max_discount_amount
        integer usage_limit
        integer used_count
        timestamptz valid_from
        timestamptz valid_until
        boolean is_active
    }

    CART {
        uuid cart_id PK
        varchar customer_id FK
        uuid applied_coupon_id FK
        varchar status "ACTIVE | CHECKED_OUT | ABANDONED"
        numeric subtotal_amount
        numeric discount_amount
        numeric total_amount
        integer version
        timestamptz created_at
        timestamptz updated_at
    }

    CART_ITEM {
        uuid cart_item_id PK
        uuid cart_id FK
        varchar product_id FK
        integer quantity
        numeric unit_price
        timestamptz created_at
        timestamptz updated_at
    }

    INVENTORY {
        varchar product_id PK "PK, FK to product"
        varchar warehouse_id "Default WH_PRIMARY_01"
        integer total_stock "100"
        integer available_quantity "CHECK >= 0"
        integer reserved_quantity "CHECK >= 0"
        integer sold_quantity "CHECK >= 0"
        bigint version "Optimistic/Audit Counter"
        timestamptz created_at
        timestamptz updated_at
    }

    INVENTORY_RESERVATION {
        uuid reservation_id PK
        varchar product_id FK
        varchar customer_id FK
        uuid sale_id FK
        integer quantity "CHECK >= 1"
        numeric locked_unit_price
        varchar status "RESERVED|PAYMENT_PENDING|CONFIRMED|SOLD|PAYMENT_FAILED|TIMEOUT|RELEASED"
        varchar release_reason "PAYMENT_FAILED | TIMEOUT | NULL"
        varchar idempotency_key "UNIQUE"
        timestamptz expires_at
        integer version
        timestamptz created_at
        timestamptz updated_at
    }

    PAYMENT {
        uuid payment_id PK
        uuid reservation_id FK
        varchar customer_id FK
        numeric amount "CHECK > 0"
        varchar currency "INR | USD"
        varchar payment_method "CARD_TOKEN | UPI"
        varchar payment_provider "STRIPE | RAZORPAY"
        varchar status "INITIATED|PENDING|SUCCESS|FAILED|TIMEOUT_UNKNOWN|RECONCILED_SUCCESS|RECONCILED_FAILED|REFUNDED"
        varchar idempotency_key "UNIQUE"
        varchar transaction_reference "UNIQUE"
        varchar provider_tx_id
        varchar failure_reason
        integer version
        timestamptz created_at
        timestamptz updated_at
    }

    ORDERS {
        uuid order_id PK
        uuid reservation_id FK "UNIQUE, FK"
        uuid payment_id FK "UNIQUE, FK"
        varchar customer_id FK
        uuid coupon_id FK
        varchar status "CREATED|PAYMENT_PENDING|CONFIRMED|PROCESSING|SHIPPED|OUT_FOR_DELIVERY|DELIVERED|CANCELLED|REFUND_PENDING|REFUNDED"
        numeric subtotal_amount
        numeric discount_amount
        numeric tax_amount
        numeric total_amount
        text shipping_address_json
        integer version
        timestamptz created_at
        timestamptz updated_at
    }

    ORDER_ITEM {
        uuid order_item_id PK
        uuid order_id FK
        varchar product_id FK
        integer quantity
        numeric unit_price
        numeric line_total
        timestamptz created_at
    }

    SHIPMENT {
        uuid shipment_id PK
        uuid order_id FK "UNIQUE, FK"
        varchar carrier_name "DELHIVERY | FEDEX"
        varchar tracking_number "UNIQUE"
        varchar status "PENDING|BOOKED|IN_TRANSIT|OUT_FOR_DELIVERY|DELIVERED|RETURNED"
        timestamptz estimated_delivery
        timestamptz shipped_at
        timestamptz delivered_at
        timestamptz created_at
        timestamptz updated_at
    }

    NOTIFICATION {
        uuid notification_id PK
        varchar customer_id FK
        uuid order_id FK
        uuid event_id "Idempotency Reference"
        varchar channel "EMAIL | SMS | PUSH"
        varchar event_type
        varchar recipient_address
        text message_body
        varchar status "PENDING | SENT | FAILED"
        timestamptz sent_at
        timestamptz created_at
    }

    OUTBOX_EVENT {
        uuid event_id PK
        varchar aggregate_type "ORDER | PAYMENT | INVENTORY"
        varchar aggregate_id
        varchar event_type "OrderRequested | OrderConfirmed | PaymentFailed"
        jsonb payload
        varchar status "PENDING | PUBLISHED | DEAD_LETTER"
        integer retry_count
        timestamptz next_retry_at
        varchar last_error
        timestamptz created_at
        timestamptz processed_at
    }

    AUDIT_LOG {
        bigserial audit_id PK
        varchar entity_type
        varchar entity_id
        varchar action_type
        varchar actor_id
        jsonb previous_state
        jsonb new_state
        varchar trace_id
        timestamptz created_at
    }
```

### 2.1 Reference ER Layout Mapping Note
For judges comparing against the reference ER sheet (`SALESTORM - ER Diagram (Reference Layout).pdf`):
* `FlashSale` maps 1-to-1 to our **`SALE_DEAL`** table (`sale_id`, `product_id`, `starts_at`, `ends_at`, `sale_price`, `max_qty_per_customer`).
* `Warehouse` is represented by `inventory.warehouse_id` (and managed by `FulfilmentService`).
* `PaymentGateway` and `ShippingProvider` are modeled via pluggable strategy adapters (`IPaymentGatewayAdapter`, `IShippingPartnerAdapter`) with provider metadata persisted in `payment.payment_provider` and `shipment.carrier_name`.

---

## 3. Indexing & Hot-Row Optimization Strategy

| Table | Index Name & Columns | Index Type | Query Optimized |
| :--- | :--- | :--- | :--- |
| `inventory_reservation` | `idx_res_expiry_sweep (expires_at) WHERE status = 'RESERVED'` | **Partial B-Tree Index** | Allows `ExpirySweeperWorker` to find expired `RESERVED` holds in $O(\log K)$ without scanning millions of historical `SOLD` or `RELEASED` rows! |
| `payment` | `uq_payment_active_reservation (reservation_id) WHERE status IN ('PENDING', 'SUCCESS', 'RECONCILED_SUCCESS')` | **Partial Unique Index** | Guarantees a reservation can never have two active/successful payments simultaneously. |
| `payment` | `idx_payment_reconcile (created_at) WHERE status = 'TIMEOUT_UNKNOWN'` | **Partial B-Tree Index** | Allows `PaymentReconciliationWorker` to immediately locate timed-out gateway transactions needing status verification. |
| `outbox_event` | `idx_outbox_pending_poll (next_retry_at, created_at) WHERE status = 'PENDING'` | **Partial B-Tree Index** | Keeps `OutboxPublisherWorker` polling query (`FOR UPDATE SKIP LOCKED`) sub-millisecond even after millions of events are marked `PUBLISHED`. |
| `inventory` | `FILLFACTOR = 70` storage parameter on table | **PostgreSQL HOT (Heap-Only Tuple) Updates** | Leaves 30% free space on each 8KB data page so high-frequency `UPDATE inventory` statements write updated tuples on the **same page** without index bloat or page splits. |

---

## 4. Architectural Decisions for Phase 7 Database Design (Working Rule #3)

### Decision 7.1: Explicit Four-Column Stock Ledger (`total_stock`, `available_quantity`, `reserved_quantity`, `sold_quantity`) vs. `SUM()` Aggregation
* **Choice:** Maintain four explicit integer columns on `inventory` enforced by `CHECK (available_quantity + reserved_quantity + sold_quantity = total_stock)` and `CHECK (available_quantity >= 0)`.
* **Alternatives Rejected:**
  1. *Storing only `total_stock` and calculating available stock dynamically via `SELECT total_stock - COALESCE(SUM(quantity), 0) FROM inventory_reservation WHERE status IN ('RESERVED', 'SOLD')`.*
* **Why it fits SALESTORM:** Dynamic `SUM()` queries require scanning and locking multiple rows in `inventory_reservation` and cannot prevent concurrent inserts from exceeding 100 without `SERIALIZABLE` table locks. Our four-column ledger turns stock conservation into an $O(1)$ single-row conditional update verified instantaneously by the database engine.
* **What it sacrifices:** Requires every reservation state transition (`RESERVED` $\rightarrow$ `SOLD` or `RELEASED`) to update both `inventory_reservation` and `inventory` inside the same local ACID transaction.
