-- =============================================================================
-- SALESTORM | SYSCRAFTERS 2026 — TEAM 1
-- Phase 7: Authoritative PostgreSQL 16 Data Definition Language (DDL)
-- Covers all 16 Canonical Entities with PKs, FKs, CHECK Constraints,
-- Partial Unique Indexes, Optimistic Version Columns, and Audit Timestamps.
-- =============================================================================

BEGIN;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- -----------------------------------------------------------------------------
-- 1. CUSTOMER (VARCHAR(64) supports both UUIDs and human-readable IDs like 'cust_00101')
-- -----------------------------------------------------------------------------
CREATE TABLE customer (
    customer_id   VARCHAR(64) PRIMARY KEY DEFAULT gen_random_uuid()::text,
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    full_name     VARCHAR(150) NOT NULL,
    phone_number  VARCHAR(30),
    tier_level    VARCHAR(20) NOT NULL DEFAULT 'STANDARD'
                  CHECK (tier_level IN ('STANDARD', 'GOLD', 'PLATINUM')),
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_customer_email ON customer (email);

-- -----------------------------------------------------------------------------
-- 2. CATEGORY
-- -----------------------------------------------------------------------------
CREATE TABLE category (
    category_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_category_id UUID REFERENCES category (category_id) ON DELETE SET NULL,
    name               VARCHAR(100) NOT NULL UNIQUE,
    slug               VARCHAR(120) NOT NULL UNIQUE,
    is_active          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- 3. PRODUCT
-- -----------------------------------------------------------------------------
CREATE TABLE product (
    product_id             VARCHAR(64) PRIMARY KEY, -- e.g. 'PRODUCT_X'
    category_id            UUID NOT NULL REFERENCES category (category_id) ON DELETE RESTRICT,
    sku                    VARCHAR(64) NOT NULL UNIQUE,
    name                   VARCHAR(255) NOT NULL,
    description            TEXT,
    base_price             NUMERIC(12, 2) NOT NULL CHECK (base_price >= 0),
    is_flash_sale_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    is_active              BOOLEAN NOT NULL DEFAULT TRUE,
    version                BIGINT NOT NULL DEFAULT 1,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_product_category ON product (category_id) WHERE is_active = TRUE;
CREATE INDEX idx_product_flash_eligible ON product (is_flash_sale_eligible) WHERE is_active = TRUE;

-- -----------------------------------------------------------------------------
-- 4. INVENTORY (Authoritative Stock Ledger with Strict Conservation Constraints)
-- -----------------------------------------------------------------------------
CREATE TABLE inventory (
    product_id         VARCHAR(64) PRIMARY KEY REFERENCES product (product_id) ON DELETE CASCADE,
    warehouse_id       VARCHAR(64) NOT NULL DEFAULT 'WH_PRIMARY_01',
    total_stock        INTEGER NOT NULL CHECK (total_stock >= 0),
    available_quantity INTEGER NOT NULL CHECK (available_quantity >= 0),
    reserved_quantity  INTEGER NOT NULL DEFAULT 0 CHECK (reserved_quantity >= 0),
    sold_quantity      INTEGER NOT NULL DEFAULT 0 CHECK (sold_quantity >= 0),
    version            BIGINT NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- HARD GUARANTEE: Stock Conservation Law enforced at Storage Engine level
    CONSTRAINT chk_inventory_conservation
        CHECK (available_quantity + reserved_quantity + sold_quantity = total_stock)
) WITH (fillfactor = 70); -- Enables PostgreSQL Heap-Only Tuple (HOT) updates on hot rows

-- -----------------------------------------------------------------------------
-- 5. SALE_DEAL (Flash-Sale Campaign Configuration)
-- -----------------------------------------------------------------------------
CREATE TABLE sale_deal (
    sale_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id           VARCHAR(64) NOT NULL REFERENCES product (product_id) ON DELETE CASCADE,
    campaign_name        VARCHAR(150) NOT NULL,
    sale_price           NUMERIC(12, 2) NOT NULL CHECK (sale_price >= 0),
    max_qty_per_customer INTEGER NOT NULL DEFAULT 1 CHECK (max_qty_per_customer >= 1),
    allocated_stock      INTEGER NOT NULL CHECK (allocated_stock > 0),
    starts_at            TIMESTAMPTZ NOT NULL,
    ends_at              TIMESTAMPTZ NOT NULL,
    status               VARCHAR(20) NOT NULL DEFAULT 'SCHEDULED'
                         CHECK (status IN ('SCHEDULED', 'ACTIVE', 'ENDED', 'CANCELLED')),
    version              BIGINT NOT NULL DEFAULT 1,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_sale_window CHECK (ends_at > starts_at)
);

CREATE INDEX idx_sale_deal_active ON sale_deal (product_id, starts_at, ends_at)
    WHERE status = 'ACTIVE';

-- -----------------------------------------------------------------------------
-- 6. COUPON
-- -----------------------------------------------------------------------------
CREATE TABLE coupon (
    coupon_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code                VARCHAR(50) NOT NULL UNIQUE,
    discount_type       VARCHAR(20) NOT NULL CHECK (discount_type IN ('PERCENTAGE', 'FLAT')),
    discount_value      NUMERIC(10, 2) NOT NULL CHECK (discount_value > 0),
    min_order_amount    NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (min_order_amount >= 0),
    max_discount_amount NUMERIC(12, 2) CHECK (max_discount_amount IS NULL OR max_discount_amount > 0),
    usage_limit         INTEGER NOT NULL CHECK (usage_limit > 0),
    used_count          INTEGER NOT NULL DEFAULT 0 CHECK (used_count >= 0 AND used_count <= usage_limit),
    valid_from          TIMESTAMPTZ NOT NULL,
    valid_until         TIMESTAMPTZ NOT NULL,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    version             BIGINT NOT NULL DEFAULT 1,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- 7. CART
-- -----------------------------------------------------------------------------
CREATE TABLE cart (
    cart_id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id       VARCHAR(64) NOT NULL REFERENCES customer (customer_id) ON DELETE CASCADE,
    applied_coupon_id UUID REFERENCES coupon (coupon_id) ON DELETE SET NULL,
    status            VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
                      CHECK (status IN ('ACTIVE', 'CHECKED_OUT', 'ABANDONED')),
    subtotal_amount   NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (subtotal_amount >= 0),
    discount_amount   NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    total_amount      NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (total_amount >= 0),
    version           BIGINT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX uq_cart_active_customer ON cart (customer_id) WHERE status = 'ACTIVE';

-- -----------------------------------------------------------------------------
-- 8. CART_ITEM
-- -----------------------------------------------------------------------------
CREATE TABLE cart_item (
    cart_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cart_id      UUID NOT NULL REFERENCES cart (cart_id) ON DELETE CASCADE,
    product_id   VARCHAR(64) NOT NULL REFERENCES product (product_id) ON DELETE RESTRICT,
    quantity     INTEGER NOT NULL CHECK (quantity >= 1),
    unit_price   NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_cart_product UNIQUE (cart_id, product_id)
);

-- -----------------------------------------------------------------------------
-- 9. INVENTORY_RESERVATION (Temporary Hold with TTL & Idempotency Enforcement)
-- -----------------------------------------------------------------------------
CREATE TABLE inventory_reservation (
    reservation_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    product_id        VARCHAR(64) NOT NULL REFERENCES product (product_id) ON DELETE RESTRICT,
    customer_id       VARCHAR(64) NOT NULL REFERENCES customer (customer_id) ON DELETE RESTRICT,
    sale_id           UUID REFERENCES sale_deal (sale_id) ON DELETE SET NULL,
    quantity          INTEGER NOT NULL DEFAULT 1 CHECK (quantity >= 1),
    locked_unit_price NUMERIC(12, 2) NOT NULL CHECK (locked_unit_price >= 0),
    status            VARCHAR(25) NOT NULL DEFAULT 'RESERVED'
                      CHECK (status IN (
                          'RESERVED',
                          'PAYMENT_PENDING',
                          'CONFIRMED',
                          'SOLD',
                          'PAYMENT_FAILED',
                          'TIMEOUT',
                          'RELEASED'
                      )),
    release_reason    VARCHAR(25) CHECK (release_reason IS NULL OR release_reason IN ('PAYMENT_FAILED', 'TIMEOUT')),
    idempotency_key   VARCHAR(128) NOT NULL UNIQUE,
    expires_at        TIMESTAMPTZ NOT NULL,
    version           BIGINT NOT NULL DEFAULT 1,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Partial index for fast multi-worker TTL ExpirySweeperWorker queries (RESERVED only)
CREATE INDEX idx_res_expiry_sweep
    ON inventory_reservation (expires_at ASC)
    WHERE status = 'RESERVED';

CREATE INDEX idx_res_customer_product
    ON inventory_reservation (customer_id, product_id);

-- -----------------------------------------------------------------------------
-- 10. PAYMENT (Idempotent Financial Ledger)
-- -----------------------------------------------------------------------------
CREATE TABLE payment (
    payment_id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id        UUID NOT NULL REFERENCES inventory_reservation (reservation_id) ON DELETE RESTRICT,
    customer_id           VARCHAR(64) NOT NULL REFERENCES customer (customer_id) ON DELETE RESTRICT,
    amount                NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    currency              VARCHAR(3) NOT NULL DEFAULT 'INR',
    payment_method        VARCHAR(30) NOT NULL DEFAULT 'CARD_TOKEN',
    payment_provider      VARCHAR(30) NOT NULL DEFAULT 'STRIPE',
    status                VARCHAR(25) NOT NULL DEFAULT 'INITIATED'
                          CHECK (status IN (
                              'INITIATED',
                              'PENDING',
                              'SUCCESS',
                              'FAILED',
                              'TIMEOUT_UNKNOWN',
                              'RECONCILED_SUCCESS',
                              'RECONCILED_FAILED',
                              'REFUNDED'
                          )),
    idempotency_key       VARCHAR(128) NOT NULL UNIQUE,
    transaction_reference VARCHAR(128) NOT NULL UNIQUE,
    provider_tx_id        VARCHAR(128),
    failure_reason        VARCHAR(255),
    version               BIGINT NOT NULL DEFAULT 1,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- HARD GUARANTEE: Prevents double active/successful payment for the same reservation_id
CREATE UNIQUE INDEX uq_payment_active_reservation
    ON payment (reservation_id)
    WHERE status IN ('PENDING', 'SUCCESS', 'RECONCILED_SUCCESS');

-- Partial index for PaymentReconciliationWorker
CREATE INDEX idx_payment_timeout_reconcile
    ON payment (created_at ASC)
    WHERE status = 'TIMEOUT_UNKNOWN';

-- -----------------------------------------------------------------------------
-- 11. ORDERS (Durable Order Header with Inbox Deduplication on reservation_id)
-- -----------------------------------------------------------------------------
CREATE TABLE orders (
    order_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id        UUID NOT NULL UNIQUE REFERENCES inventory_reservation (reservation_id) ON DELETE RESTRICT,
    payment_id            UUID NOT NULL UNIQUE REFERENCES payment (payment_id) ON DELETE RESTRICT,
    customer_id           VARCHAR(64) NOT NULL REFERENCES customer (customer_id) ON DELETE RESTRICT,
    coupon_id             UUID REFERENCES coupon (coupon_id) ON DELETE SET NULL,
    status                VARCHAR(25) NOT NULL DEFAULT 'CREATED'
                          CHECK (status IN (
                              'CREATED',
                              'PAYMENT_PENDING',
                              'CONFIRMED',
                              'PROCESSING',
                              'SHIPPED',
                              'OUT_FOR_DELIVERY',
                              'DELIVERED',
                              'CANCELLED',
                              'REFUND_PENDING',
                              'REFUNDED'
                          )),
    subtotal_amount       NUMERIC(12, 2) NOT NULL CHECK (subtotal_amount >= 0),
    discount_amount       NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (discount_amount >= 0),
    tax_amount            NUMERIC(12, 2) NOT NULL DEFAULT 0 CHECK (tax_amount >= 0),
    total_amount          NUMERIC(12, 2) NOT NULL CHECK (total_amount >= 0),
    shipping_address_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    version               BIGINT NOT NULL DEFAULT 1,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_orders_customer_created ON orders (customer_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders (status);

-- -----------------------------------------------------------------------------
-- 12. ORDER_ITEM
-- -----------------------------------------------------------------------------
CREATE TABLE order_item (
    order_item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id      UUID NOT NULL REFERENCES orders (order_id) ON DELETE CASCADE,
    product_id    VARCHAR(64) NOT NULL REFERENCES product (product_id) ON DELETE RESTRICT,
    quantity      INTEGER NOT NULL CHECK (quantity >= 1),
    unit_price    NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    line_total    NUMERIC(12, 2) NOT NULL CHECK (line_total >= 0),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_order_item_order ON order_item (order_id);

-- -----------------------------------------------------------------------------
-- 13. SHIPMENT
-- -----------------------------------------------------------------------------
CREATE TABLE shipment (
    shipment_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id           UUID NOT NULL UNIQUE REFERENCES orders (order_id) ON DELETE RESTRICT,
    carrier_name       VARCHAR(50) NOT NULL,
    tracking_number    VARCHAR(100) NOT NULL UNIQUE,
    status             VARCHAR(25) NOT NULL DEFAULT 'PENDING'
                       CHECK (status IN (
                           'PENDING',
                           'BOOKED',
                           'IN_TRANSIT',
                           'OUT_FOR_DELIVERY',
                           'DELIVERED',
                           'RETURNED'
                       )),
    estimated_delivery TIMESTAMPTZ,
    shipped_at         TIMESTAMPTZ,
    delivered_at       TIMESTAMPTZ,
    version            BIGINT NOT NULL DEFAULT 1,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- 14. NOTIFICATION (Idempotent Customer Notification Log)
-- -----------------------------------------------------------------------------
CREATE TABLE notification (
    notification_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    customer_id       VARCHAR(64) NOT NULL REFERENCES customer (customer_id) ON DELETE CASCADE,
    order_id          UUID REFERENCES orders (order_id) ON DELETE SET NULL,
    event_id          UUID NOT NULL,
    channel           VARCHAR(15) NOT NULL CHECK (channel IN ('EMAIL', 'SMS', 'PUSH')),
    event_type        VARCHAR(50) NOT NULL,
    recipient_address VARCHAR(255) NOT NULL,
    message_body      TEXT NOT NULL,
    status            VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                      CHECK (status IN ('PENDING', 'SENT', 'FAILED')),
    sent_at           TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Prevents duplicate notifications when Kafka events are replayed
    CONSTRAINT uq_notification_event_channel UNIQUE (event_id, channel)
);

-- -----------------------------------------------------------------------------
-- 15. OUTBOX_EVENT (Transactional Outbox Pattern for Reliable Messaging)
-- -----------------------------------------------------------------------------
CREATE TABLE outbox_event (
    event_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type VARCHAR(50) NOT NULL, -- 'ORDER', 'PAYMENT', 'INVENTORY'
    aggregate_id   VARCHAR(128) NOT NULL,
    event_type     VARCHAR(64) NOT NULL, -- 'OrderRequested', 'OrderConfirmed', 'PaymentFailed'
    payload        JSONB NOT NULL,
    status         VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                   CHECK (status IN ('PENDING', 'PUBLISHED', 'DEAD_LETTER')),
    retry_count    INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at  TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_error     TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at   TIMESTAMPTZ
);

-- Partial index for OutboxPublisherWorker polling with FOR UPDATE SKIP LOCKED
CREATE INDEX idx_outbox_pending_poll
    ON outbox_event (next_retry_at ASC, created_at ASC)
    WHERE status = 'PENDING';

-- -----------------------------------------------------------------------------
-- 16. AUDIT_LOG (Immutable Compliance & State Change Trail)
-- -----------------------------------------------------------------------------
CREATE TABLE audit_log (
    audit_id       BIGSERIAL PRIMARY KEY,
    entity_type    VARCHAR(50) NOT NULL,
    entity_id      VARCHAR(128) NOT NULL,
    action_type    VARCHAR(50) NOT NULL,
    actor_id       VARCHAR(128) NOT NULL,
    previous_state JSONB,
    new_state      JSONB,
    trace_id       VARCHAR(64),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_entity ON audit_log (entity_type, entity_id, created_at DESC);
CREATE INDEX idx_audit_trace ON audit_log (trace_id);

COMMIT;
