# Phase 10A: Empirical Validation Report
**Project:** SALESTORM — High-Concurrency Flash-Sale E-Commerce Platform  
**Event:** SYSCRAFTERS 2026 (Design-First System Design Hackathon)  
**Team:** Team 1  
**Simulation Script:** [`simulate_flash_sale.py`](./simulate_flash_sale.py)  

---

## 1. Purpose of the Validation Simulation

Following the **Design-First** rule of SYSCRAFTERS 2026, [`simulate_flash_sale.py`](./simulate_flash_sale.py) is a self-contained multi-threaded empirical verification harness (`ThreadPoolExecutor(max_workers=64)` + SQLite WAL with our exact Phase 7 `CHECK` and `UNIQUE` constraints + our Phase 3 Two-Layer Atomic Gate) built to prove that every architectural invariant holds under the exact stress parameters in the brief and all jury edge cases (`HIGH-01`, `HIGH-02`, `HIGH-03`, `MED-04`, `MED-06`).

---

## 2. Real Execution Output (Unmodified Terminal Capture)

Command executed:
```powershell
python "SALESTORM_TEAM_1\11_AI_Assisted_Validation\simulate_flash_sale.py"
```

Real output:
```text
================================================================================
SALESTORM | SYSCRAFTERS 2026 - TEAM 1
Phase 10: Executing Multi-Threaded 10,000-Request Flash-Sale & Audit Suite
================================================================================
[PASS] Scenario 1: OrderStateMachine strictly rejected CREATED -> DELIVERED
[PASS] Scenario 2A: ExpirySweeperWorker reclaimed 4 expired reservations & returned 410 Gone on stale key replay
[PASS] Scenario 2B: Redis restart CACHE_MISS (-1) single-flight re-seeded stock from DB without false SOLD_OUT
[PASS] Scenario 2C: Late-payment after expiry guarded by rowcount==1 check & auto-refunded (REFUNDED)
[PASS] Scenario 2D: PaymentReconciliationWorker resolved 1 TIMEOUT_UNKNOWN (202) payment -> RECONCILED_SUCCESS
[INFO] Wave 1 complete: 95 paid (94 direct + 1 reconciled), 5 failed payment (5%) & released 5 units.
[PASS] Scenario 3: During OrderService 30s outage -> CircuitBreaker state=OPEN, trips=1, fast_fails=80, buffered_outbox_events=100, orders_created=0
--------------------------------------------------------------------------------
FINAL EMPIRICAL VALIDATION SUMMARY (JSON):
{
  "elapsed_seconds": 0.878,
  "thread_pool_workers": 64,
  "total_reserve_requests_processed": 10212,
  "redis_lua_evaluations": 10213,
  "redis_fast_path_rejects": 9900,
  "redis_idempotent_replays": 200,
  "redis_cache_misses_reseeded": 1,
  "final_inventory": {
    "total_stock": 100,
    "available_quantity": 0,
    "reserved_quantity": 0,
    "sold_quantity": 100,
    "version": 223
  },
  "invariant_samples_checked": 39,
  "expired_reservations_reclaimed": 5,
  "successful_payments_settled": 100,
  "reconciled_timeout_unknown_payments": 1,
  "failed_payments_released": 5,
  "late_payments_auto_refunded": 1,
  "gateway_net_settled_charges": 100,
  "confirmed_orders_after_outage_recovery": 100,
  "outbox_events_published": 100,
  "circuit_breaker_trips": 1,
  "circuit_breaker_fast_fails": 80,
  "circuit_breaker_final_state": "CLOSED",
  "http_status_breakdown": {
    "201_RESERVED": 111,
    "200_RESERVE_IDEMPOTENT_REPLAY": 200,
    "409_SOLD_OUT": 9900,
    "410_RESERVATION_EXPIRED": 1,
    "200_PAYMENT_SUCCESS": 99,
    "200_PAYMENT_IDEMPOTENT_REPLAY": 20,
    "202_PAYMENT_TIMEOUT_RECONCILING": 1,
    "402_PAYMENT_FAILED_RELEASED": 5,
    "200_LATE_PAYMENT_AUTO_REFUNDED": 1
  }
}
--------------------------------------------------------------------------------
ALL 9 HARD INVARIANTS & JURY AUDIT EDGE CASES VERIFIED WITH ZERO VIOLATIONS!
================================================================================
```

---

## 3. Mathematical Reconciliation of Every Counter in the Output

Every single number in the output above reconciles to the exact unit and request:

1. **Total Reservation Requests (`10,212`) & Redis Lua Evaluations (`10,213`):**
   * `4` initial abandoned reservations (`Scenario 2A`: expired & reclaimed by `ExpirySweeperWorker` using `RESERVED -> TIMEOUT -> RELEASED` with `release_reason = 'TTL_EXPIRED'`) + `1` stale key replay returning `410_RESERVATION_EXPIRED` (`HIGH-02`).
   * `+ 1` reservation after a simulated Redis node restart (`Scenario 2B`, `HIGH-03`): first Lua call returned `-1 (CACHE_MISS)`, triggered the single-flight DB seed (`redis_cache_misses_reseeded = 1`), and succeeded on the retry Lua call (`+2` Lua calls for `1` HTTP request $\rightarrow 10,213$ Lua calls for $10,212$ requests!). Subsequently expired (`+1` reclaimed $\rightarrow$ `expired_reservations_reclaimed = 5`).
   * `+ 1` reservation for the late-payment after expiry test (`Scenario 2C`, `HIGH-01`), which was forcibly expired before settlement; `Tx_Pay_Settle` caught `rowcount == 0` on `WHERE status = 'PAYMENT_PENDING'` and issued an automatic gateway refund (`200_LATE_PAYMENT_AUTO_REFUNDED = 1`).
   * `+ 10,000` primary flash-sale customer requests across 64 concurrent threads (`100` won in Wave 1; `9,900` rejected with `409_SOLD_OUT`).
   * `+ 200` duplicate retry requests (`2%` of 10,000 across 64 threads, all `200` caught by `RedisLuaStockGate` and returned `200_RESERVE_IDEMPOTENT_REPLAY` with zero extra stock deducted).
   * `+ 5` Wave 2 customer requests that claimed the `5` units released by the `5%` payment failures (`PAYMENT_PENDING -> PAYMENT_FAILED -> RELEASED`).
   * **Total Requests:** $4 + 1 + 1 + 1 + 10,000 + 200 + 5 = 10,212$ (`201_RESERVED [111] + 200_RESERVE_IDEMPOTENT_REPLAY [200] + 409_SOLD_OUT [9,900] + 410_RESERVATION_EXPIRED [1] = 10,212`).

2. **Inventory Version Counter (`version = 223`):**
   * Initial seed starts at `version = 1`.
   * `Scenario 2A`: `4` abandoned reservations (`+4`) + `4` expiry releases (`+4`) = `+8`
   * `Scenario 2B`: `1` cache-miss reservation (`+1`) + `1` expiry release (`+1`) = `+2`
   * `Scenario 2C`: `1` late-pay reservation (`+1`) + `1` expiry release (`+1`) + `0` inventory mutation on auto-refunded late settlement = `+2`
   * `Wave 1`: `100` reservations (`+100`) + `94` direct payment successes (`+94`) + `1` reconciled `TIMEOUT_UNKNOWN` payment (`+1`) + `5` payment failures releasing stock (`+5`) = `+200`
   * `Wave 2`: `5` reservations (`+5`) + `5` payment successes (`+5`) = `+10`
   * **Total Version:** $1 + 8 + 2 + 2 + 200 + 10 = 223$.

3. **Hard Invariants Proven:**
   * `sold_quantity <= 100` (`sold_quantity == 100`, `available_quantity == 0`, `reserved_quantity == 0`).
   * `available_quantity >= 0` and `available + reserved + sold == 100` across all `39` mid-flight invariant snapshots.
   * `gateway_net_settled_charges == 100` (`99` direct `SUCCESS` + `1` `RECONCILED_SUCCESS`, while the `1` late payment after expiry was automatically refunded).
   * `confirmed_orders_after_outage_recovery == 100` (100% of paid purchases buffered in `outbox_event` during the `OrderService` outage reached `CONFIRMED` once the Circuit Breaker probed `HALF_OPEN` $\rightarrow$ `CLOSED`).
