## Table of Contents

- [Version 1: Failure Handling Mechanism](#version-1-failure-handling-mechanism)
- [Version 2: Production-Ready Architecture](#version-2-production-ready-architecture)
  - [1. Integration Testing Strategy](#1-integration-testing-strategy)
  - [2. Asynchronous Test Synchronization](#2-asynchronous-test-synchronization)
  - [3. Observability & Metrics](#3-observability--metrics)
  - [4. Distributed Rate Limiting](#4-distributed-rate-limiting)
  - [5. Clean Architecture for Testability](#5-clean-architecture-for-testability)

# System Architecture & Evolution

## Version 1: Failure Handling Mechanism

How this code handles failures:

* **DB Crash:** The transaction in `CreateOrder` rolls back. No phantom emails.
* **Worker Crash:** If the worker dies during `SendEmail` (Phase 2), the row remains `processing`. The `RescueStuckJobs` function (which you would run in a separate cron or goroutine) will eventually reset it to `pending`.
* **Slow Email API:** Because we release the DB connection after Phase 1, a 30-second email delay does not block other database queries.
* **Race Conditions:** `FOR UPDATE SKIP LOCKED` ensures 50 copies of this worker can run efficiently without stepping on each other's toes.

---

## Version 2: Production-Ready Architecture

Here is the comprehensive summary of Version 2, covering the evolution from a "working" system to a Production-Ready, Observable, and Testable architecture.

### 1. Integration Testing Strategy
* **The Limitation of Mocks:** We established that mocking the database driver (`go-sqlmock`) is insufficient for concurrency testing because mocks cannot simulate complex engine behaviors like `SKIP LOCKED` or transaction isolation.
* **Testcontainers:** We adopted **Testcontainers** to spin up a real PostgreSQL Docker container for tests.
* **Performance Optimization:** Instead of spinning up a container per test (slow), we used the **TestMain Pattern** to spin up a Singleton Container once for the entire suite, using `TRUNCATE` to clean data between tests.

### 2. Asynchronous Test Synchronization
* **The "Sleep" Anti-Pattern:** We rejected using `time.Sleep()` to wait for workers, as it leads to slow and flaky tests.
* **Polling Assertions:** We implemented `assert.Eventually` (Polling) to repeatedly check the database state (e.g., every 10ms) until the job status changes to `sent`. This ensures