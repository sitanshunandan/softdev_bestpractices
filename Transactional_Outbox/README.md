How this code handles failures:
DB Crash: The transaction in CreateOrder rolls back. No phantom emails.

Worker Crash: If the worker dies during SendEmail (Phase 2), the row remains 'processing'. The RescueStuckJobs function (which you would run in a separate cron or goroutine) will eventually reset it to 'pending'.

Slow Email API: Because we release the DB connection after Phase 1, a 30-second email delay does not block other database queries.

Race Conditions: FOR UPDATE SKIP LOCKED ensures 50 copies of this worker can run efficiently without stepping on each other's toes.