# SQLx ODBC driver (experimental)

Enable the root `odbc` feature to expose `sqlx::odbc`, `Odbc`, `OdbcConnection`, `OdbcPool`, and `OdbcTransaction`.

Native connection lifecycle, queries, prepare/describe metadata, statement caching, buffered retrieval, outer transactions, and checked macros are implemented. OpenEdge acceptance is partial; other drivers are unverified. Nested transactions are rejected. Custom begin SQL is rejected before execution because generic ODBC cannot verify the transaction-state contract required by SQLx; nested custom statements return InvalidSavePointStatement without changing depth.

## Worker lifecycle

Native connection construction, calls, and destruction stay on the worker; the async frontend owns only channels and state. The command queue holds at most 16 commands, and replies at most one result. Cancelled connection attempts drop their eventual native connection on the worker. Dropping a connection never joins or invokes native cleanup on the async thread. Explicit close waits for native destruction; hard close has the same behavior because ODBC cannot safely force-disconnect a blocked native handle.

Rollback is an ordered, nonblocking enqueue. Flush and ping run after previously queued rollback; cleanup failure retires the worker. A full rollback queue retires the connection rather than blocking the async caller. Worker termination has an independent notification channel, so queued replies cannot strand awaiters after failure or panic.

Native calls cannot be forcibly cancelled: a hung driver may retain its thread and handles until the call returns. Login timeout defaults to 15 seconds and can be set with `login_timeout(seconds)` (zero requests unlimited); driver support varies. This setting is not preserved by URL serialization. Result streaming holds at most 32 owned rows/completion messages, and receiver drop unblocks producers. Individual large values may allocate more memory. Statements use a worker-owned LRU cache (default 100, configurable with `statement_cache_capacity`, zero disables) and a 30-second driver query timeout. `cached_statements_size` and `clear_cached_statements` expose cache management. `.persistent(false)` prevents retention. `.buffered(true)` enables 32-row columnar batches; default retrieval is row-by-row. Buffered text is limited to 32768 UTF-16 code units and binary to 65536 bytes per value, with truncation errors rather than silent shortening.

`OdbcDatabaseError` retains every diagnostic record supplied by the dependency and exposes the original native error. The final record provides SQLSTATE, while the displayed message includes the entire chain. Unknown constraint codes remain unclassified. Diagnostics can contain driver-supplied sensitive data; do not log them without appropriate handling.

The crate implements SQLx 0.9's owned statement/argument contracts, owned row/value storage, column indexing, parameter staging with typed NULL preservation and atomic encoding failures, and pool/executor type compatibility. Checked ODBC query macros support online and offline metadata with `odbc,macros` enabled. Missing parameter types are rejected rather than treated as reliable type checking. Any, migrations, and CLI backend integration are not provided.

## Scalar compatibility

| Rust | SQL/native parameter |
|---|---|
| i8, i16, i32, i64 | TINYINT, SMALLINT, INTEGER, BIGINT |
| f32, f64 | REAL, DOUBLE |
| bool | BIT, only 0 and 1 decode |
| str, String | Owned UTF-16 WVARCHAR/WLONGVARCHAR parameters; strict UTF-8 SQLx storage |
| [u8], Vec<u8> | VARBINARY; empty non-NULL input explicitly rejected |
| Option<T> | NULL retaining T's SQL type |

`OdbcArguments::to_native_parameters()` returns owned input parameters with native length/null indicators; keep the returned vector alive through native execution. Empty text receives backing storage without changing its zero-length indicator. Empty binary input is rejected because the tested OpenEdge driver converted it to a zero byte. The executor consumes these parameters on its worker. Text binding uses owned UTF-16 buffers to avoid interpreting UTF-8 bytes through driver-specific legacy encodings. Native wide-binding unit tests and OpenEdge persisted Latin-1 round trips pass, including accented text and large values. The tested database uses ISO8859-1: Greek/Japanese cannot be stored losslessly there. UTF-16 client binding does not override the destination database character set; unsupported characters may be replaced by the driver/server.

Numeric row storage uses exact-width little-endian values internally, converted to native scalar storage before binding. SQL NUMERIC/DECIMAL values are stored as text and can also decode into `f32` or `f64`, including explicit query-macro type overrides, without enabling `rust_decimal`. This conversion rounds to binary floating-point precision; use `Decimal` when exact decimal arithmetic is required. Malformed decimal text and conversions producing nonfinite floats are rejected. Other cross-type numeric conversions remain rejected instead of silently narrowing. Invalid UTF-8, invalid booleans, missing values, and wrong scalar widths fail decoding. The `chrono` feature supports NaiveDate (years 1 through 9999), NaiveTime (whole seconds only), and NaiveDateTime (nanoseconds, no leap seconds), binding native ODBC temporal structures. The `rust_decimal` feature supports Decimal using exact text with explicit SQL DECIMAL precision and scale; NULL defaults to DECIMAL(29,0). Both features are forwarded from the root crate. Driver precision limitations still require database validation. Unsigned integers, timezone-aware timestamps, and the separate `time` crate integration are not supported. Live OpenEdge tests verify buffered/unbuffered numeric, text, NULL, date/time and decimal result parity, as well as parameter predicates. OpenEdge prohibits parameters in the SELECT list; tests bind them in WHERE predicates.

## Options

Use `OdbcConnectOptions::new(connection_string)` for a native ODBC connection string. URL parsing accepts `odbc://?connection_string=<percent-encoded-string>`; use `to_url_lossy()` to serialize options safely rather than manually escaping delimiters. The resulting URL contains credentials if the native string does, so do not log it. Debug output omits the connection string.

## Execution safety and validation

Multi-result execution reads each count before advancing, including DML-first results. Two low-level unsafe calls are restricted to advancing a statement with no bound result buffers and constructing a cursor only after a positive result-column count; owned input parameters remain alive throughout. Unknown negative counts currently contribute zero to SQLx's affected-row total. Native execution errors retire the worker conservatively. Raw transaction-control SQL is not tracked; use the transaction API.

Live smoke testing requires `ODBC_TEST_CONNECTION_STRING` and `ODBC_TEST_QUERY` (a read-only query returning an INTEGER column): `cargo test -p sqlx-odbc --test live live_query_and_transaction_lifecycle -- --ignored --exact`. The local Progress DSN contains only a driver. Supply endpoint and credentials through the environment without logging secrets. Do not run every ignored test indiscriminately: scratch-table tests perform DDL and the cleanup test is manual.

OpenEdge validation covers lifecycle, pool cancellation, cache bounds, abandoned streams, parameter/metadata handling, type parity, server overflow retirement, and 16000-character buffered/unbuffered retrieval. A separate ignored unit test verifies actual client truncation before row delivery. After the database restart, all nine applicable integration tests plus the client-truncation unit test pass serially, including DML/rollback, 2000-byte binary, empty text, accented Latin-1 text, 16000-character ASCII and 8000-character Latin-1 persisted round trips in both retrieval modes. Scratch-table DROP succeeds. The database codepage is confirmed by its operator as ISO8859-1; Greek/Japanese persistence requires a Unicode-capable database and remains a separate ignored acceptance test. Scratch text setup and cleanup use nonoverlapping sessions, and cleanup reports the exact table without hiding the original failure. OpenEdge rejects tested semicolon batches, so successful multi-result acceptance needs another configured backend.

A separate `multiple_result_sets_and_reuse` test accepts `ODBC_BATCH_CONNECTION_STRING` for a driver supporting prepared SELECT batches. It checks metadata changes, completion counts, buffered/unbuffered parity, cached and nonpersistent execution, retained rows, and early-drop reuse. It is compiled but not live-validated here; the only registered database driver is OpenEdge. DML-first batch acceptance still needs a suitable backend fixture.

## Development

The manifest pins odbc-api 28.2.0 to Git revision `4d4a72479ef3ecad4d87bab69e4e2dfba0b131eb` over SSH. Fetching requires repository access; publishing requires a registry release of that dependency. An ODBC driver manager such as unixODBC must be installed for native linking. No data source is required for ordinary unit tests. Linux builds have been checked; Windows and macOS are unverified.

```sh
cargo test -p sqlx-odbc --lib
cargo +stable clippy -p sqlx-odbc --all-targets -- -D warnings
cargo check -p sqlx --no-default-features --features odbc,runtime-tokio
cargo check -p sqlx --no-default-features --features odbc,runtime-async-std
cargo check -p sqlx --no-default-features --features odbc,postgres,macros,runtime-tokio
```

`sqlx-core/offline` is deliberately enabled so feature unification with other drivers cannot change the required executor contract. The driver `offline` feature adds serializable metadata and macro type mappings. Run offline positive/compile-fail tests with `SQLX_OFFLINE=true SQLX_OFFLINE_DIR=$PWD/.sqlx cargo test -p sqlx --test odbc-macros --features odbc,runtime-tokio`.
