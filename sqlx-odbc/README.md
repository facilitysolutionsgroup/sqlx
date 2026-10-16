# SQLx ODBC driver (experimental)

Enable the root `odbc` feature for `sqlx::odbc`, `Odbc`, `OdbcConnection`, `OdbcPool`, and `OdbcTransaction`. The driver supports native connection lifecycle, queries, prepare/describe metadata, statement caching, buffered retrieval, outer transactions, and checked macros. Compatibility varies by ODBC driver; validate the features you use against your database. Nested transactions and custom begin SQL are rejected.

## Worker lifecycle

Native connection construction, calls, and destruction stay on a worker thread. The async frontend owns channels and state. The command queue holds at most 16 commands, and replies at most one result. Cancelled connection attempts drop their eventual native connection on the worker. Dropping a connection never joins or invokes native cleanup on the async thread. Explicit and hard close wait for native destruction; ODBC cannot safely force-disconnect a blocked native handle.

Rollback is an ordered, nonblocking enqueue. Flush and ping run after previously queued rollback; cleanup failure retires the worker. A full rollback queue retires the connection rather than blocking the async caller. Worker termination has an independent notification channel so queued replies cannot strand awaiters after failure or panic.

Native calls cannot be forcibly cancelled: a hung driver may retain its thread and handles until the call returns. Login timeout defaults to 15 seconds and can be set with `login_timeout(seconds)` (zero requests unlimited); driver support varies. This setting is not preserved by URL serialization. Result streaming holds at most 32 owned rows/completion messages, and receiver drop unblocks producers. Statements use a worker-owned LRU cache (default 100, configurable with `statement_cache_capacity`; zero disables) and a 30-second driver query timeout. `.persistent(false)` prevents retention. `.buffered(true)` enables 32-row columnar batches; default retrieval is row-by-row. Buffered text is limited to 32768 UTF-16 code units and binary to 65536 bytes per value, with truncation errors rather than silent shortening.

`OdbcDatabaseError` retains every diagnostic record supplied by the dependency and exposes the original native error. The final record provides SQLSTATE, while the displayed message includes the entire chain. Unknown constraint codes remain unclassified. Diagnostics can contain driver-supplied sensitive data; do not log them without appropriate handling.

The driver implements SQLx 0.9's owned statement/argument contracts, owned row/value storage, column indexing, parameter staging with typed NULL preservation, and pool/executor type compatibility. Checked ODBC query macros support online and offline metadata with `odbc,macros` enabled. Missing parameter types are rejected rather than treated as reliable type checking. ODBC support for `Any` and migrations is not provided; `cargo sqlx prepare` compiles checked queries against the configured ODBC URL.

## Scalar compatibility

| Rust | SQL/native parameter |
|---|---|
| i8, i16, i32, i64 | TINYINT, SMALLINT, INTEGER, BIGINT |
| f32, f64 | REAL, DOUBLE |
| bool | BIT, only 0 and 1 decode |
| str, String | Owned UTF-16 WVARCHAR/WLONGVARCHAR parameters; strict UTF-8 SQLx storage |
| [u8], Vec<u8> | VARBINARY; empty non-NULL input explicitly rejected |
| Option<T> | NULL retaining T's SQL type |

`OdbcArguments::to_native_parameters()` returns owned input parameters with native length/null indicators; keep the returned vector alive through native execution. Empty text receives backing storage without changing its zero-length indicator. Empty binary input is rejected because some drivers bind it as a zero byte. Text binding uses owned UTF-16 buffers; the destination database character set still determines which characters can be stored losslessly.

Numeric row storage uses exact-width little-endian values internally. SQL NUMERIC/DECIMAL values are stored as text and can decode into `f32` or `f64`; binary floating-point conversion loses decimal precision. Use `Decimal` for exact arithmetic. Invalid UTF-8, invalid booleans, missing values, wrong widths, malformed decimals, and nonfinite float conversions fail decoding. The `chrono` feature supports NaiveDate (years 1 through 9999), NaiveTime (whole seconds), and NaiveDateTime (nanoseconds, no leap seconds). The `rust_decimal` feature supports Decimal using exact text with SQL DECIMAL precision and scale. Driver precision and SQL dialect restrictions require database-specific validation. Unsigned integers, timezone-aware timestamps, and the separate `time` crate integration are not supported.

## Options and execution safety

Use `OdbcConnectOptions::new(connection_string)` for a native ODBC connection string. URL parsing accepts `odbc://?connection_string=<percent-encoded-string>`; use `to_url_lossy()` to serialize options rather than manually escaping delimiters. The resulting URL contains credentials if the native string does, so do not log it. Debug output omits the connection string.

Multi-result execution reads each count before advancing. Two low-level unsafe calls are restricted to advancing a statement with no bound result buffers and constructing a cursor only after a positive result-column count; owned input parameters remain alive throughout. Unknown negative counts currently contribute zero to SQLx's affected-row total. Native execution errors retire the worker conservatively. Raw transaction-control SQL is not tracked; use the transaction API.

## Development

The manifest pins a custom `odbc-api` revision over SSH. Fetching requires repository access; publishing requires a registry release of that dependency. An ODBC driver manager such as unixODBC must be installed for native linking. No data source is needed for unit tests. Linux builds have been checked; Windows and macOS are unverified.

```sh
cargo test -p sqlx-odbc --lib
cargo +stable clippy -p sqlx-odbc --all-targets -- -D warnings
cargo check -p sqlx --no-default-features --features odbc,runtime-tokio
cargo check -p sqlx --no-default-features --features odbc,postgres,macros,runtime-tokio
```

`sqlx-core/offline` is enabled so feature unification cannot change the required executor contract. The driver `offline` feature adds serializable metadata and macro type mappings. Use a database-specific integration suite and a read-only connection URL to validate query macros and driver behavior; never run scratch-table tests on a working database.
