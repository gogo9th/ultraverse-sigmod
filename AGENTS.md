<section id="project-info">
# Ultraverse Technical Documentation for Implementation Agents

## 1. System Overview
Ultraverse is a retroactive operation framework designed to recover from attacks (e.g., SQL injection) by changing, adding, or removing past committed queries without fully rolling back the database. It consists of two main components:
1.  **Database System:** Supports efficient retroactive operations on SQL queries.
2.  **Web Application Framework:** Ensures web application semantics are preserved during retroactive updates.

---

## 2. Database System Architecture
The agent must implement the following components as described in the architecture:

### 2.1 Query Analyzer
* Runs in the background with an unmodified database system.
* **Functions:**
    * Records column-wise read/write dependencies ($R/W$ sets).
    * Performs row-wise dependency analysis (Query Clustering).
    * Computes and logs table state hashes (Hash-Jumper).

### 2.2 Replay Scheduler
* Manages the rollback and replay phases.
* Executes multiple non-dependent queries in parallel during replay to maximize speed.

---

## 3. Dependency Analysis Logic
The core of Ultraverse is determining which queries depend on the retroactive target query ($Q_\tau$).

### 3.1 Column-wise Read/Write Sets ($R/W$)
The agent must parse SQL to extract $R$ and $W$ sets based on **Table A**:

| Query Type | Read Set ($R$) | Write Set ($W$) |
| :--- | :--- | :--- |
| **SELECT** | Columns in `SELECT`/`WHERE` + FKs to external tables. | $\emptyset$ |
| **INSERT** | Inner sub-query $R$ sets + FK columns. | All columns of the target table. |
| **UPDATE/DELETE** | Inner sub-query $R$ + Target columns read + FKs + `WHERE` columns. | Updated/Deleted columns + Referencing FKs in external tables. |
| **CREATE/ALTER** | FK columns of external tables. | All columns of the created/altered table. |
| **TRIGGER** | Union of $R$ sets of all inner queries. | Union of $W$ sets of all inner queries. |

### 3.2 Dependency Rules (Column-wise)
Based on **Table 1**, a dependency $Q_n \rightarrow Q_m$ exists if:
1.  **Direct:** $Q_n$ reads/writes a column that $Q_m$ wrote previously ($m < n$).
2.  **Transitive:** If $Q_n \rightarrow Q_m$ and $Q_m \rightarrow Q_l$, then $Q_n \rightarrow Q_l$.
3.  **Trigger:** If a query $Q_n$ depends on the target $Q_\tau$ and triggers $T_x$, then $T_x$ also depends on $Q_\tau$.

---

## 4. Optimization Techniques

### 4.1 Row-wise Query Clustering
Refines dependencies by checking if queries operate on overlapping table rows.
* **Cluster Key:** A column (e.g., `uid`) used to partition queries.
* **Logic:** If $K_c(Q_n) \cap K_c(Q_m) = \emptyset$, queries are independent and replay can be skipped for $Q_n$.
* **Key Selection:** Choose the column that minimizes the variance of cluster sizes (Choice Rule).

### 4.2 Hash-Jumper
Terminates replay early if the database state becomes identical to the pre-rollback state.
* **Implementation:** Compute an incremental hash of the table (add/subtract row hashes modulo $p$) upon every commit.
* **Check:** During replay, compare the current table hash with the logged hash. If they match, skip further replay for that table.

---

## 5. Retroactive Operation Workflow
The agent should implement the following state machine:

1.  **Rollback Phase:**
    * Identify dependent queries using the Dependency Graph.
    * Rollback **Mutated Tables** (written by dependents) and **Consulted Tables** (read by dependents) to a temporary database.
2.  **Replay Phase:**
    * Execute the retroactive change (add/remove/update $Q_\tau$).
    * Replay dependent queries from the temporary DB, running non-conflicting queries in parallel.
3.  **Update Phase:**
    * Lock the original database.
    * Reflect changes from the temporary DB to the original DB.
    * Unlock the database.
</section>
<section id="implementation-info">
# Implementation Info (Code-Based)

## 1. Executables / Entry Points
- `src/Application.*`: getopt-based CLI base. Each app implements optString()/main() and runs via `exec()`.
- `src/statelogd.cpp`: Reads binlog.index sequentially and generates `.ultstatelog` + `.ultchkpoint`. Required options: `-b` (binlog.index), `-o` (log name), `-k` (key columns). Uses `BinaryLogSequentialReader` (MySQLBinaryLogReaderV2); `-p` adds procedure logs, `-n` exits after EOF, `-r` restores checkpoints, `-d` discards previous logs, `-c` thread count, `-G/-Q` debug output, `-v/-V` log level.
- `src/db_state_change.cpp`: State change CLI. Environment variables `DB_HOST/DB_PORT/DB_USER/DB_PASS` are required. Actions are in the form `action1:action2:...` and support `make_cluster`, `rollback=gid[,gid...]`, `auto-rollback=ratio`, `prepend=gid,sqlfile`, `full-replay`, `replay`. Options: `-i` (state log), `-d` (DB), `-b` (backup), `-k` (key column groups), `-a` (alias), `-C` (threads), `-S` (skip gid), `-r` (report), `-N` (keep intermediate DB), `-w` (write state log), `-D` (dry-run), `-s/-e` (gid range), `--no-exec-replace-query` (manual replace-query mode). Range comparison method is selected via `RANGE_COMP_METHOD` (intersect/eqonly).
- `src/state_log_viewer.cpp`: `.ultstatelog` viewer. `-i` required, `-s/-e` range, `-v/-V` verbose output.

## 2. Execution Flow (CLI)
1) Generate `.ultstatelog`/`.ultchkpoint` from binlog via `statelogd`.
2) Generate `.ultcluster`, `.ultcolumns`, `.ulttables`, `.ultindex` via `db_state_change ... make_cluster`.
3) Run `db_state_change ... rollback=...` or `prepend=...` -> the **prepare phase** generates `.ultreplayplan` + a report (JSON).
4) Run `db_state_change ... replay` -> replays in parallel based on `.ultreplayplan`, then applies replace queries to reflect changes back to the original DB. (With `--no-exec-replace-query`, it does not execute and only prints queries, keeping the intermediate DB.)
5) (Optional) Run `db_state_change ... full-replay` -> sequentially replays all transactions except rollback GIDs.
- Actions are chained with `:`; `rollback=-` reads the GID list from stdin.
- `db_state_change --replay-from <gid>` performs **pre-replay** during the replay phase. It executes the range `<gid>..(at least target GID-1)` in parallel using a separate RowGraph, and then runs the existing replay plan after that work completes. **GID starts from 0**, and `--replay-from 0` is valid.

## 3. Code/Directory Map (Path -> Role)
- `src/base/*`: Shared interfaces/base logic. `base/DBEvent.*` does SQL parsing and R/W set construction; `base/DBHandlePool.*`/`TaskExecutor.*` handle execution and concurrency.
- `src/mariadb/binlog/*`: Binlog reading. `BinaryLogSequentialReader` walks binlog.index sequentially, and `MySQLBinaryLogReaderV2` decodes events using libbinlogevents.
- `src/mariadb/DBEvent.*`: MySQL event wrappers (Query/Row/TableMap/IntVar/Rand/UserVar).
- `src/mariadb/DBHandle.*`: MySQL connection/query execution, including test mocks.
- `src/mariadb/state/new/*`: Current state log/replay pipeline.
  - `StateLogWriter/Reader`, `GIDIndexReader/Writer`: `.ultstatelog`/`.ultindex` I/O.
  - `StateChanger.*`, `StateChangePlan/Report/ReplayPlan`: prepare/replay/full-replay orchestration.
  - `analysis/TaintAnalyzer.*`: Column taint propagation/filtering.
  - `cluster/StateCluster.*`, `cluster/StateRelationshipResolver.*`: Row-level clustering and FK/alias resolution.
  - `graph/RowGraph.*`: Parallel replay graph/worker scheduling.
  - `StateIO.*`: I/O abstraction (file/mock/backup loader).
  - `ProcLogReader/ProcMatcher`: Procedure hint restoration/tracing.
- `src/mariadb/state/*`: Legacy shared data structures (StateItem/StateHash, etc.).
- `include/state_log_hdr.h`: State log headers/time structs.
- `parserlib/*`: TiDB parser fork + C API (`capi.go`) + protobuf definitions (`ultparser_query.proto`).
- `dependencies/sql-parser/*`: Hyrise tokenizer.
- `mysql-server/`: MySQL source tree (externally provided; excluded via `.gitignore`). Required to build libbinlogevents.
- `scripts/esperanza/*`: Scripts to run BenchBase automatically and compare results.
- `tests/*`: Catch2-based unit tests (taint/rowgraph/statecluster/sqlparser, etc.).
- `cmake/*`, `src/CMakeLists.txt`: Build/dependency configuration.

## 4. Data/File Formats (As Implemented)
- `.ultstatelog`: `StateLogWriter` writes packed `TransactionHeader` + Protobuf-serialized `Transaction` records consecutively. Can skip using `TransactionHeader.nextPos`. (`StateLogReader::skipTransaction`)
- `.ultindex`: GID -> log offset index. `GIDIndexWriter` appends; `GIDIndexReader` queries via mmap.
- `.ultcluster`: `StateCluster` (row-level cluster) Protobuf binary (single message).
- `.ulttables`, `.ultcolumns`: `TableDependencyGraph`/`ColumnDependencyGraph` Protobuf binaries (updated in make_cluster; prepare performs column-wise filtering using column-set taint instead of graphs).
- `.ultreplayplan`: `StateChangeReplayPlan` Protobuf binary (`gids`, `userQueries`, `rollbackGids`, `replaceQueries`).
- `.ultchkpoint`: statelogd checkpoint file. Serialize code is currently commented out, so practical use is limited.

## 5. Core Module Map
- SQL parser:
  - `parserlib/capi.go` + `parserlib/parser/*`: TiDB parser (fork) + instance-based C API (`ult_sql_parser_create/ult_sql_parse_new/ult_query_hash_new/ult_parse_jsonify_new`) that produces protobuf (`ultparser_query.proto`).
  - `parserlib/capi.go` marshals parse results with `google.golang.org/protobuf/proto` and returns binary buffers via `C.CBytes` (callers must `free`).
  - Supported: GROUP BY/HAVING/aggregate/subquery/SET/SELECT INTO/DECIMAL.
  - C++ `base/DBEvent.cpp::QueryEventBase::parse()` consumes parse results from `libultparser` and handles DML/DDL.
  - Tokenizer: `dependencies/sql-parser` (Hyrise).
- Binlog parsing: `src/mariadb/binlog/*` + `src/mariadb/DBEvent.*` (converts MySQL binlog events into DBEvent).
- State log I/O: `StateLogWriter/Reader`, `GIDIndexReader/Writer`, `StateClusterWriter`.
- State change orchestration: `StateChanger` + `StateChangePlan/Context/Report`.
- Row-level clustering: `StateCluster` (v2), `RowCluster` (legacy).
- Parallel replay graph: `RowGraph` (dependency graph based on read/write ranges).

## 6. Internal Processing Steps (StateChanger)
1) **make_cluster**: Scan `.ultstatelog` -> build `StateCluster` + `ColumnDependencyGraph`/`TableDependencyGraph`, then store `.ultcluster/.ultcolumns/.ulttables/.ultindex`.
2) **prepare (rollback/prepend)**: `TaintAnalyzer` (column-wise) -> `StateCluster` (row-wise) to shrink replay GIDs, write `.ultreplayplan`, and generate `replaceQuery`.
3) **replay**: Read `.ultreplayplan` and replay in parallel via `RowGraph` (applies statement context).
4) **full-replay**: Sequentially replay all transactions except rollback GIDs to the intermediate DB and print only the "DB rename" guidance.

## 7. Dependency/Clustering Logic Summary
- `QueryEventBase::buildRWSet()` builds per-DML-type read/write `StateItem`s.
- `StateCluster` maintains per-key-column `StateRange` -> GID sets, and treats writes that intersect ranges read by rollback/prepend targets as replay targets.
- `RelationshipResolver` resolves column alias/FK chains to match real key columns. `CachedRelationshipResolver` provides a caching layer.
- `RowGraph` tracks the last read/write node per key range and adds edges (W->R, R->W, W->W), then workers execute entrypoints in parallel. It resolves FK/alias chains and normalizes to key columns; when key columns are not detected, it conservatively serializes via per-table/global wildcards.
- `-k` uses `,` to separate groups and `+` to express composite key groups (e.g., `users.id,orders.user_id+orders.item_id`). **Composite keys within the same table use AND**, and **groups that include multiple tables are processed as the intersection per key type (OR)**. Columns that do not exist are not expanded to wildcards.
- `Query` stores a separate column-wise read/write column set (serialized in `.ultstatelog`). In prepare, it does a first-stage filter using column-wise dependency (taint) and then shrinks via row-wise analysis. If column-wise is unrelated but key-column StateItems exist, prepare still runs row-wise `StateCluster::shouldReplay` and only then propagates column taint. DDL queries are skipped per query.
- `QueryEventBase` reflects subqueries/aggregate/GROUP BY/HAVING/DECIMAL/function expressions in the read columns, and conditionally adds per-table wildcards when there is no WHERE clause.
- In prepare, `analysis::TaintAnalyzer` propagates column taints and then shrinks via row-wise analysis; transactions where key columns are not detected are immediately marked for replay.
- `StateCluster::generateReplaceQuery()` uses per-table key-column projections, and adds columns that reference key columns via FKs to the projection so referenced tables are included in replace targets. Composite keys within the same table use AND, multi-table groups use OR to build WHERE, and columns not in the table are excluded from the predicate.
- `RowGraph` strengthened parallel replay stabilization and ordering control with per-column/composite-group worker queues, hold nodes, and wildcard holders.
- `RelationshipResolver`/`RowCluster` includes alias coercion, FK/alias chain infinite-loop guards, lowercase normalization, and implicit table inference for `_id`-suffixed columns.
- `StateChanger.sqlload` stores column-wise read/write sets via `DBEvent::columnRWSet()`, and `Query` keeps statement context (last_insert_id/insert_id/rand_seed/user vars).
- Replay applies `SET TIMESTAMP=<query.timestamp()>` before executing each statement to reproduce `CURRENT_TIMESTAMP()` results, and then applies the statement context (insert_id/rand_seed/user vars).
- `ProcMatcher::trace()` does not extract the readSet from IF/WHILE condition expressions, and unions only statements inside the blocks.

## 8. Hash-Jumper Notes
- `StateHash`/`HashWatcher` are designed to update table hashes incrementally from binlog row events to support early termination when the same state is reached.
- Currently, `StateChanger` only has members, and it is not used in the actual execution path.
- `TableDependencyGraph` reflects write-only queries as table dependencies, and `HashWatcher` uses `BinaryLogSequentialReader`.

## 9. Procedure Handling
- `statelogd` restores `ProcCall` from the `callid/procname/args/vars` fields of the `__ULTRAVERSE_PROCEDURE_HINT` row event. `args/vars` are parsed as a JSON object.
- When reconstructing CALL, `statelogd` replaces OUT/INOUT parameters with `@__ultraverse_out_<idx>` user vars, applies INOUT input values in advance via `SET @...` in the statement context, and copies the first query's statement context to the synthetic CALL to preserve RAND/user var determinism.
- `ProcMatcher` parses `procdef/<proc>.sql` to keep procedure metadata/statements, and estimates R/W sets via `trace()`.
- For procedure-call transactions, in addition to `ProcMatcher::trace()`, read/write sets and column sets collected from RowEvent in the same transaction are merged into the procCall Query and used for row/column analysis.
- Hash-based reconstruction via internal statement-hash matching (matchForward) has been removed: currently only the **CALL query + trace results** are recorded/analyzed.
- `ProcLogReader` can be opened via `statelogd.procedureLogPath`, but most matching/application paths are currently unconnected.

## 10. Current Limitations / Notes
- DDL support is incomplete: `processDDL()` is partial, and DDL queries are skipped with a warning in make_cluster/prepare.
- `prepend` input SQL supports DML only (DDL is an error/skip).
- `statelogd` requires `binlog_row_metadata=FULL`; `PARTIAL_UPDATE_ROWS_EVENT` is unsupported.
- Table_map_event SIGNEDNESS optional metadata is mapped to per-column unsigned flags; RowEvent integer decoding uses unsigned when the flag is set (missing metadata defaults to signed).
- `statelogd` sizes `TaskExecutor` using `statelogd.threadCount` (min 1); `QUERY` parsing and `ROW_QUERY` analysis run on worker threads, while `ROW_EVENT` `mapToTable()` + statement context handling remain on the main reader thread.
- Procedure hints support only the `callid/procname/args/vars` format; the legacy `callinfo` array format is unsupported.
- Protobuf-based `.ult*` serialization is a breaking change; legacy cereal logs are not readable, and the cereal submodule/serialization sources have been removed.
- `statelogd`'s `.ultchkpoint` serialization is commented out, so `-r` restore is limited.
- `db_state_change` fixes `stateLogPath` to `.` (FIXME), and `BINLOG_PATH` is stored only in the plan and unused in the current path.
- `db_state_change`'s `--gid-range` and `--skip-gids` now filter prepare/auto-rollback analysis (replay plan selection); make_cluster still scans all GIDs.
- `auto-rollback` (StateChanger::bench_prepareRollback) is benchmark-only and now runs the same column-taint + row-wise analysis as prepare; it selects rollback targets by ratio over in-scope GIDs (range/skip applied) and reports accurate replay counts without creating an intermediate DB.
- `statelogd` parses JSON `keyColumns` as groups, then flattens them for RW set computation to reflect `+` composite keys. It uses the same group parsing as `db_state_change`.
- In make_cluster, using column alias (`-a`) switches to sequential processing.
- `HashWatcher` exists only as a design and is not wired into the execution path.
- Esperanza bench scripts pass rollback GID lists directly into the action and use `full-replay`, and they reflect `+` composite key groups.
- TATP data generation must keep `msc_location`/`vlr_location` within signed INT32 range (<= 2147483647) unless the schema is changed to UNSIGNED.
- mysql-connector requires draining all result sets after `CALL` (e.g., `cursor.nextset()`), since TATP procedures like `InsertCallForwarding` include a `SELECT` that returns a result set.
- TATP `InsertCallForwarding` must choose an unused `start_time` for `(s_id, sf_type)` (or skip) to avoid duplicate primary keys; the client-side transaction now filters available slots.
- TATP `InsertCallForwarding` must also use an existing `(s_id, sf_type)` from `special_facility` to satisfy the FK; the client now picks a valid `sf_type` or skips if none exist.
- Esperanza's BenchBase table diff uses `ROW(...) NOT IN (...)` over a manually listed column set, so it can miss mismatches when NULLs are present, when duplicate rows are possible, or when tables/columns are omitted from `DB_TABLE_DIFF_OPTIONS`.
- Esperanza table diff can optionally print detailed diff rows via `ESPERANZA_TABLEDIFF_DETAILS=1`, and `ESPERANZA_TABLEDIFF_LIMIT` controls the max rows shown (default 20; 0 disables detail output).
- SEATS DDL uses DECIMAL for airport lat/long/GMT offset (ap_gmt_offset uses DECIMAL(6,2) to accommodate sentinel values like 327.0), airport distance, customer balance, and prices to avoid float drift; seats_standalone tablediff no longer needs epsilon comparisons.
- SEATS procdefs (`NewReservation`, `DeleteReservation`) now use DECIMAL(7,3) for price variables to match DECIMAL price columns.
- `StateCluster` now normalizes key column groups via FK/alias resolver and remaps cluster keys to the canonical column during `make_cluster`/`prepare`; regenerate `.ultcluster` (run `make_cluster`) if older files used non-canonical key columns.
- `StateCluster::Cluster::match()` now applies FK/alias chain resolution for WRITE items as well, so foreign-key writes can contribute to cluster matching.
- `StateChanger::analyzeReplayPlan()` now refreshes target cache on the first non-target transaction after a rollback target that skipped revalidation, preventing stale cache when consecutive rollback gids are skipped by range/skip rules.
- `MySQLBackupLoader` checks mysql client paths in `MYSQL_BIN_PATH`/`MYSQL_BIN`/`MYSQL_PATH` first; if a directory is given, it appends `/mysql`; otherwise it uses `/usr/bin/mysql`.
- When deserializing `StateData`, string memory must follow the `malloc/free` convention (`new/free` mixing can crash).
- `ProcMatcher::trace()` interprets procedure parameters, `DECLARE` local variables, and `@var` as symbols. `SELECT ... INTO` defaults to UNKNOWN, but if a KNOWN value already exists (hints/initial vars), it is kept. Functions/complex expressions become UNKNOWN; if such a value is written to a key column, it remains a wildcard.
- RowEvent string-length decoding avoids unaligned/strict-aliasing reads (uses memcpy-style loads) to keep -O2 behavior consistent.
- RowEvent variable-length decoding derives row size from length prefixes (VARCHAR/VAR_STRING/STRING/BLOB/JSON) and clamps to remaining bytes; unknown field lengths now emit warnings and skip the rest of the row payload.
- `StateData::Copy()` now copies string/decimal buffers using the stored length (not `strdup`) to preserve embedded nulls and avoid serialization overreads.
- `BinaryLogReaderBase` has a virtual destructor to avoid new/delete size mismatch when deleting derived binlog readers via base pointer.
- `RowEvent::mapToTable()` / `readRow()` mutate internal vectors and are not thread-safe or idempotent; callers should treat a `RowEvent` instance as single-threaded and call `mapToTable()` once.
- `QueryEventBase::parse()` uses a `thread_local` parser handle (`ult_sql_parser_create`), so SQL parsing is per-thread and can run in parallel.
- `StateRange::MakeWhereQuery()` now formats string-typed values as hex literals (`X'..'`) to keep replace-query predicates safe for varchar/binary data.
- `QueryEventBase::processExprForColumns()` falls back to the base table of a single derived table when a subquery has no primary table; multi-table derived/joins remain conservative (unqualified identifiers are not auto-mapped).
- `StateItem::MakeRange2()` mutates an internal cache and is not thread-safe when the same `StateItem` is shared across threads; `CachedRelationshipResolver` now precomputes `MakeRange2()` before caching to avoid concurrent writes, but callers should still avoid cross-thread mutation.
- `StateRange` uses an internal shared_ptr for range vectors; mutating APIs now detach (copy-on-write) to prevent aliasing when a `StateRange` is copied and then modified.
- `RowGraph::gc()` is stop-the-world: it pauses column/composite worker threads and waits for in-flight tasks to drain before removing vertices; replay GC interval is ~10s (pre-replay and main replay).
- `WhereClauseBuilder` treats column-to-column WHERE comparisons as wildcard row ranges and adds both columns to the read set; `StateLogViewer` suppresses empty-where warnings for wildcard items.
- `state_log_viewer` only prints `varMap`; it does not display `QueryStatementContext` (rand seed/insert id/user vars).
- `statelogd` formats DOUBLE literals for reconstructed CALL statements using `max_digits10` precision to preserve round-trip numeric accuracy.
- `StateData` stores DECIMAL values as raw binary bytes (no normalization). DECIMAL equality uses byte compare; range comparisons (<, <=, >, >=) are disabled, and SQL WHERE formatting emits hex literal `X'...'`.
- TPC-C `scripts/esperanza/tpcc_sql/ddl-mysql.sql` RandomNumber/NonUniformRandom now take a seed and advance `@tpcc_seed`; `NewOrder`/`Delivery` initialize `@tpcc_seed` with `UNIX_TIMESTAMP(CURRENT_TIMESTAMP())`.

## 11. Test/I/O Abstractions
- DB handle abstraction: `mariadb::DBHandle` (interface) + `MySQLDBHandle` + `MockedDBHandle`; also introduced `DBResult` and `DBHandlePoolAdapter`.
- State log/cluster/backup I/O abstraction: `IStateLogReader`/`MockedStateLogReader`, `IStateClusterStore`/`FileStateClusterStore`/`MockedStateClusterStore`, `IBackupLoader`/`MySQLBackupLoader`; grouped and injected via `StateIO`.
- `StateLogReader::seekGid()` works based on `GIDIndexReader`.
- Tests target Catch2 v3 + C++20; `sqlparser-test` uses the new parser API.
- Minimum CMake version is 3.28; fmt/spdlog are managed via FetchContent.

## 12. DECIMAL Handling
- Added `DECIMAL` to `DMLQueryExpr.ValueType` in `parserlib/ultparser_query.proto`, and pass it via the `decimal` string field.
- C++ `StateData` stores normalized strings as `en_column_data_decimal` and supports DECIMAL in comparisons/hashing/serialization.
- Improved range-intersection/AND handling in `StateItem`, and improved `FUNCTION_WILDCARD` handling. `state_log_time.sec_part` was changed to `uint32_t`.

## 13. MySQL libbinlogevents Integration (Phase 1, 2026-01-21)
- CMake option: `ULTRAVERSE_MYSQLD_SRC_PATH` (default `./mysql-server`). It checks for `libs/mysql/binlog/event` and fails with `FATAL_ERROR` if missing.
- The `mysql-server` source directory is ignored by the repo (via `/mysql-server` in `.gitignore`).
- Build module: `cmake/mysql_binlogevents.cmake` defines the static library `mysql_binlog_event_standalone`.
  - Collected via GLOB: `binlog/event`, `codecs`, `compression`, `serialization`, `gtid`, `containers`.
  - Adds MySQL mysys sources: `mysys/pack.cc` + `mysys/decimal.cc` (packed-length helpers + DECIMAL binary conversion used by row decoding).
  - Include dirs: `${ULTRAVERSE_MYSQLD_SRC_PATH}/libs`, `${ULTRAVERSE_MYSQLD_SRC_PATH}/include`, `${ULTRAVERSE_MYSQLD_SRC_PATH}`.
  - Compile defs: `STANDALONE_BINLOG`, `BINLOG_EVENT_COMPRESSION_USE_ZSTD_system`.
  - Link: `ZLIB::ZLIB`, `PkgConfig::ZSTD`.
- `src/CMakeLists.txt` includes the module and links `mysql_binlog_event_standalone` into `ultraverse`.
- Known successful build (ultraverse target only):
  - `cmake -S . -B build`
  - `GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build --target ultraverse`
  - Use `/tmp` cache paths to avoid Go cache permission issues.
  - The user must provide `my_config.h` at `${ULTRAVERSE_MYSQLD_SRC_PATH}/include/my_config.h`.

## 14. MySQL Binlog Reader/Adapter Integration (Phase 2, 2026-01-21)
- Added `MySQLBinaryLogReaderV2`: maps events and handles FDE/checksum/TRANSACTION_PAYLOAD_EVENT using libbinlogevents.
- `BinaryLogSequentialReader` was switched to use the V2 reader.
- Maps column names using optional metadata (COLUMN_NAME) from `Table_map_event`, assuming `binlog_row_metadata=FULL`. If missing, it warns and skips.
- Parses row events using column bitmaps; `PARTIAL_UPDATE_ROWS_EVENT` is unsupported (warn and skip).
- Rows-event width uses our own length-encoded integer decode (`MySQLBinaryLogReaderV2::readNetFieldLength`), and row payload decoding into values happens in `RowEvent::readRow`.
- `TableMapEvent` stores MySQL raw `enum_field_types` + per-column metadata (MySQL `read_field_metadata` rules; NEWDECIMAL metadata is big-endian). `RowEvent::readRow()` uses `libbinlogevents` `calc_field_size()` and MySQL metadata rules for STRING/VARCHAR/BLOB/ENUM/SET parsing to avoid row-image drift.
- `HashWatcher` was switched to the MySQL binlog sequential reader path.

## 15. MySQL Binlog Migration Complete (Phase 3, 2026-01-21)
- Removed custom binlog readers: deleted `MySQLBinaryLogReader` (v1) and `MariaDBBinaryLogReader`.
- Simplified `BinaryLogSequentialReader` to MySQL-only and pinned it to V2.
- Cleaned up `statelogd`/`HashWatcher` to use `BinaryLogSequentialReader`.

## 16. Additional MySQL Binlog Event Support (Phase 4, 2026-01-21)
- Decode `INTVAR_EVENT`/`RAND_EVENT`/`USER_VAR_EVENT` into `DBEvent` and connect them to statement context for the next statement in `statelogd`.
- Serialize statement context (last_insert_id/insert_id/rand_seed/user var) in `state::v2::Query`.
- Reproduce the same session state by applying `SET` before executing statements during replay.
- USER_VAR is applied as `SET @v := ...`; charset/collation mapping remains TODO.

</section>

<section id="agent-rules">
# AGENT RULES

## 1. Interaction & Language
- If you are unsure about something or have questions while working, prioritize asking the user to clarify rather than guessing.
- The user may not be a native English speaker. Please conduct all conversations and plan writing in the language specified by the `$LANG` environment variable.
- When there is important project information or a major change, update `AGENTS.md` so the documentation reflects the latest state.
- When you learn new facts during work (code behavior/constraints, etc.), record them in `AGENTS.md` to keep it up to date.
- If you cannot proceed due to missing permissions, you must request elevation from the user. (If a command fails due to insufficient permissions, you must elevate the command to the user for approval.)

## 2. Workflow Protocol (Important)
Codex acts autonomously by default, but you must change behavior when the **[Explicit Plan Mode]** conditions below apply.

### [Explicit Plan Mode] Triggers
1. The user explicitly requests **"Plan mode"**, **"planning mode"**, or **"design first"**.
2. The work would cause structural changes across **3 or more files**, or is a risky change that touches **core logic**.

### [Explicit Plan Mode] Rules
When the conditions are met, **stop implementing immediately** and follow this procedure:
1. **Stop:** Do not write or modify code. (Reading files is allowed.)
2. **Plan:** Use the `update_plan` tool to write a detailed implementation plan, scope of impact, and expected risks in **Korean**.
3. **Ask:** Present the plan to the user and ask for approval by saying **"Shall we proceed with this plan?"**.
4. **Action:** Modify code only after the user gives explicit approval (e.g., "OK", "go ahead").

*(For simple changes or bug fixes that do not meet the conditions above, proceed immediately without asking for approval and report the result as usual.)*

## COMMIT CONVENTIONS

- When writing a git commit, prioritize following the existing commit conventions, and do not add yourself as a co-author.
- The commit convention is:

```
[scope]: [subject]
```

- [scope]: a short word describing the change area (e.g., core, ui, docs)
- [subject]: a concise description of the change (imperative mood)

### EXAMPLES
- `transport/quic: Add QUIC connection retry logic`
- `msgdef/v1/channels: Update channel message definitions`
- `docs(README): Add an installation guide to README`
- `test(transport/quic): Add QUIC transport test cases`

## 3. Reference Docs
- If you need detailed algorithm/theory information, read `ultraverse.md`:
  - Mathematical definition and workflow of Retroactive Operation
  - R/W set generation policy (Table A)
  - Query Dependency Graph construction rules
  - Details of Query Clustering / Hash-Jumper algorithms

## 4. AGENTS.md Update Rules
If you discover new facts like the following during work, **you must record them in AGENTS.md**:
- Roles of new modules/files not documented yet
- Important findings about code behavior (e.g., hidden constraints, bug workarounds)
- New build/test commands or caveats
- Undocumented behavior of CLI options/environment variables/settings
- Performance findings (bottlenecks, optimization tips)

Recommended locations to record them:
- File locations/roles -> `<section id="file-location-map">`
- Build/tests -> `<section id="build-commands">`
- Implementation details/execution flow -> `<section id="implementation-info">`
- Limitations/notes -> `## 10. Current Limitations / Notes` under `<section id="implementation-info">`

## 5. Test Writing Rules (Important)
When writing tests, you must follow the rules below:

1. **Does it match the intent of `ultraverse.md`?**
   - Tests must verify the purpose and behavior of Retroactive Operation as defined in `ultraverse.md`.
   - Design tests based on the documented spec (R/W set policy, Query Dependency rules, Clustering logic, etc.).

2. **Always be honest: write the "correct tests", not tests that merely pass.**
   - Even if the test fails right now, write the test that matches the intent of `ultraverse.md`.
   - If the current implementation differs from the spec, write tests against the spec and then fix the implementation.
   - **The goal is not to make tests pass, but to raise the project's completeness.**
   - Do not "game" tests to fit the existing code, and do not weaken assertions just to make tests always succeed.

</section>




<section id="build-commands">
# Build & Test Commands

## Build
```bash
# Configure
# - The MySQL source tree (=libbinlogevents) must exist at ./mysql-server,
#   or specify it via cmake -DULTRAVERSE_MYSQLD_SRC_PATH=/path/to/mysql-server
# - The MySQL source tree must be configured by CMake at least once
#   (because we reference autogenerated headers like include/my_config.h, include/mysql_version.h)
# - Use -DCMAKE_BUILD_TYPE=Debug/Release for single-config generators (default: Debug)
cmake -S . -B build

# Build all targets (use /tmp caches to avoid Go cache permission issues)
GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build

# Build specific target (ultraverse library only)
GOCACHE=/tmp/go-build-cache GOMODCACHE=/tmp/go-mod-cache GOPATH=/tmp/go cmake --build build --target ultraverse

# Build specific executables
cmake --build build --target statelogd
cmake --build build --target db_state_change
cmake --build build --target state_log_viewer
```
Notes:
- CMake copies `libultparser.so` next to `ultraverse`, `statelogd`, `db_state_change`, and `state_log_viewer`, and sets RPATH to `$ORIGIN` (or `@loader_path` on macOS) so the binaries can resolve the parser library from their own directory.
- The same RPATH + copy rule applies to unit test executables so `ctest` can locate `libultparser.so`.
- On macOS, `libultparser.so` install_name is set to `@rpath/libultparser.so`, and runtime targets patch their load command to use `@rpath` (fixes dyld lookup when running outside the build directory).
- GitHub Actions uses a prebuilt CI container image `ghcr.io/<repo-owner>/ultraverse-ci:ubuntu-24.04` built from `ci/Dockerfile` via `.github/workflows/build-ci-image.yml`.
- `parserlib` uses `go mod download` during build to avoid failing on missing `pb/ultparser_query.pb.go`; re-run CMake if you updated `parserlib/CMakeLists.txt`.

## Test
```bash
# Run all tests
cd build && ctest

# Build + run all tests (allTests target)
cmake --build build --target allTests

# Run specific test
cd build && ctest -R statecluster-test
cd build && ctest -R sqlparser-test

# Run a test executable directly (more detailed output)
./build/tests/statecluster-test
./build/tests/sqlparser-test
./build/tests/rowgraph-test
```

## Available Test Targets
- `statecluster-test`: StateCluster row-level clustering tests
- `rowgraph-test`: RowGraph parallel execution graph tests
- `relationship-resolver-test`: Column alias/FK chain resolution tests
- `statehash-test`: Table state hash tests
- `stateitem-test`: StateItem range operation tests
- `query-transaction-serialization-test`: Query/Transaction Protobuf round-trip tests
- `sqlparser-test`: libultparser SQL parsing tests
- `tabledependencygraph-test`: Table dependency graph tests
- `naminghistory-test`: Table rename history tests
- `rowcluster-test`: RowCluster tests
- `taintanalyzer-test`: Column taint propagation tests
- `queryeventbase-rwset-test`: Query R/W set generation tests
- `procmatcher-trace-test`: Procedure tracing tests
- `statechanger-test`: StateChanger integration tests

## Esperanza Mini Integration (Minishop)
```bash
# Minishop integration test (run MySQL -> run scenario -> verify rollback/replay)
python scripts/esperanza/minishop.py
```
- `ULTRAVERSE_HOME` must point to the directory that contains `statelogd`/`db_state_change`/`state_log_viewer` binaries.
- Scenario SQL files: `scripts/esperanza/minishop/*.sql`; procedure definitions: `scripts/esperanza/procdefs/minishop/*.sql`.
- When creating procedures, apply the hint-injection patch via `procpatcher`; it generates `procpatcher/__ultraverse__helper.sql` in the session path.

## Unit Test Notes (Esperanza)
- For Esperanza unit tests/scripts, `cd scripts/esperanza` first and then run them.
- Default timeout is set to 120 minutes.
- Run with elevated permissions.
- Use `python3` instead of `python`.

## Prerequisites
- CMake 3.28+
- C++20-capable compiler
- Go (for building parserlib)
- MySQL source (for libbinlogevents; default `./mysql-server`)
- Dependencies: Boost, OpenSSL, protobuf, zstd, tbb, libmysqlclient
  - MariaDB client fallback is removed; MySQL client library is required.

</section>


<section id="file-location-map">
# File Location Map (Path -> Role)

## Executables (src/*.cpp)
| File | Role |
|------|------|
| `src/statelogd.cpp` | Daemon that converts binlog.index into `.ultstatelog`/`.ultchkpoint`. |
| `src/db_state_change.cpp` | State change CLI (make_cluster, rollback, prepend, replay, full-replay). |
| `src/state_log_viewer.cpp` | Viewer for inspecting `.ultstatelog` contents. |
| `src/Application.cpp`, `src/Application.hpp` | getopt-based CLI base class. |

## Core Libraries
| Directory/File | Role |
|---------------|------|
| `src/base/DBEvent.cpp`, `src/base/DBEvent.hpp` | Common event logic and R/W set construction based on SQL parse results. |
| `src/mariadb/DBEvent.cpp`, `src/mariadb/DBEvent.hpp` | MySQL binlog event wrappers; converts decoded results into Query objects. |
| `src/mariadb/DBHandle.cpp`, `src/mariadb/DBHandle.hpp` | DB connections/query execution + test mocks. |
| `src/mariadb/state/WhereClauseBuilder.*` | Helper for parsing WHERE clauses and building StateItem; shared by statelogd/ProcMatcher. |
| `src/mariadb/binlog/BinaryLogSequentialReader.*` | Sequential reader based on binlog.index. |
| `src/mariadb/binlog/MySQLBinaryLogReaderV2.*` | MySQL binlog parsing/mapping using libbinlogevents. |

## State Management (src/mariadb/state/new/)
| File | Role |
|------|------|
| `src/mariadb/state/new/Transaction.*`, `src/mariadb/state/new/Query.*` | Transaction/Query data structures + Protobuf conversion helpers. |
| `src/mariadb/state/new/StateLogWriter.*`, `src/mariadb/state/new/StateLogReader.*` | `.ultstatelog` file I/O. |
| `src/mariadb/state/new/GIDIndexWriter.*`, `src/mariadb/state/new/GIDIndexReader.*` | `.ultindex` GID->offset index. |
| `src/mariadb/state/new/StateChanger.*` | State change orchestration (prepare/replay/full-replay). |
| `src/mariadb/state/new/StateChanger.prepare.cpp` | Prepare phase: column taint -> row-wise filtering. |
| `src/mariadb/state/new/StateChanger.replay.cpp` | Replay phase: parallel replay execution. |
| `src/mariadb/state/new/StateChanger.sqlload.cpp` | SQL loading and R/W set computation. |
| `src/mariadb/state/new/StateChangePlan.*` | State change plan (actions, GID ranges, skip lists, etc.). |
| `src/mariadb/state/new/StateChangeContext.hpp` | Execution context (replay target GIDs, replace queries, etc.). |
| `src/mariadb/state/new/StateChangeReport.*` | Report (JSON) generation. |
| `src/mariadb/state/new/StateChangeReplayPlan.hpp` | `.ultreplayplan` serialization schema. |
| `src/mariadb/state/new/StateIO.*` | State I/O abstraction (file/mock/backup loader). |
| `src/mariadb/state/new/proto/ultraverse_state.proto` | Protobuf schemas for state log/cluster/plan serialization. |
| `src/mariadb/state/new/proto/ultraverse_state_fwd.hpp` | Forward declarations for generated Protobuf types. |

## Clustering & Analysis (src/mariadb/state/new/)
| File | Role |
|------|------|
| `src/mariadb/state/new/cluster/StateCluster.*` | Row-level cluster (StateRange->GID mapping per key column). |
| `src/mariadb/state/new/cluster/StateRelationshipResolver.*` | Column alias/FK chain resolution. |
| `src/mariadb/state/new/cluster/NamingHistory.*` | Table rename history tracking. |
| `src/mariadb/state/new/cluster/RowCluster.*` | Legacy cluster implementation. |
| `src/mariadb/state/new/analysis/TaintAnalyzer.*` | Column-wise taint propagation analysis. |
| `src/mariadb/state/new/graph/RowGraph.*` | Dependency graph/worker scheduling for parallel replay. |
| `src/mariadb/state/new/TableDependencyGraph.*` | Table-level dependency graph. |
| `src/mariadb/state/new/ColumnDependencyGraph.*` | Column-level dependency graph. |
| `src/mariadb/state/new/HashWatcher.*` | State-hash watcher for Hash-Jumper (not wired into the execution path yet). |

## Procedure Handling (src/mariadb/state/new/)
| File | Role |
|------|------|
| `src/mariadb/state/new/ProcCall.*` | Procedure call data structures. |
| `src/mariadb/state/new/ProcMatcher.*` | Reconstruct/trace internal queries from procedure definitions. |
| `src/mariadb/state/new/ProcLogReader.*` | Procedure log reader. |

## SQL Parser (parserlib/)
| File | Role |
|------|------|
| `parserlib/capi.go` | C API entry point (ult_sql_parser_create, ult_sql_parse_new, etc.). |
| `parserlib/parser/*` | Parser implementation based on the TiDB parser fork. |
| `parserlib/ultparser_query.proto` | Protobuf schema for parse results. |

## Procpatcher (procpatcher/)
| File | Role |
|------|------|
| `procpatcher/main.go` | Stored procedure patch CLI. |
| `procpatcher/delimiter/preprocessor.go` | DELIMITER-aware lexer: statement splitting + original byte-offset tracking. |
| `procpatcher/patcher/text_patch.go` | Patch original text based on OriginTextPosition (preserves indentation). |

- In `--depatch` mode, `procpatcher` removes `__ULTRAVERSE_PROCEDURE_HINT` INSERTs and legacy `__ultraverse_callinfo` declarations/INSERTs; in this mode it does not generate `__ultraverse__helper.sql`.
- `--repatch` runs depatch first and then patches again with the latest hint format; helper SQL generation is the same as in patch mode.

## Esperanza (scripts/esperanza/)
| File | Role |
|------|------|
| `scripts/esperanza/minishop.py` | Minishop integration test (run the scenario and verify rollback/replay). |
| `scripts/esperanza/minishop/*.sql` | Minishop schema/procedure/scenario/verification SQL. |
| `scripts/esperanza/procdefs/minishop/*.sql` | Procedure definitions for statelogd ProcMatcher. |
| `scripts/esperanza/tatp_standalone.py` | Standalone TATP runner (MySQL lifecycle, workload execution, statelog, and state change). |
| `scripts/esperanza/esperanza/epinions/*` | Epinions workload helpers (constants, Zipfian generators, random utilities). |
| `scripts/esperanza/esperanza/tatp/*` | TATP workload helpers (constants, random utilities, data generator, transactions, workload executor, session). |

## Tests (tests/)
Test files are named as `<target-module>-test.cpp`. Uses Catch2 v3.

</section>
