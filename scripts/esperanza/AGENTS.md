# Repository Guidelines

## Project Structure & Module Organization
- Entry scripts: `tpcc.py`, `tatp.py`, `seats.py`, `epinions.py`, `astore.py`, `updateonly.py`, `minishop.py`, `minishop_prepend.py`, `tpcc_standalone.py`, `epinions_standalone.py` run each workload and perform the prepare/execute (or scenario execution), statelog generation, and rollback/replay flow end-to-end.
- `minishop_prepend.py` validates a rollback + prepend (partial refund correction) case based on the Minishop scenario.
- `minishop.py` patches procedure definitions via `procpatcher` and applies them to MySQL; it also generates `procpatcher/__ultraverse__helper.sql` under the session path.
- `procpatcher` has its own `go.mod` under `procpatcher/`, so run it from that directory with `go run .`.
- `esperanza/benchbase/`: `BenchmarkSession` orchestrates BenchBase (runs benchbase.jar directly) and Ultraverse CLI invocations.
- For BenchBase workloads, set `KEY_COLUMNS`/`COLUMN_ALIASES` to match the DDL's PK/FK/aliases (same-table mapping). Represent composite keys with `+`.
- `esperanza/mysql/`: local MySQL daemon control utilities (`mysqld.py`).
- `esperanza/utils/`: shared helpers (MySQL downloads, logs, report parsing, etc.).
- `esperanza/tpcc/`: standalone TPC-C helpers (constants, random utilities, and initial data generator).
- `esperanza/epinions/`: Epinions helpers (constants, Zipfian generators, random utilities).
- `esperanza/epinions/data_generator.py`: Epinions initial data generator (users/items/reviews/trusts) using Zipfian distributions.
- `epinions_sql/`: Epinions DDL copies used for local schema loading.
- `esperanza/tpcc/session.py`: standalone TPC-C session runner (MySQL lifecycle, DDL/data load, workload execution, and Ultraverse CLI orchestration without BenchBase).
- `esperanza/epinions/session.py`: standalone Epinions session runner (MySQL lifecycle, DDL/data load, workload execution, and Ultraverse CLI orchestration without BenchBase).
- `esperanza/tpcc/transactions.py`: TPC-C stored-procedure transaction wrappers (explicit START TRANSACTION/CALL/COMMIT).
- `esperanza/tpcc/workload.py`: query-count-based TPC-C workload executor with weighted transaction selection and per-transaction warehouse choice.
- `esperanza/tpcc/constants.py`: standalone TPC-C weights are `NEW_ORDER=45`, `PAYMENT=43`, `DELIVERY=4`, with `ORDER_STATUS`/`STOCK_LEVEL` disabled (0).
- `esperanza/tpcc/transactions.py`: per-transaction warehouse behavior is simplified — NewOrder uses a 1% remote supply warehouse (if scale_factor > 1), Payment uses a ~15% remote customer warehouse, Delivery is local-only; Order-Status/Stock-Level are stubs.
- `procdefs/<bench>/`: procedure definition files (copied to `runs/.../procdef` at runtime).
- `mysql_conf/my.cnf`: MySQL configuration template.
- Artifacts: `runs/<bench>-<amount>-<timestamp>/` (logs, `*.report.json`, `dbdump*.sql`) and `cache/` (MySQL distribution cache).

## Build, Test, and Development Commands
- `source envfile`: Sets the `ULTRAVERSE_HOME`, `BENCHBASE_HOME`, `BENCHBASE_NODE_HOME`, `MYSQL_BIN_PATH` paths.
- BenchBase runs `benchbase.jar` directly instead of `run-mariadb`, so `BENCHBASE_HOME/target/benchbase-mariadb/benchbase.jar` must exist. (If needed, run `./make-mariadb` in the BenchBase repo.)
- `python tpcc.py` (or `python tatp.py`, `python seats.py`, `python epinions.py`, `python astore.py`, `python updateonly.py`): Runs the full session and stores results under `runs/`.
- Ultraverse binaries must exist under `ULTRAVERSE_HOME`; the scripts invoke `statelogd` and `db_state_change` internally.

## Coding Style & Naming Conventions
- Keep Python 4-space indentation, snake_case function/variable names, and lowercase module filenames.
- Keep workload-specific constants (key columns, aliases, comparison options) in each entry script, and move shared logic under `esperanza/`.

## Testing Guidelines
- There is no dedicated test runner in this directory. After making changes, run at least one workload and check `runs/*/*.report.json` and `dbdump_latest.sql`.
- If needed, enable the table-comparison logic in the scripts to validate reproducibility.
- `db_state_change` now executes `replaceQuery` automatically during replay, so BenchBase scripts should not run it manually; table diffs compare `benchbase` (post-replay) with the full-replay intermediate DB.
- For Esperanza unit tests/scripts, `cd` into this directory first and then run them.
- Before running tests, always run `source envfile` to set required environment variables such as `ULTRAVERSE_HOME`.
- For `state_log_viewer`, the `-i` argument must be the **log base name** (e.g., `benchbase`), not the `.ultstatelog` filename, to print correctly.
- `statelogd` output may include binary bytes, which can cause UTF-8 decoding failures when collecting logs; test scripts should ignore/replace decoding errors.
- `db_state_change` supports `--replay-from <gid>`: during replay, it executes the range `<gid>..(at least target GID-1)` in parallel using a separate RowGraph and then runs the existing replay plan. Ultraverse GIDs start at 0, but **pre-replay runs only when `--replay-from` is provided** (default is unset). Minishop와 BenchBase 스크립트는 rollback/replay에 `--replay-from 0`을 사용한다.
- Default timeout is set to 120 minutes.
- Run with elevated permissions.
- Use `python3` instead of `python`.

## Commit & Pull Request Guidelines
- Use the `scope/path: summary` format for commit messages. Example: `scripts/esperanza: add macOS support`.
- In PRs, include a summary of changes, the workloads/commands you ran, and any newly added config/artifact paths. Link related issues if any; screenshots are optional if there are no UI changes.

## Configuration & Runtime Notes
- `envfile` is a path template; adjust it for your local environment, but it is recommended not to commit personal paths.
- `download_mysql()` downloads a platform-specific MySQL distribution into `cache/`. It requires network access, and skips re-downloading if the cache exists.
- 벤치마크 스크립트는 `download_mysql()`이 확인한 MySQL 배포판의 `bin` 경로를 `MYSQL_BIN_PATH`에 자동 설정한다.
- `MySQLDaemon.start()` returns a context manager; use `with session.mysqld.start():` so the daemon always stops on scope exit (including exceptions).
- `esperanza/tpcc/data_generator.py` shows tqdm progress bars during data load when tqdm is installed (disabled if stderr is not a TTY).
- `esperanza/tpcc/workload.py` shows a tqdm progress bar for workload execution when tqdm is installed (falls back to periodic prints if stderr is not a TTY).
