# Repository Guidelines

## Project Structure & Module Organization
- Entry scripts: `tpcc.py`, `tatp.py`, `seats.py`, `epinions.py`, `astore.py`, `updateonly.py`, `minishop.py`, `minishop_prepend.py` run each workload and perform the prepare/execute (or scenario execution), statelog generation, and rollback/replay flow end-to-end.
- `minishop_prepend.py` validates a rollback + prepend (partial refund correction) case based on the Minishop scenario.
- `minishop.py` patches procedure definitions via `procpatcher` and applies them to MySQL; it also generates `procpatcher/__ultraverse__helper.sql` under the session path.
- `procpatcher` has its own `go.mod` under `procpatcher/`, so run it from that directory with `go run .`.
- `esperanza/benchbase/`: `BenchmarkSession` orchestrates BenchBase (runs benchbase.jar directly) and Ultraverse CLI invocations.
- For BenchBase workloads, set `KEY_COLUMNS`/`COLUMN_ALIASES` to match the DDL's PK/FK/aliases (same-table mapping). Represent composite keys with `+`.
- `esperanza/mysql/`: local MySQL daemon control utilities (`mysqld.py`).
- `esperanza/utils/`: shared helpers (MySQL downloads, logs, report parsing, etc.).
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
- For Esperanza unit tests/scripts, `cd` into this directory first and then run them.
- Before running tests, always run `source envfile` to set required environment variables such as `ULTRAVERSE_HOME`.
- For `state_log_viewer`, the `-i` argument must be the **log base name** (e.g., `benchbase`), not the `.ultstatelog` filename, to print correctly.
- `statelogd` output may include binary bytes, which can cause UTF-8 decoding failures when collecting logs; test scripts should ignore/replace decoding errors.
- `db_state_change` supports `--replay-from <gid>`: during replay, it executes the range `<gid>..(at least target GID-1)` in parallel using a separate RowGraph and then runs the existing replay plan. Ultraverse GIDs start at 0, so the default is `replay_from=0`, and the Minishop scenario also uses 0.
- Default timeout is set to 120 minutes.
- Run with elevated permissions.
- Use `python3` instead of `python`.

## Commit & Pull Request Guidelines
- Use the `scope/path: summary` format for commit messages. Example: `scripts/esperanza: add macOS support`.
- In PRs, include a summary of changes, the workloads/commands you ran, and any newly added config/artifact paths. Link related issues if any; screenshots are optional if there are no UI changes.

## Configuration & Runtime Notes
- `envfile` is a path template; adjust it for your local environment, but it is recommended not to commit personal paths.
- `download_mysql()` downloads a platform-specific MySQL distribution into `cache/`. It requires network access, and skips re-downloading if the cache exists.
