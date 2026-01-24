# procpatcher

`procpatcher` patches MySQL stored procedure source so that **procedure call hints are recorded in the binary log**.
With ROW-format binlogs, procedure call information is not directly available, so we inject a hint `INSERT` into the
procedure body as a workaround.

## How it works

- Parse the input SQL file and find `CREATE PROCEDURE` statements.
- Insert `INSERT INTO __ULTRAVERSE_PROCEDURE_HINT ...` right before **early-exit points**
  (e.g., `LEAVE <proc_label>`, `SIGNAL`, `RETURN`).
- Insert once more right before the procedure **final END**.
- At each insertion point, dump only the **parameters and local variables that are visible** at that scope into JSON.
- Keep existing indentation/newlines/`DELIMITER` directives **as much as possible** by inserting into the original text.

> Note: `LEAVE` is treated as a procedure early-exit only when it targets the **procedure's outer label**.
> `LEAVE` used to exit loops is intentionally ignored.

## Hint table schema

To ensure hint inserts appear in the binlog without actually storing data, `procpatcher` uses a `BLACKHOLE` table.
It also generates `__ultraverse__helper.sql` alongside the patched output.

```sql
CREATE TABLE IF NOT EXISTS __ULTRAVERSE_PROCEDURE_HINT (
    callid BIGINT UNSIGNED NOT NULL,
    procname VARCHAR(255) NOT NULL,
    args VARCHAR(4096),
    vars VARCHAR(4096),
    PRIMARY KEY (callid)
) ENGINE = BLACKHOLE;
```

## Usage

```bash
# Print patched SQL to stdout
go run ./procpatcher <input.sql>

# Write patched SQL to a file
go run ./procpatcher <input.sql> <output.sql>

# Remove hint inserts (depatch) and print to stdout
go run ./procpatcher --depatch <input.sql>

# Remove hint inserts (depatch) and write to a file
go run ./procpatcher --depatch <input.sql> <output.sql>

# Repatch legacy hints (depatch + patch) and print to stdout
go run ./procpatcher --repatch <input.sql>

# Repatch legacy hints (depatch + patch) and write to a file
go run ./procpatcher --repatch <input.sql> <output.sql>
```

- If an output path is provided, `__ultraverse__helper.sql` is created in the **same directory** as the output file.
- If no output path is provided (stdout), `__ultraverse__helper.sql` is created in the **current working directory**.
  - In `--depatch` mode, helper SQL is **not** generated. `--repatch` follows patch behavior.

### Example

```bash
go run ./procpatcher ./procpatcher/test-cases/lucky_chance.sql /tmp/lucky_patched.sql
```

## Idempotency (re-patching protection)

If a procedure already contains `__ULTRAVERSE_PROCEDURE_HINT`, `procpatcher` skips patching that procedure and prints a
warning to stderr.

## Depatch behavior

- Removes `INSERT INTO __ULTRAVERSE_PROCEDURE_HINT ...` statements added by `procpatcher`.
- Removes legacy `callinfo`-based hints:
  - `DECLARE __ultraverse_callinfo ...;`
  - `INSERT INTO __ULTRAVERSE_PROCEDURE_HINT (callinfo) VALUES (__ultraverse_callinfo);`
- Leaves commented-out `--INSERT ...` lines intact.

## Repatch behavior

- Runs depatch first, then applies the latest patch format.
- Useful for migrating legacy `callinfo` hints to `callid/procname/args/vars`.

## Notes / Limitations

- tidb/parser is MySQL-compatible, but it does **not** parse MySQL CLI `DELIMITER` directives.
  `procpatcher` detects `DELIMITER` lines while splitting statements, and applies patches by inserting into the original
  text to preserve formatting.
- If a statement cannot be parsed, it is left **unchanged** and a warning is printed.
