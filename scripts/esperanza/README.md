# Esperanza

Benchmark and test scripts for Ultraverse retroactive operation framework.

## Quick Start

```bash
cd scripts/esperanza
poetry install                    # Install Python dependencies (first time only)
vim envfile                       # Set ULTRAVERSE_HOME
source envfile
poetry run python tpcc_standalone.py
```

## Prerequisites

- Python 3.10+
- [Poetry](https://python-poetry.org/) (Python package manager)
- Ultraverse binaries (`statelogd`, `db_state_change`, `state_log_viewer`)

## Configuration

Edit `envfile` before running scripts:

| Variable | Required | Description |
|----------|----------|-------------|
| `ULTRAVERSE_HOME` | Yes | Path to directory containing Ultraverse binaries |
| `BENCHBASE_HOME` | BenchBase only | Path to ultraverse-benchbase |
| `BENCHBASE_NODE_HOME` | BenchBase only | Path to benchbase-nodejs |

## Available Scripts

### Standalone Scripts (No BenchBase Required)

| Script | Workload | Description |
|--------|----------|-------------|
| `tpcc_standalone.py` | TPC-C | Order processing benchmark |
| `epinions_standalone.py` | Epinions | Review/rating system benchmark |
| `tatp_standalone.py` | TATP | Telecom application benchmark |
| `seats_standalone.py` | SEATS | Airline reservation benchmark |
| `minishop.py` | Minishop | Integration test with stored procedures |

### BenchBase Scripts

| Script | Workload | Description |
|--------|----------|-------------|
| `tpcc.py` | TPC-C | BenchBase-driven TPC-C |
| `epinions.py` | Epinions | BenchBase-driven Epinions |
| `tatp.py` | TATP | BenchBase-driven TATP |
| `seats.py` | SEATS | BenchBase-driven SEATS |

## Directory Structure

```
scripts/esperanza/
├── *_standalone.py     # Standalone workload scripts
├── *.py                # BenchBase workload scripts
├── envfile             # Environment configuration
├── esperanza/          # Shared Python modules
├── procdefs/           # Stored procedure definitions
├── *_sql/              # SQL files (DDL, DML, etc.)
├── runs/               # Execution results (auto-generated)
└── cache/              # MySQL distribution cache (auto-generated)
```

## Output

Results are saved to `runs/<workload>-<timestamp>/`:

- `*.report.json` - Execution report (GID counts, replay stats)
- `dbdump*.sql` - Database dumps (before/after rollback)
- `statelogd.log`, `replay.log` - Process logs
- `*.ultstatelog`, `*.ultcluster`, etc. - Ultraverse state files

## Troubleshooting

- **Scripts fail immediately**: Make sure you ran `source envfile`
- **Permission errors**: Some operations may require `sudo`
- **Binary not found**: Verify `ULTRAVERSE_HOME` points to the correct build directory

For detailed implementation notes, see [AGENTS.md](AGENTS.md).
