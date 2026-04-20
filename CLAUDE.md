# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a gRPC adapter that bridges ASCOW/ASCOW Pro HPC management systems with Slurm job scheduler. It translates ASCOW API calls into Slurm native operations (CLI commands + direct MySQL queries to `slurm_acct_db`).

## Commands

```bash
# Generate protobuf code from external interface repo
make protos

# Build binary (default amd64, override with ARCH=arm64)
make build

# Run locally
make run

# Run tests
go test ./...

# Run tests in a specific package
go test ./user/
go test ./job/
```

**Build requirements**: Go 1.20+, Buf CLI, Git >= 2.18.0

## Architecture

### Entry Point & Global State

`main.go` is a ~4400-line monolithic file containing all 6 gRPC service implementations. Global state is initialized once at startup:
- `configValue` — parsed `config/config.yaml` via Viper
- `db` — MySQL connection to Slurm accounting database
- `logger` — logrus with JSON format, rotating file + stdout

### gRPC Services (all in `main.go`)

| Struct | Service | Responsibility |
|--------|---------|----------------|
| `serverUser` | UserService | User↔account associations |
| `serverAccount` | AccountService | Account CRUD, block/unblock |
| `serverConfig` | ConfigService | Partition/QoS/cluster info |
| `serverJob` | JobService | Job submit/cancel/monitor |
| `serverVersion` | VersionService | Version metadata |
| `serverApp` | AppService | Connection info |

### Slurm Integration

Two integration paths:
1. **CLI**: Shell execution via `utils.RunCommand()` / `utils.ExecuteShellCommand()` — calls `sacctmgr`, `scontrol`, `sbatch`, `squeue`, `scancel`
2. **Database**: Direct SQL queries against `slurm_acct_db` MySQL database for job history and accounting data

The `$PATH` is extended at startup to include Slurm binary directories.

### Utilities (`utils/utils.go`)

Key helpers: config parsing, shell execution with exit codes, DB connectivity, job state translation (int → Slurm state), LDAP user validation, account/user name validation (rejects uppercase).

### Protobuf / Code Generation

Proto definitions live in external repo `github.com/abhpc/ascow-scheduler-adapter-interface`. Running `make protos` clones that repo and regenerates Go code via Buf. Do not manually edit generated files.

### Configuration (`config/config.yaml`)

```yaml
mysql:        # Slurm accounting DB connection
service:      # gRPC port (default 8972)
slurm:        # Slurm binary paths, cluster name
modulepath:   # Environment module path for software tracking
```

### Error Handling Pattern

Services return gRPC `status.Error` with `errdetails.ErrorInfo` containing string reason codes (e.g., `"ACCOUNT_NOT_FOUND"`, `"SQL_QUERY_FAILED"`). Use `codes.NotFound`, `codes.Internal`, `codes.PermissionDenied` as appropriate.

### Test Structure

Tests are organized by service domain in subdirectories: `account/`, `config/`, `job/`, `user/`, `version/`. Uses testify assertions.
