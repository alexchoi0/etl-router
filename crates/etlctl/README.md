# etlctl

Command-line interface for managing ETL Router resources.

## Overview

`etlctl` provides a kubectl-like experience for interacting with ETL Router clusters. Apply manifests, query resources, visualize pipelines, and manage backups.

## Installation

```bash
cargo install --path crates/etlctl
```

Or build from source:

```bash
cargo build -p etlctl --release
./target/release/etlctl --help
```

## Usage

```bash
etlctl [OPTIONS] <COMMAND>

Options:
  -s, --server <SERVER>       Router server address [default: http://localhost:8080]
  -n, --namespace <NAMESPACE> Namespace for resources [default: default]
  -h, --help                  Print help
  -V, --version               Print version
```

## Commands

### apply

Apply resource manifests from files or directories.

```bash
# Apply a single file
etlctl apply -f pipeline.yaml

# Apply all files in a directory
etlctl apply -f manifests/

# Apply with specific namespace
etlctl apply -f pipeline.yaml -n production
```

### get

List resources of a specific type.

```bash
# List pipelines
etlctl get pipelines

# List sources in all namespaces
etlctl get sources -A

# Output as JSON
etlctl get transforms -o json

# Output as YAML
etlctl get sinks -o yaml
```

### describe

Show detailed information about a resource.

```bash
etlctl describe pipeline user-analytics
etlctl describe source kafka-users
```

### delete

Remove a resource.

```bash
etlctl delete pipeline user-analytics
etlctl delete source kafka-users --force
```

### graph

Visualize pipeline DAG.

```bash
# ASCII output
etlctl graph -f pipelines/

# DOT format (for Graphviz)
etlctl graph -f pipelines/ --format dot > pipeline.dot
dot -Tpng pipeline.dot -o pipeline.png

# From server
etlctl graph --from-server
```

### validate

Validate manifests without applying.

```bash
etlctl validate -f pipeline.yaml
etlctl validate -f manifests/ --recursive
```

### backup

Manage cluster backups.

```bash
# Create a backup
etlctl backup create --dest s3://bucket/backups/
etlctl backup create --dest file:///var/backups/

# List backups
etlctl backup list --dest s3://bucket/backups/

# Describe a backup
etlctl backup describe abc123 --source s3://bucket/backups/

# Restore from backup
etlctl backup restore abc123 --source s3://bucket/backups/
etlctl backup restore abc123 --source s3://bucket/backups/ --validate-only

# Clean up old backups
etlctl backup cleanup --dest s3://bucket/backups/ --keep 10
```

## Manifest Format

```yaml
apiVersion: etl.router/v1
kind: Pipeline
metadata:
  name: user-analytics
  namespace: default
  labels:
    team: analytics
spec:
  source: kafka-users
  steps:
    - filter-active
    - enrich-geo
  sink: clickhouse-analytics
  dlq:
    enabled: true
    maxRetries: 3
```

## Output Formats

| Format | Flag | Description |
|--------|------|-------------|
| Table | `-o table` | Human-readable table (default) |
| YAML | `-o yaml` | YAML output |
| JSON | `-o json` | JSON output |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ETLCTL_SERVER` | Default server address |
| `ETLCTL_NAMESPACE` | Default namespace |
| `RUST_LOG` | Log level (debug, info, warn, error) |

## Examples

```bash
# Full workflow
etlctl apply -f sources/kafka.yaml
etlctl apply -f transforms/filter.yaml
etlctl apply -f sinks/s3.yaml
etlctl apply -f pipelines/analytics.yaml

etlctl get pipelines
etlctl describe pipeline analytics

# Backup before changes
etlctl backup create --dest s3://my-bucket/etl-backups/

# Make changes...

# Rollback if needed
etlctl backup restore <backup-id> --source s3://my-bucket/etl-backups/
```
