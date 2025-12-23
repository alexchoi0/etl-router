# conveyor-etl-cli

Command-line interface for managing Conveyor ETL resources.

## Overview

`conveyor-etl-cli` provides a kubectl-like experience for interacting with Conveyor ETL clusters. Apply manifests, query resources, visualize pipelines, and manage backups.

## Installation

```bash
cargo install --path crates/conveyor-etl-cli
```

Or build from source:

```bash
cargo build -p conveyor-etl-cli --release
./target/release/conveyor-etl-cli --help
```

## Usage

```bash
conveyor-etl-cli [OPTIONS] <COMMAND>

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
conveyor-etl-cli apply -f pipeline.yaml

# Apply all files in a directory
conveyor-etl-cli apply -f manifests/

# Apply with specific namespace
conveyor-etl-cli apply -f pipeline.yaml -n production
```

### get

List resources of a specific type.

```bash
# List pipelines
conveyor-etl-cli get pipelines

# List sources in all namespaces
conveyor-etl-cli get sources -A

# Output as JSON
conveyor-etl-cli get transforms -o json

# Output as YAML
conveyor-etl-cli get sinks -o yaml
```

### describe

Show detailed information about a resource.

```bash
conveyor-etl-cli describe pipeline user-analytics
conveyor-etl-cli describe source kafka-users
```

### delete

Remove a resource.

```bash
conveyor-etl-cli delete pipeline user-analytics
conveyor-etl-cli delete source kafka-users --force
```

### graph

Visualize pipeline DAG.

```bash
# ASCII output
conveyor-etl-cli graph -f pipelines/

# DOT format (for Graphviz)
conveyor-etl-cli graph -f pipelines/ --format dot > pipeline.dot
dot -Tpng pipeline.dot -o pipeline.png

# From server
conveyor-etl-cli graph --from-server
```

### validate

Validate manifests without applying.

```bash
conveyor-etl-cli validate -f pipeline.yaml
conveyor-etl-cli validate -f manifests/ --recursive
```

### backup

Manage cluster backups.

```bash
# Create a backup
conveyor-etl-cli backup create --dest s3://bucket/backups/
conveyor-etl-cli backup create --dest file:///var/backups/

# List backups
conveyor-etl-cli backup list --dest s3://bucket/backups/

# Describe a backup
conveyor-etl-cli backup describe abc123 --source s3://bucket/backups/

# Restore from backup
conveyor-etl-cli backup restore abc123 --source s3://bucket/backups/
conveyor-etl-cli backup restore abc123 --source s3://bucket/backups/ --validate-only

# Clean up old backups
conveyor-etl-cli backup cleanup --dest s3://bucket/backups/ --keep 10
```

## Manifest Format

```yaml
apiVersion: conveyor.etl/v1
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
| `CONVEYOR_ETL_CLI_SERVER` | Default server address |
| `CONVEYOR_ETL_CLI_NAMESPACE` | Default namespace |
| `RUST_LOG` | Log level (debug, info, warn, error) |

## Examples

```bash
# Full workflow
conveyor-etl-cli apply -f sources/kafka.yaml
conveyor-etl-cli apply -f transforms/filter.yaml
conveyor-etl-cli apply -f sinks/s3.yaml
conveyor-etl-cli apply -f pipelines/analytics.yaml

conveyor-etl-cli get pipelines
conveyor-etl-cli describe pipeline analytics

# Backup before changes
conveyor-etl-cli backup create --dest s3://my-bucket/etl-backups/

# Make changes...

# Rollback if needed
conveyor-etl-cli backup restore <backup-id> --source s3://my-bucket/etl-backups/
```
