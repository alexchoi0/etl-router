# conveyorctl

Command-line interface for managing Conveyor resources.

## Overview

`conveyorctl` provides a kubectl-like experience for interacting with Conveyor clusters. Apply manifests, query resources, visualize pipelines, and manage backups.

## Installation

```bash
cargo install --path crates/conveyorctl
```

Or build from source:

```bash
cargo build -p conveyorctl --release
./target/release/conveyorctl --help
```

## Usage

```bash
conveyorctl [OPTIONS] <COMMAND>

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
conveyorctl apply -f pipeline.yaml

# Apply all files in a directory
conveyorctl apply -f manifests/

# Apply with specific namespace
conveyorctl apply -f pipeline.yaml -n production
```

### get

List resources of a specific type.

```bash
# List pipelines
conveyorctl get pipelines

# List sources in all namespaces
conveyorctl get sources -A

# Output as JSON
conveyorctl get transforms -o json

# Output as YAML
conveyorctl get sinks -o yaml
```

### describe

Show detailed information about a resource.

```bash
conveyorctl describe pipeline user-analytics
conveyorctl describe source kafka-users
```

### delete

Remove a resource.

```bash
conveyorctl delete pipeline user-analytics
conveyorctl delete source kafka-users --force
```

### graph

Visualize pipeline DAG.

```bash
# ASCII output
conveyorctl graph -f pipelines/

# DOT format (for Graphviz)
conveyorctl graph -f pipelines/ --format dot > pipeline.dot
dot -Tpng pipeline.dot -o pipeline.png

# From server
conveyorctl graph --from-server
```

### validate

Validate manifests without applying.

```bash
conveyorctl validate -f pipeline.yaml
conveyorctl validate -f manifests/ --recursive
```

### backup

Manage cluster backups.

```bash
# Create a backup
conveyorctl backup create --dest s3://bucket/backups/
conveyorctl backup create --dest file:///var/backups/

# List backups
conveyorctl backup list --dest s3://bucket/backups/

# Describe a backup
conveyorctl backup describe abc123 --source s3://bucket/backups/

# Restore from backup
conveyorctl backup restore abc123 --source s3://bucket/backups/
conveyorctl backup restore abc123 --source s3://bucket/backups/ --validate-only

# Clean up old backups
conveyorctl backup cleanup --dest s3://bucket/backups/ --keep 10
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
| `CONVEYORCTL_SERVER` | Default server address |
| `CONVEYORCTL_NAMESPACE` | Default namespace |
| `RUST_LOG` | Log level (debug, info, warn, error) |

## Examples

```bash
# Full workflow
conveyorctl apply -f sources/kafka.yaml
conveyorctl apply -f transforms/filter.yaml
conveyorctl apply -f sinks/s3.yaml
conveyorctl apply -f pipelines/analytics.yaml

conveyorctl get pipelines
conveyorctl describe pipeline analytics

# Backup before changes
conveyorctl backup create --dest s3://my-bucket/etl-backups/

# Make changes...

# Rollback if needed
conveyorctl backup restore <backup-id> --source s3://my-bucket/etl-backups/
```
