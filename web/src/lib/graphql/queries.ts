import { gql } from '@urql/svelte';

export const CLUSTER_STATUS = gql`
  query ClusterStatus {
    clusterStatus {
      nodeId
      role
      leader
      term
      commitIndex
      lastApplied
      memberCount
    }
  }
`;

export const SERVICES = gql`
  query Services {
    services {
      serviceId
      serviceType
      endpoint
      healthStatus
      metadata
      registeredAt
      lastHeartbeat
    }
  }
`;

export const SERVICE = gql`
  query Service($serviceId: String!) {
    service(serviceId: $serviceId) {
      serviceId
      serviceType
      endpoint
      healthStatus
      metadata
      registeredAt
      lastHeartbeat
    }
  }
`;

export const PIPELINES = gql`
  query Pipelines {
    pipelines {
      pipelineId
      name
      enabled
      sourceId
      sinkId
      processors
      createdAt
      updatedAt
    }
  }
`;

export const PIPELINE = gql`
  query Pipeline($pipelineId: String!) {
    pipeline(pipelineId: $pipelineId) {
      pipelineId
      name
      enabled
      sourceId
      sinkId
      processors
      createdAt
      updatedAt
    }
  }
`;

export const CONSUMER_GROUPS = gql`
  query ConsumerGroups {
    consumerGroups {
      groupId
      members
      offsets
    }
  }
`;

export const SOURCE_OFFSETS = gql`
  query SourceOffsets($sourceId: String!) {
    sourceOffsets(sourceId: $sourceId) {
      sourceId
      partitions {
        partitionId
        offset
        lag
      }
    }
  }
`;

export const WATERMARKS = gql`
  query Watermarks($sourceId: String!) {
    watermarks(sourceId: $sourceId) {
      sourceId
      partition
      position
      eventTime
    }
  }
`;

export const GROUP_OFFSETS = gql`
  query GroupOffsets($groupId: String!, $sourceId: String) {
    groupOffsets(groupId: $groupId, sourceId: $sourceId) {
      groupId
      sourceId
      partitions {
        partition
        offset
      }
    }
  }
`;

export const SERVICE_CHECKPOINT = gql`
  query ServiceCheckpoint($serviceId: String!) {
    serviceCheckpoint(serviceId: $serviceId) {
      serviceId
      checkpointId
      sourceOffsets {
        sourceId
        offset
      }
      createdAt
    }
  }
`;

export const METRICS_OVERVIEW = gql`
  query MetricsOverview {
    clusterStatus {
      nodeId
      role
      leader
      term
      commitIndex
      lastApplied
      memberCount
    }
    services {
      serviceId
      serviceName
      serviceType
      endpoint
      health
      groupId
      registeredAt
      lastHeartbeat
    }
    pipelines {
      pipelineId
      name
      enabled
      version
    }
    consumerGroups {
      groupId
      stageId
      members
      generation
      partitionAssignments {
        memberId
        partitions
      }
    }
  }
`;

export const ERROR_STATS = gql`
  query ErrorStats {
    errorStats {
      totalErrors
      unresolvedCount
      errorsLastHour
      errorsByType {
        errorType
        count
      }
    }
  }
`;

export const OPERATIONAL_ERRORS = gql`
  query OperationalErrors($filter: ErrorFilter) {
    operationalErrors(filter: $filter) {
      id
      errorType
      pipelineId
      serviceId
      stageId
      message
      details
      timestamp
      retryCount
      resolved
    }
  }
`;

export const OPERATIONAL_ERROR = gql`
  query OperationalError($id: String!) {
    operationalError(id: $id) {
      id
      errorType
      pipelineId
      serviceId
      stageId
      message
      details
      timestamp
      retryCount
      resolved
    }
  }
`;
