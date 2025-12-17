import { gql } from '@urql/svelte';

export const REGISTER_SERVICE = gql`
  mutation RegisterService($input: RegisterServiceInput!) {
    registerService(input: $input) {
      serviceId
      serviceType
      endpoint
      healthStatus
    }
  }
`;

export const DEREGISTER_SERVICE = gql`
  mutation DeregisterService($serviceId: String!) {
    deregisterService(serviceId: $serviceId)
  }
`;

export const CREATE_PIPELINE = gql`
  mutation CreatePipeline($input: CreatePipelineInput!) {
    createPipeline(input: $input) {
      pipelineId
      name
      enabled
    }
  }
`;

export const ENABLE_PIPELINE = gql`
  mutation EnablePipeline($pipelineId: String!) {
    enablePipeline(pipelineId: $pipelineId) {
      pipelineId
      enabled
    }
  }
`;

export const DISABLE_PIPELINE = gql`
  mutation DisablePipeline($pipelineId: String!) {
    disablePipeline(pipelineId: $pipelineId) {
      pipelineId
      enabled
    }
  }
`;

export const DELETE_PIPELINE = gql`
  mutation DeletePipeline($pipelineId: String!) {
    deletePipeline(pipelineId: $pipelineId)
  }
`;

export const COMMIT_OFFSET = gql`
  mutation CommitOffset($sourceId: String!, $partitionId: Int!, $offset: Int!) {
    commitOffset(sourceId: $sourceId, partitionId: $partitionId, offset: $offset)
  }
`;

export const COMMIT_GROUP_OFFSET = gql`
  mutation CommitGroupOffset($groupId: String!, $sourceId: String!, $partitionId: Int!, $offset: Int!) {
    commitGroupOffset(groupId: $groupId, sourceId: $sourceId, partitionId: $partitionId, offset: $offset)
  }
`;

export const RESOLVE_ERROR = gql`
  mutation ResolveError($id: String!) {
    resolveError(id: $id) {
      success
      message
    }
  }
`;

export const CLEAR_RESOLVED_ERRORS = gql`
  mutation ClearResolvedErrors {
    clearResolvedErrors {
      success
      message
    }
  }
`;
