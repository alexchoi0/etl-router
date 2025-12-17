<script lang="ts">
  import { queryStore, getContextClient, mutationStore } from '@urql/svelte';
  import { onMount } from 'svelte';
  import { ERROR_STATS, OPERATIONAL_ERRORS } from '$lib/graphql/queries';
  import { RESOLVE_ERROR, CLEAR_RESOLVED_ERRORS } from '$lib/graphql/mutations';
  import { Card, CardHeader, CardTitle, CardDescription, CardContent, Badge, Button } from '$lib/components/ui';
  import { AlertTriangle, Clock, CheckCircle, XCircle, Filter, RefreshCw, Trash2 } from 'lucide-svelte';

  const client = getContextClient();

  type ErrorType = 'PIPELINE_FAILURE' | 'TRANSFORM_ERROR' | 'SINK_ERROR' | 'SOURCE_ERROR' | 'DLQ_RECORD' | 'SERVICE_UNHEALTHY' | 'TIMEOUT' | 'VALIDATION_ERROR' | 'UNKNOWN';

  type OperationalError = {
    id: string;
    errorType: ErrorType;
    pipelineId: string | null;
    serviceId: string | null;
    stageId: string | null;
    message: string;
    details: string | null;
    timestamp: string;
    retryCount: number;
    resolved: boolean;
  };

  type ErrorFilter = {
    errorType?: ErrorType;
    pipelineId?: string;
    resolved?: boolean;
    limit?: number;
    offset?: number;
  };

  let filter = $state<ErrorFilter>({ limit: 50, offset: 0 });
  let showResolved = $state(false);
  let selectedErrorType = $state<ErrorType | ''>('');
  let selectedError = $state<OperationalError | null>(null);
  let lastUpdate = $state<Date | null>(null);
  let refreshInterval: ReturnType<typeof setInterval> | null = null;

  const statsQuery = queryStore({
    client,
    query: ERROR_STATS,
    requestPolicy: 'network-only',
  });

  const errorsQuery = queryStore({
    client,
    query: OPERATIONAL_ERRORS,
    variables: { filter },
    requestPolicy: 'network-only',
  });

  $effect(() => {
    const newFilter: ErrorFilter = {
      limit: 50,
      offset: 0,
      resolved: showResolved ? undefined : false,
      errorType: selectedErrorType || undefined,
    };
    filter = newFilter;
  });

  async function refresh() {
    await Promise.all([
      client.query(ERROR_STATS, {}, { requestPolicy: 'network-only' }).toPromise(),
      client.query(OPERATIONAL_ERRORS, { filter }, { requestPolicy: 'network-only' }).toPromise(),
    ]);
    statsQuery.reexecute({ requestPolicy: 'network-only' });
    errorsQuery.reexecute({ requestPolicy: 'network-only' });
    lastUpdate = new Date();
  }

  function startPolling() {
    if (!refreshInterval) {
      refresh();
      refreshInterval = setInterval(refresh, 1000);
    }
  }

  function stopPolling() {
    if (refreshInterval) {
      clearInterval(refreshInterval);
      refreshInterval = null;
    }
  }

  function handleVisibilityChange() {
    if (document.hidden) {
      stopPolling();
    } else {
      startPolling();
    }
  }

  onMount(() => {
    startPolling();
    document.addEventListener('visibilitychange', handleVisibilityChange);
    return () => {
      stopPolling();
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  });

  async function resolveError(id: string) {
    await client.mutation(RESOLVE_ERROR, { id }).toPromise();
    refresh();
  }

  async function clearResolvedErrors() {
    await client.mutation(CLEAR_RESOLVED_ERRORS, {}).toPromise();
    refresh();
  }

  function getErrorTypeLabel(errorType: ErrorType): string {
    const labels: Record<ErrorType, string> = {
      PIPELINE_FAILURE: 'Pipeline Failure',
      TRANSFORM_ERROR: 'Transform Error',
      SINK_ERROR: 'Sink Error',
      SOURCE_ERROR: 'Source Error',
      DLQ_RECORD: 'DLQ Record',
      SERVICE_UNHEALTHY: 'Service Unhealthy',
      TIMEOUT: 'Timeout',
      VALIDATION_ERROR: 'Validation Error',
      UNKNOWN: 'Unknown',
    };
    return labels[errorType] ?? errorType;
  }

  function getErrorTypeBadgeVariant(errorType: ErrorType): 'default' | 'secondary' | 'destructive' | 'success' {
    switch (errorType) {
      case 'PIPELINE_FAILURE':
      case 'SINK_ERROR':
      case 'SOURCE_ERROR':
        return 'destructive';
      case 'TRANSFORM_ERROR':
      case 'VALIDATION_ERROR':
        return 'default';
      case 'DLQ_RECORD':
      case 'SERVICE_UNHEALTHY':
      case 'TIMEOUT':
        return 'secondary';
      default:
        return 'default';
    }
  }

  function formatTimestamp(timestamp: string): string {
    return new Date(timestamp).toLocaleString();
  }

  function formatRelativeTime(timestamp: string): string {
    const diff = Date.now() - new Date(timestamp).getTime();
    const seconds = Math.floor(diff / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    const days = Math.floor(hours / 24);

    if (days > 0) return `${days}d ago`;
    if (hours > 0) return `${hours}h ago`;
    if (minutes > 0) return `${minutes}m ago`;
    return `${seconds}s ago`;
  }

  const errorTypes: ErrorType[] = [
    'PIPELINE_FAILURE',
    'TRANSFORM_ERROR',
    'SINK_ERROR',
    'SOURCE_ERROR',
    'DLQ_RECORD',
    'SERVICE_UNHEALTHY',
    'TIMEOUT',
    'VALIDATION_ERROR',
    'UNKNOWN',
  ];
</script>

<div class="space-y-8">
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-3xl font-bold tracking-tight">Operational Errors</h1>
      <p class="text-muted-foreground">Monitor pipeline failures, DLQ records, and service errors</p>
    </div>
    <div class="flex items-center gap-4">
      {#if lastUpdate}
        <div class="flex items-center gap-2 text-sm text-muted-foreground">
          <Clock class="h-4 w-4" />
          Last updated: {lastUpdate.toLocaleTimeString()}
        </div>
      {/if}
      <Button variant="outline" size="sm" onclick={() => refresh()}>
        <RefreshCw class="h-4 w-4 mr-2" />
        Refresh
      </Button>
    </div>
  </div>

  <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Total Errors</CardTitle>
        <AlertTriangle class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $statsQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $statsQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold">{$statsQuery.data?.errorStats?.totalErrors ?? 0}</div>
          <p class="text-xs text-muted-foreground">All time</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Unresolved</CardTitle>
        <XCircle class="h-4 w-4 text-destructive" />
      </CardHeader>
      <CardContent>
        {#if $statsQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $statsQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold text-destructive">
            {$statsQuery.data?.errorStats?.unresolvedCount ?? 0}
          </div>
          <p class="text-xs text-muted-foreground">Needs attention</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Last Hour</CardTitle>
        <Clock class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $statsQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $statsQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold">{$statsQuery.data?.errorStats?.errorsLastHour ?? 0}</div>
          <p class="text-xs text-muted-foreground">Recent errors</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Error Types</CardTitle>
        <Filter class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $statsQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $statsQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold">
            {$statsQuery.data?.errorStats?.errorsByType?.length ?? 0}
          </div>
          <p class="text-xs text-muted-foreground">Different types</p>
        {/if}
      </CardContent>
    </Card>
  </div>

  {#if $statsQuery.data?.errorStats?.errorsByType?.length > 0}
    <Card>
      <CardHeader>
        <CardTitle>Errors by Type</CardTitle>
        <CardDescription>Distribution of errors across different categories</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="flex flex-wrap gap-3">
          {#each $statsQuery.data.errorStats.errorsByType as typeCount}
            <div class="flex items-center gap-2 rounded-lg border px-3 py-2">
              <Badge variant={getErrorTypeBadgeVariant(typeCount.errorType)}>
                {getErrorTypeLabel(typeCount.errorType)}
              </Badge>
              <span class="font-mono text-sm font-medium">{typeCount.count}</span>
            </div>
          {/each}
        </div>
      </CardContent>
    </Card>
  {/if}

  <Card>
    <CardHeader>
      <div class="flex items-center justify-between">
        <div>
          <CardTitle>Error Log</CardTitle>
          <CardDescription>Recent operational errors with details</CardDescription>
        </div>
        <div class="flex items-center gap-4">
          <div class="flex items-center gap-2">
            <label class="text-sm text-muted-foreground">Type:</label>
            <select
              bind:value={selectedErrorType}
              class="rounded-md border bg-background px-3 py-1.5 text-sm"
            >
              <option value="">All Types</option>
              {#each errorTypes as errorType}
                <option value={errorType}>{getErrorTypeLabel(errorType)}</option>
              {/each}
            </select>
          </div>
          <label class="flex items-center gap-2 text-sm">
            <input type="checkbox" bind:checked={showResolved} class="rounded" />
            Show Resolved
          </label>
          {#if showResolved}
            <Button variant="outline" size="sm" onclick={() => clearResolvedErrors()}>
              <Trash2 class="h-4 w-4 mr-2" />
              Clear Resolved
            </Button>
          {/if}
        </div>
      </div>
    </CardHeader>
    <CardContent>
      {#if $errorsQuery.fetching && !$errorsQuery.data}
        <p class="text-muted-foreground">Loading...</p>
      {:else if $errorsQuery.error}
        <p class="text-destructive">Failed to load errors: {$errorsQuery.error.message}</p>
      {:else if !$errorsQuery.data?.operationalErrors?.length}
        <div class="flex flex-col items-center justify-center py-12 text-center">
          <CheckCircle class="h-12 w-12 text-green-500 mb-4" />
          <p class="text-lg font-medium">No errors found</p>
          <p class="text-sm text-muted-foreground">
            {showResolved ? 'No errors match your filters' : 'All errors have been resolved'}
          </p>
        </div>
      {:else}
        <div class="space-y-4">
          {#each $errorsQuery.data.operationalErrors as error}
            <div
              class="rounded-lg border p-4 transition-colors hover:bg-accent/50 cursor-pointer"
              class:opacity-60={error.resolved}
              onclick={() => selectedError = selectedError?.id === error.id ? null : error}
            >
              <div class="flex items-start justify-between gap-4">
                <div class="flex-1 min-w-0">
                  <div class="flex items-center gap-2 mb-1">
                    <Badge variant={getErrorTypeBadgeVariant(error.errorType)}>
                      {getErrorTypeLabel(error.errorType)}
                    </Badge>
                    {#if error.resolved}
                      <Badge variant="success">Resolved</Badge>
                    {/if}
                    {#if error.retryCount > 0}
                      <Badge variant="secondary">Retried {error.retryCount}x</Badge>
                    {/if}
                  </div>
                  <p class="text-sm font-medium truncate">{error.message}</p>
                  <div class="flex items-center gap-4 mt-1 text-xs text-muted-foreground">
                    <span title={formatTimestamp(error.timestamp)}>
                      {formatRelativeTime(error.timestamp)}
                    </span>
                    {#if error.pipelineId}
                      <span>Pipeline: {error.pipelineId}</span>
                    {/if}
                    {#if error.serviceId}
                      <span>Service: {error.serviceId}</span>
                    {/if}
                    {#if error.stageId}
                      <span>Stage: {error.stageId}</span>
                    {/if}
                  </div>
                </div>
                {#if !error.resolved}
                  <Button
                    variant="outline"
                    size="sm"
                    onclick={(e: Event) => { e.stopPropagation(); resolveError(error.id); }}
                  >
                    <CheckCircle class="h-4 w-4 mr-1" />
                    Resolve
                  </Button>
                {/if}
              </div>

              {#if selectedError?.id === error.id && error.details}
                <div class="mt-4 pt-4 border-t">
                  <p class="text-xs font-medium text-muted-foreground mb-2">Details</p>
                  <pre class="text-xs bg-muted rounded p-3 overflow-x-auto whitespace-pre-wrap">{error.details}</pre>
                </div>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
    </CardContent>
  </Card>
</div>
