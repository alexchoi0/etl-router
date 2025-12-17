<script lang="ts">
  import { queryStore, getContextClient } from '@urql/svelte';
  import { CLUSTER_STATUS, SERVICES, PIPELINES, ERROR_STATS } from '$lib/graphql/queries';
  import { Card, CardHeader, CardTitle, CardDescription, CardContent, Badge } from '$lib/components/ui';
  import { Server, GitBranch, Activity, Database, AlertTriangle } from 'lucide-svelte';

  const client = getContextClient();

  const clusterQuery = queryStore({ client, query: CLUSTER_STATUS });
  const servicesQuery = queryStore({ client, query: SERVICES });
  const pipelinesQuery = queryStore({ client, query: PIPELINES });
  const errorStatsQuery = queryStore({ client, query: ERROR_STATS });

  function getRoleBadgeVariant(role: string): 'default' | 'secondary' | 'success' {
    switch (role?.toLowerCase()) {
      case 'leader':
        return 'success';
      case 'follower':
        return 'secondary';
      default:
        return 'default';
    }
  }
</script>

<div class="space-y-8">
  <div>
    <h1 class="text-3xl font-bold tracking-tight">Dashboard</h1>
    <p class="text-muted-foreground">Overview of your ETL Router cluster</p>
  </div>

  <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Cluster Status</CardTitle>
        <Activity class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $clusterQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $clusterQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else if $clusterQuery.data?.clusterStatus}
          <div class="flex items-center gap-2">
            <div class="text-2xl font-bold">{$clusterQuery.data.clusterStatus.role}</div>
            <Badge variant={getRoleBadgeVariant($clusterQuery.data.clusterStatus.role)}>
              Term {$clusterQuery.data.clusterStatus.term}
            </Badge>
          </div>
          <p class="text-xs text-muted-foreground">
            Commit: {$clusterQuery.data.clusterStatus.commitIndex}
          </p>
        {:else}
          <div class="text-2xl font-bold text-muted-foreground">Disconnected</div>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Services</CardTitle>
        <Server class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $servicesQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $servicesQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold">{$servicesQuery.data?.services?.length ?? 0}</div>
          <p class="text-xs text-muted-foreground">Registered services</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Pipelines</CardTitle>
        <GitBranch class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $pipelinesQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $pipelinesQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          {@const pipelines = $pipelinesQuery.data?.pipelines ?? []}
          {@const enabled = pipelines.filter((p: { enabled: boolean }) => p.enabled).length}
          <div class="text-2xl font-bold">{pipelines.length}</div>
          <p class="text-xs text-muted-foreground">{enabled} enabled</p>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Members</CardTitle>
        <Database class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        {#if $clusterQuery.fetching}
          <div class="text-2xl font-bold">Loading...</div>
        {:else if $clusterQuery.error}
          <div class="text-2xl font-bold text-destructive">Error</div>
        {:else}
          <div class="text-2xl font-bold">{$clusterQuery.data?.clusterStatus?.memberCount ?? 0}</div>
          <p class="text-xs text-muted-foreground">Cluster nodes</p>
        {/if}
      </CardContent>
    </Card>
  </div>

  <Card>
    <CardHeader class="flex flex-row items-center justify-between">
      <div>
        <CardTitle>Operational Errors</CardTitle>
        <CardDescription>Recent errors requiring attention</CardDescription>
      </div>
      <a href="/errors" class="text-sm text-primary hover:underline">View all →</a>
    </CardHeader>
    <CardContent>
      {#if $errorStatsQuery.fetching}
        <p class="text-muted-foreground">Loading...</p>
      {:else if $errorStatsQuery.error}
        <p class="text-destructive">Failed to load error stats</p>
      {:else}
        {@const stats = $errorStatsQuery.data?.errorStats}
        <div class="grid gap-4 md:grid-cols-3">
          <div class="flex items-center gap-3">
            <div class="flex h-10 w-10 items-center justify-center rounded-full bg-destructive/10">
              <AlertTriangle class="h-5 w-5 text-destructive" />
            </div>
            <div>
              <p class="text-2xl font-bold text-destructive">{stats?.unresolvedCount ?? 0}</p>
              <p class="text-xs text-muted-foreground">Unresolved</p>
            </div>
          </div>
          <div class="flex items-center gap-3">
            <div class="flex h-10 w-10 items-center justify-center rounded-full bg-secondary">
              <Activity class="h-5 w-5 text-muted-foreground" />
            </div>
            <div>
              <p class="text-2xl font-bold">{stats?.errorsLastHour ?? 0}</p>
              <p class="text-xs text-muted-foreground">Last hour</p>
            </div>
          </div>
          <div class="flex items-center gap-3">
            <div class="flex h-10 w-10 items-center justify-center rounded-full bg-secondary">
              <Database class="h-5 w-5 text-muted-foreground" />
            </div>
            <div>
              <p class="text-2xl font-bold">{stats?.totalErrors ?? 0}</p>
              <p class="text-xs text-muted-foreground">Total</p>
            </div>
          </div>
        </div>
        {#if stats?.errorsByType?.length > 0}
          <div class="mt-4 pt-4 border-t">
            <p class="text-sm text-muted-foreground mb-2">By Type</p>
            <div class="flex flex-wrap gap-2">
              {#each stats.errorsByType as typeCount}
                <Badge variant="secondary" class="font-mono">
                  {typeCount.errorType}: {typeCount.count}
                </Badge>
              {/each}
            </div>
          </div>
        {/if}
      {/if}
    </CardContent>
  </Card>

  <div class="grid gap-4 md:grid-cols-2">
    <Card>
      <CardHeader>
        <CardTitle>Recent Services</CardTitle>
        <CardDescription>Latest registered services</CardDescription>
      </CardHeader>
      <CardContent>
        {#if $servicesQuery.fetching}
          <p class="text-muted-foreground">Loading...</p>
        {:else if $servicesQuery.error}
          <p class="text-destructive">Failed to load services</p>
        {:else if !$servicesQuery.data?.services?.length}
          <p class="text-muted-foreground">No services registered</p>
        {:else}
          <div class="space-y-4">
            {#each $servicesQuery.data.services.slice(0, 5) as service}
              <div class="flex items-center gap-4">
                <div class="flex h-9 w-9 items-center justify-center rounded-full bg-secondary">
                  <Server class="h-4 w-4" />
                </div>
                <div class="flex-1 space-y-1">
                  <p class="text-sm font-medium leading-none">{service.serviceId}</p>
                  <p class="text-sm text-muted-foreground">{service.endpoint}</p>
                </div>
                <Badge variant={service.healthStatus === 'healthy' ? 'success' : 'destructive'}>
                  {service.healthStatus}
                </Badge>
              </div>
            {/each}
          </div>
        {/if}
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Active Pipelines</CardTitle>
        <CardDescription>Currently enabled pipelines</CardDescription>
      </CardHeader>
      <CardContent>
        {#if $pipelinesQuery.fetching}
          <p class="text-muted-foreground">Loading...</p>
        {:else if $pipelinesQuery.error}
          <p class="text-destructive">Failed to load pipelines</p>
        {:else if !$pipelinesQuery.data?.pipelines?.length}
          <p class="text-muted-foreground">No pipelines configured</p>
        {:else}
          <div class="space-y-4">
            {#each $pipelinesQuery.data.pipelines.filter((p: { enabled: boolean }) => p.enabled).slice(0, 5) as pipeline}
              <div class="flex items-center gap-4">
                <div class="flex h-9 w-9 items-center justify-center rounded-full bg-secondary">
                  <GitBranch class="h-4 w-4" />
                </div>
                <div class="flex-1 space-y-1">
                  <p class="text-sm font-medium leading-none">{pipeline.name || pipeline.pipelineId}</p>
                  <p class="text-sm text-muted-foreground">{pipeline.sourceId} → {pipeline.sinkId}</p>
                </div>
                <Badge variant="success">Enabled</Badge>
              </div>
            {/each}
          </div>
        {/if}
      </CardContent>
    </Card>
  </div>
</div>
