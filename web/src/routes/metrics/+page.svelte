<script lang="ts">
  import { queryStore, getContextClient } from '@urql/svelte';
  import { onMount } from 'svelte';
  import { METRICS_OVERVIEW, SOURCE_OFFSETS, GROUP_OFFSETS, CONSUMER_GROUPS, SERVICES } from '$lib/graphql/queries';
  import { Card, CardHeader, CardTitle, CardDescription, CardContent, Badge } from '$lib/components/ui';
  import { Activity, TrendingUp, Clock, Users, Database, ArrowUpRight, ArrowDownRight } from 'lucide-svelte';

  const client = getContextClient();

  const metricsQuery = queryStore({
    client,
    query: METRICS_OVERVIEW,
    requestPolicy: 'network-only',
  });

  const servicesQuery = queryStore({ client, query: SERVICES });
  const consumerGroupsQuery = queryStore({ client, query: CONSUMER_GROUPS });

  type OffsetHistory = {
    timestamp: number;
    totalOffset: number;
  };

  type SourceOffsetData = {
    current: Map<string, Map<number, number>>;
    history: OffsetHistory[];
    throughput: number;
  };

  type ConsumerLag = {
    groupId: string;
    sourceId: string;
    partition: number;
    sourceOffset: number;
    consumerOffset: number;
    lag: number;
  };

  let sourceOffsets = $state<SourceOffsetData>({
    current: new Map(),
    history: [],
    throughput: 0,
  });

  let consumerLags = $state<ConsumerLag[]>([]);
  let refreshInterval: ReturnType<typeof setInterval> | null = null;
  let lastUpdate = $state<Date | null>(null);

  async function fetchSourceOffsets() {
    const sources = $servicesQuery.data?.services?.filter(
      (s: { serviceType: string }) => s.serviceType === 'SOURCE'
    ) ?? [];

    const newOffsets = new Map<string, Map<number, number>>();
    let totalOffset = 0;

    for (const source of sources) {
      const result = await client.query(SOURCE_OFFSETS, { sourceId: source.serviceId }).toPromise();
      if (result.data?.sourceOffsets) {
        const partitionMap = new Map<number, number>();
        for (const offset of result.data.sourceOffsets) {
          partitionMap.set(offset.partition, offset.offset);
          totalOffset += offset.offset;
        }
        newOffsets.set(source.serviceId, partitionMap);
      }
    }

    const now = Date.now();
    const newHistory = [...sourceOffsets.history, { timestamp: now, totalOffset }].slice(-60);

    let throughput = 0;
    if (newHistory.length >= 2) {
      const oldest = newHistory[0];
      const newest = newHistory[newHistory.length - 1];
      const timeDiffSec = (newest.timestamp - oldest.timestamp) / 1000;
      if (timeDiffSec > 0) {
        throughput = (newest.totalOffset - oldest.totalOffset) / timeDiffSec;
      }
    }

    sourceOffsets = {
      current: newOffsets,
      history: newHistory,
      throughput,
    };

    lastUpdate = new Date();
  }

  async function fetchConsumerLags() {
    const groups = $consumerGroupsQuery.data?.consumerGroups ?? [];
    const lags: ConsumerLag[] = [];

    for (const group of groups) {
      const result = await client.query(GROUP_OFFSETS, { groupId: group.groupId }).toPromise();
      if (result.data?.groupOffsets) {
        for (const groupOffset of result.data.groupOffsets) {
          const sourcePartitions = sourceOffsets.current.get(groupOffset.sourceId);
          if (sourcePartitions) {
            for (const partOffset of groupOffset.partitions) {
              const sourceOffset = sourcePartitions.get(partOffset.partition) ?? 0;
              lags.push({
                groupId: group.groupId,
                sourceId: groupOffset.sourceId,
                partition: partOffset.partition,
                sourceOffset,
                consumerOffset: partOffset.offset,
                lag: sourceOffset - partOffset.offset,
              });
            }
          }
        }
      }
    }

    consumerLags = lags;
  }

  async function refresh() {
    await fetchSourceOffsets();
    await fetchConsumerLags();
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

  const totalLag = $derived(consumerLags.reduce((sum, l) => sum + Math.max(0, l.lag), 0));
  const healthyServices = $derived(
    ($metricsQuery.data?.services ?? []).filter((s: { health: string }) => s.health === 'HEALTHY').length
  );
  const totalServices = $derived(($metricsQuery.data?.services ?? []).length);
  const enabledPipelines = $derived(
    ($metricsQuery.data?.pipelines ?? []).filter((p: { enabled: boolean }) => p.enabled).length
  );
  const totalPipelines = $derived(($metricsQuery.data?.pipelines ?? []).length);

  function formatNumber(n: number): string {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M';
    if (n >= 1_000) return (n / 1_000).toFixed(2) + 'K';
    return n.toFixed(0);
  }

  function formatThroughput(n: number): string {
    if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M/s';
    if (n >= 1_000) return (n / 1_000).toFixed(2) + 'K/s';
    return n.toFixed(1) + '/s';
  }
</script>

<div class="space-y-8">
  <div class="flex items-center justify-between">
    <div>
      <h1 class="text-3xl font-bold tracking-tight">Metrics</h1>
      <p class="text-muted-foreground">Real-time pipeline and throughput monitoring</p>
    </div>
    {#if lastUpdate}
      <div class="flex items-center gap-2 text-sm text-muted-foreground">
        <Clock class="h-4 w-4" />
        Last updated: {lastUpdate.toLocaleTimeString()}
      </div>
    {/if}
  </div>

  <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Throughput</CardTitle>
        <TrendingUp class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div class="flex items-center gap-2">
          <div class="text-2xl font-bold">{formatThroughput(sourceOffsets.throughput)}</div>
          {#if sourceOffsets.throughput > 0}
            <ArrowUpRight class="h-4 w-4 text-green-500" />
          {:else}
            <ArrowDownRight class="h-4 w-4 text-muted-foreground" />
          {/if}
        </div>
        <p class="text-xs text-muted-foreground">Events per second (estimated)</p>
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Consumer Lag</CardTitle>
        <Database class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div class="text-2xl font-bold">{formatNumber(totalLag)}</div>
        <p class="text-xs text-muted-foreground">
          {consumerLags.length} partition{consumerLags.length !== 1 ? 's' : ''} tracked
        </p>
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Services Health</CardTitle>
        <Activity class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div class="text-2xl font-bold">{healthyServices}/{totalServices}</div>
        <p class="text-xs text-muted-foreground">Healthy services</p>
      </CardContent>
    </Card>

    <Card>
      <CardHeader class="flex flex-row items-center justify-between space-y-0 pb-2">
        <CardTitle class="text-sm font-medium">Active Pipelines</CardTitle>
        <Users class="h-4 w-4 text-muted-foreground" />
      </CardHeader>
      <CardContent>
        <div class="text-2xl font-bold">{enabledPipelines}/{totalPipelines}</div>
        <p class="text-xs text-muted-foreground">Pipelines enabled</p>
      </CardContent>
    </Card>
  </div>

  <div class="grid gap-4 lg:grid-cols-2">
    <Card>
      <CardHeader>
        <CardTitle>Throughput History</CardTitle>
        <CardDescription>Offset progression over the last 2 minutes</CardDescription>
      </CardHeader>
      <CardContent>
        <div class="h-48 flex items-end gap-1">
          {#if sourceOffsets.history.length > 1}
            {@const minOffset = Math.min(...sourceOffsets.history.map(h => h.totalOffset))}
            {@const maxOffset = Math.max(...sourceOffsets.history.map(h => h.totalOffset))}
            {@const range = maxOffset - minOffset || 1}
            {#each sourceOffsets.history as point, i}
              {@const height = ((point.totalOffset - minOffset) / range) * 100}
              <div
                class="flex-1 bg-primary rounded-t transition-all duration-300"
                style="height: {Math.max(4, height)}%"
                title="{new Date(point.timestamp).toLocaleTimeString()}: {formatNumber(point.totalOffset)} events"
              ></div>
            {/each}
          {:else}
            <div class="flex-1 flex items-center justify-center text-muted-foreground">
              Collecting data...
            </div>
          {/if}
        </div>
        <div class="mt-2 flex justify-between text-xs text-muted-foreground">
          <span>2 min ago</span>
          <span>Now</span>
        </div>
      </CardContent>
    </Card>

    <Card>
      <CardHeader>
        <CardTitle>Source Offsets</CardTitle>
        <CardDescription>Current position per source and partition</CardDescription>
      </CardHeader>
      <CardContent>
        {#if sourceOffsets.current.size === 0}
          <p class="text-muted-foreground">No sources found</p>
        {:else}
          <div class="space-y-4 max-h-48 overflow-y-auto">
            {#each [...sourceOffsets.current.entries()] as [sourceId, partitions]}
              <div>
                <div class="font-medium text-sm mb-1">{sourceId}</div>
                <div class="flex flex-wrap gap-2">
                  {#each [...partitions.entries()].sort((a, b) => a[0] - b[0]) as [partition, offset]}
                    <Badge variant="secondary" class="font-mono text-xs">
                      P{partition}: {formatNumber(offset)}
                    </Badge>
                  {/each}
                </div>
              </div>
            {/each}
          </div>
        {/if}
      </CardContent>
    </Card>
  </div>

  <Card>
    <CardHeader>
      <CardTitle>Consumer Lag Details</CardTitle>
      <CardDescription>Lag per consumer group, source, and partition</CardDescription>
    </CardHeader>
    <CardContent>
      {#if consumerLags.length === 0}
        <p class="text-muted-foreground">No consumer groups tracked</p>
      {:else}
        <div class="overflow-x-auto">
          <table class="w-full text-sm">
            <thead>
              <tr class="border-b">
                <th class="text-left py-2 font-medium">Consumer Group</th>
                <th class="text-left py-2 font-medium">Source</th>
                <th class="text-right py-2 font-medium">Partition</th>
                <th class="text-right py-2 font-medium">Source Offset</th>
                <th class="text-right py-2 font-medium">Consumer Offset</th>
                <th class="text-right py-2 font-medium">Lag</th>
              </tr>
            </thead>
            <tbody>
              {#each consumerLags.sort((a, b) => b.lag - a.lag) as lag}
                <tr class="border-b last:border-0">
                  <td class="py-2 font-mono text-xs">{lag.groupId}</td>
                  <td class="py-2 font-mono text-xs">{lag.sourceId}</td>
                  <td class="py-2 text-right">{lag.partition}</td>
                  <td class="py-2 text-right font-mono">{formatNumber(lag.sourceOffset)}</td>
                  <td class="py-2 text-right font-mono">{formatNumber(lag.consumerOffset)}</td>
                  <td class="py-2 text-right">
                    <Badge variant={lag.lag > 1000 ? 'destructive' : lag.lag > 100 ? 'secondary' : 'success'}>
                      {formatNumber(lag.lag)}
                    </Badge>
                  </td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </CardContent>
  </Card>

  <Card>
    <CardHeader>
      <CardTitle>Consumer Groups</CardTitle>
      <CardDescription>Active consumer groups and their members</CardDescription>
    </CardHeader>
    <CardContent>
      {#if $consumerGroupsQuery.fetching}
        <p class="text-muted-foreground">Loading...</p>
      {:else if $consumerGroupsQuery.error}
        <p class="text-destructive">Failed to load consumer groups</p>
      {:else if !$consumerGroupsQuery.data?.consumerGroups?.length}
        <p class="text-muted-foreground">No consumer groups found</p>
      {:else}
        <div class="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {#each $consumerGroupsQuery.data.consumerGroups as group}
            <div class="border rounded-lg p-4">
              <div class="flex items-center justify-between mb-2">
                <span class="font-medium">{group.groupId}</span>
                <Badge variant="secondary">Gen {group.generation}</Badge>
              </div>
              <div class="text-sm text-muted-foreground mb-2">
                Stage: {group.stageId}
              </div>
              <div class="text-sm">
                <span class="text-muted-foreground">Members:</span>
                <span class="ml-1">{group.members?.length ?? 0}</span>
              </div>
              {#if group.partitionAssignments?.length > 0}
                <div class="mt-2 space-y-1">
                  {#each group.partitionAssignments as assignment}
                    <div class="text-xs">
                      <span class="font-mono">{assignment.memberId}</span>
                      <span class="text-muted-foreground ml-1">
                        → P[{assignment.partitions?.join(', ') ?? ''}]
                      </span>
                    </div>
                  {/each}
                </div>
              {/if}
            </div>
          {/each}
        </div>
      {/if}
    </CardContent>
  </Card>
</div>
