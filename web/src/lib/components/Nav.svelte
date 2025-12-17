<script lang="ts">
  import { page } from '$app/stores';
  import { cn } from '$lib/utils';
  import { LayoutDashboard, Server, GitBranch, Activity, Shield, BarChart3, AlertTriangle } from 'lucide-svelte';
  import { authClient, isAdmin } from '$lib/auth-client';
  import UserMenu from './UserMenu.svelte';

  interface Props {
    authEnabled: boolean;
  }

  let { authEnabled }: Props = $props();

  const session = authClient.useSession();

  const links = [
    { href: '/', label: 'Dashboard', icon: LayoutDashboard },
    { href: '/services', label: 'Services', icon: Server },
    { href: '/pipelines', label: 'Pipelines', icon: GitBranch },
    { href: '/cluster', label: 'Cluster', icon: Activity },
    { href: '/metrics', label: 'Metrics', icon: BarChart3 },
    { href: '/errors', label: 'Errors', icon: AlertTriangle },
  ];

  const adminLinks = [
    { href: '/admin', label: 'Admin', icon: Shield },
  ];

  const userRole = $derived(($session.data?.user as { role?: string } | undefined)?.role);
  const showAdminLinks = $derived(authEnabled && isAdmin(userRole));
</script>

<nav class="flex h-16 items-center border-b bg-background px-6">
  <div class="flex items-center gap-2 font-bold text-xl">
    <GitBranch class="h-6 w-6" />
    <span>ETL Router</span>
  </div>

  <div class="ml-10 flex flex-1 items-center gap-1">
    {#each links as link}
      <a
        href={link.href}
        class={cn(
          'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground',
          $page.url.pathname === link.href
            ? 'bg-accent text-accent-foreground'
            : 'text-muted-foreground'
        )}
      >
        <link.icon class="h-4 w-4" />
        {link.label}
      </a>
    {/each}

    {#if showAdminLinks}
      <div class="mx-2 h-6 w-px bg-border"></div>
      {#each adminLinks as link}
        <a
          href={link.href}
          class={cn(
            'flex items-center gap-2 rounded-md px-3 py-2 text-sm font-medium transition-colors hover:bg-accent hover:text-accent-foreground',
            $page.url.pathname.startsWith(link.href)
              ? 'bg-accent text-accent-foreground'
              : 'text-muted-foreground'
          )}
        >
          <link.icon class="h-4 w-4" />
          {link.label}
        </a>
      {/each}
    {/if}
  </div>

  {#if authEnabled}
    <UserMenu />
  {/if}
</nav>
