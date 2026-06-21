<script lang="ts">
  import { onMount } from 'svelte';
  import HeartPulse from '@lucide/svelte/icons/heart-pulse';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import ShieldCheck from '@lucide/svelte/icons/shield-check';
  import AlertTriangle from '@lucide/svelte/icons/alert-triangle';
  import PageHeader from '$lib/components/PageHeader.svelte';
  import Panel from '$lib/components/Panel.svelte';
  import Button from '$lib/components/Button.svelte';
  import StatusPill from '$lib/components/StatusPill.svelte';
  import { api, subscribeEvents } from '$lib/api';
  import { toastError, toastSuccess } from '$lib/toast';

  type HealthSummary = { total: number; checked: number; healthy: number; neverChecked: number; consistencyIssues: number };
  type HealthEntry = {
    id: number;
    libraryItemId: number;
    libraryPath: string;
    targetPath: string;
    createdAt: string;
    lastCheckedAt?: string;
    healthOk?: boolean;
  };
  type ConsistencyIssue = {
    libraryItemId: number;
    title: string;
    mediaType: string;
    queueState: string;
  };

  let summary: HealthSummary | null = null;
  let entries: HealthEntry[] = [];
  let consistency: ConsistencyIssue[] = [];
  let loading = true;
  let checking = false;
  let republishing = false;
  let resettingOrphaned = false;

  async function load() {
    loading = true;
    try {
      const [nextSummary, nextEntries, nextConsistency] = await Promise.all([
        api.healthSummary(),
        api.healthEntries(),
        api.healthConsistency()
      ]);
      summary = nextSummary;
      entries = nextEntries.items ?? [];
      consistency = nextConsistency.items ?? [];
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
    } finally {
      loading = false;
    }
  }

  async function runCheck() {
    checking = true;
    try {
      const result = await api.runHealthCheck();
      toastSuccess(`Checked ${result.checked} — ${result.healthy} healthy`);
      await load();
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
    } finally {
      checking = false;
    }
  }

  async function republishPending() {
    republishing = true;
    try {
      await api.republishPendingLibrary();
      toastSuccess('Republish Pending started in background');
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
      republishing = false;
    }
  }

  async function resetOrphanedAvailable() {
    resettingOrphaned = true;
    try {
      await api.resetOrphanedAvailableItems();
      toastSuccess('Reset Orphaned Available Items started in background');
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
      resettingOrphaned = false;
    }
  }

  function fmtDate(value?: string, fallback = 'Never') {
    if (!value) return fallback;
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return fallback;
    return date.toLocaleString('en-GB', {
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    });
  }

  function shortName(path: string) {
    const parts = path.split('/');
    return parts[parts.length - 1] || path;
  }

  $: checked = summary?.checked ?? 0;
  $: healthy = summary?.healthy ?? 0;
  $: broken = checked - healthy;
  $: healthyPct = checked > 0 ? Math.round((healthy / checked) * 100) : 0;
  $: consistencyIssues = summary?.consistencyIssues ?? 0;

  onMount(() => {
    void load();
    return subscribeEvents((event) => {
      if (event?.kind === 'library.republish_pending') {
        toastSuccess(`Republish Pending complete: processed ${event.processed}, republished ${event.republished}, failed ${event.failed}`);
        republishing = false;
      }
      if (event?.kind === 'library.reset_orphaned') {
        toastSuccess(`Reset Orphaned complete: found ${event.found}, reset ${event.reset}, failed ${event.failed}`);
        resettingOrphaned = false;
      }
      if (!checking) void load();
    });
  });
</script>

<svelte:head><title>Health — Drakkar</title></svelte:head>

<PageHeader title="Health" subtitle="Symlink health, library consistency, and NZB article availability.">
  <Button kind="secondary" on:click={load} disabled={loading || checking}>
    <RefreshCw size={14} />
    Refresh
  </Button>
  <Button kind="primary" on:click={runCheck} disabled={loading || checking || republishing || resettingOrphaned}>
    <ShieldCheck size={14} />
    {checking ? 'Running…' : 'Run Health Check'}
  </Button>
</PageHeader>

{#if summary}
  <section class="stats-grid">
    <div class="stat-card">
      <div class="stat-value">{summary.total}</div>
      <div class="stat-label">Total published symlinks</div>
    </div>
    <div class="stat-card">
      <div class="stat-value ok">{healthy}</div>
      <div class="stat-label">Healthy symlinks ({healthyPct}%)</div>
      <div class="bar"><div class="fill ok" style={`width:${healthyPct}%`}></div></div>
    </div>
    <div class="stat-card">
      <div class="stat-value warn">{summary.neverChecked}</div>
      <div class="stat-label">Never checked</div>
    </div>
    <div class="stat-card">
      <div class="stat-value danger">{broken}</div>
      <div class="stat-label">Broken symlinks</div>
    </div>
    <div class="stat-card {consistencyIssues > 0 ? 'has-issue' : ''}">
      <div class="stat-value {consistencyIssues > 0 ? 'danger' : 'ok'}">{consistencyIssues}</div>
      <div class="stat-label">Consistency issues</div>
      <div class="stat-hint">Available items with no symlink</div>
    </div>
  </section>

  {#if summary.neverChecked > 0}
    <div class="attention">
      <div class="attention-title"><HeartPulse size={16} /> Attention</div>
      <ul>
        <li>{summary.neverChecked} item(s) have never been health-checked.</li>
        <li>Run a check now for immediate verification, or wait for the hourly background task.</li>
      </ul>
    </div>
  {/if}

  {#if consistencyIssues > 0}
    <div class="attention attention-danger">
      <div class="attention-title"><AlertTriangle size={16} /> Consistency Issues</div>
      <p>
        {consistencyIssues} library item(s) are marked <strong>available</strong> but have no published symlink.
        These items may show as available in the library but will not stream.
        Use <strong>Republish Pending</strong> when the selected release is still recoverable. Use
        <strong> Reset Orphaned Available</strong> when the item needs to be re-queued for a fresh search and download.
      </p>
      <div class="attention-actions">
        <Button kind="secondary" on:click={republishPending} disabled={loading || checking || republishing || resettingOrphaned}>
          <RefreshCw size={14} />
          {republishing ? 'Republishing…' : 'Republish Pending'}
        </Button>
        <Button kind="primary" on:click={resetOrphanedAvailable} disabled={loading || checking || republishing || resettingOrphaned}>
          <AlertTriangle size={14} />
          {resettingOrphaned ? 'Resetting…' : 'Reset Orphaned Available'}
        </Button>
      </div>
    </div>
  {/if}

  {#if consistency.length > 0}
    <Panel title="Consistency Issues" subtitle="Items marked available but missing a published symlink.">
      <div slot="actions">
        <StatusPill tone="danger">{consistency.length} item(s)</StatusPill>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Title</th>
              <th>Type</th>
              <th>Queue State</th>
            </tr>
          </thead>
          <tbody>
            {#each consistency as issue}
              <tr>
                <td>
                  <div class="row-title">{issue.title}</div>
                  <div class="row-sub">ID {issue.libraryItemId}</div>
                </td>
                <td><StatusPill tone="neutral">{issue.mediaType}</StatusPill></td>
                <td><StatusPill tone={issue.queueState === 'available' ? 'ok' : 'warn'}>{issue.queueState || 'unknown'}</StatusPill></td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    </Panel>
  {/if}

  <Panel title="Symlink Health Schedule" subtitle="All published media — symlink verified hourly against VFS target paths.">
    <div slot="actions">
      <StatusPill tone={broken > 0 ? 'warn' : 'ok'}>{entries.length} item(s)</StatusPill>
    </div>
    {#if entries.length > 0}
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Created</th>
              <th>Last Check</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {#each entries as entry}
              <tr>
                <td>
                  <div class="row-title">{shortName(entry.libraryPath)}</div>
                  <div class="row-sub">{entry.libraryPath}</div>
                </td>
                <td>{fmtDate(entry.createdAt, 'Unknown')}</td>
                <td>{fmtDate(entry.lastCheckedAt)}</td>
                <td>
                  {#if entry.healthOk === true}
                    <StatusPill tone="ok">Healthy</StatusPill>
                  {:else if entry.healthOk === false}
                    <StatusPill tone="danger">Broken</StatusPill>
                  {:else}
                    <StatusPill tone="warn">Unchecked</StatusPill>
                  {/if}
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {:else}
      <div class="empty-state">{loading ? 'Loading health entries…' : 'No published media yet.'}</div>
    {/if}
  </Panel>

  <div class="info-card">
    <div class="info-title">Deep NZB Article Check</div>
    <p>
      In addition to symlink verification, Drakkar probes NNTP providers weekly to confirm that
      the actual NNTP articles behind each published item are still available. Items whose articles
      have expired are automatically reset to <em>requested</em> so a fresh release can be selected.
      This check also runs on startup. It cannot be triggered manually — see the Tasks tab for
      the scheduled run time.
    </p>
  </div>
{/if}

<style>
  .stats-grid {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    gap: 14px;
    margin-bottom: 18px;
  }

  .stat-card,
  .attention,
  .info-card {
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 20px;
    background: hsl(var(--card) / 0.82);
  }

  .stat-card {
    padding: 22px;
  }

  .stat-card.has-issue {
    border-color: hsl(0 96% 82% / 0.28);
  }

  .stat-value {
    font-size: 2rem;
    font-weight: 700;
    line-height: 1;
  }

  .stat-value.ok { color: hsl(141 80% 68%); }
  .stat-value.warn { color: hsl(47 100% 77%); }
  .stat-value.danger { color: hsl(0 96% 82%); }

  .stat-label,
  .row-sub,
  .empty-state {
    margin-top: 8px;
    font-size: 13px;
    color: hsl(var(--muted-foreground));
  }

  .stat-hint {
    margin-top: 4px;
    font-size: 11px;
    color: hsl(var(--muted-foreground));
    opacity: 0.7;
  }

  .bar {
    margin-top: 12px;
    height: 8px;
    border-radius: 999px;
    background: hsl(0 0% 100% / 0.06);
    overflow: hidden;
  }

  .fill {
    height: 100%;
    border-radius: 999px;
    background: hsl(var(--primary));
  }

  .fill.ok { background: hsl(141 80% 68%); }

  .attention {
    padding: 16px 18px;
    margin-bottom: 18px;
    border-color: hsl(43 96% 44% / 0.28);
    background: hsl(43 96% 44% / 0.08);
  }

  .attention-danger {
    border-color: hsl(0 96% 82% / 0.28);
    background: hsl(0 96% 82% / 0.06);
  }

  .attention-title {
    display: flex;
    align-items: center;
    gap: 8px;
    font-weight: 700;
    color: hsl(47 100% 77%);
    margin-bottom: 8px;
  }

  .attention-danger .attention-title {
    color: hsl(0 96% 82%);
  }

  .attention ul {
    margin: 0;
    padding-left: 18px;
    color: hsl(var(--foreground));
  }

  .attention p {
    margin: 0;
    color: hsl(var(--foreground));
  }

  .attention-actions {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-top: 14px;
  }

  .info-card {
    padding: 20px 22px;
    margin-top: 18px;
  }

  .info-title {
    font-weight: 700;
    font-size: 14px;
    margin-bottom: 8px;
    color: hsl(var(--foreground));
  }

  .info-card p {
    margin: 0;
    font-size: 13px;
    color: hsl(var(--muted-foreground));
    line-height: 1.6;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  th,
  td {
    text-align: left;
    padding: 14px 10px;
    border-bottom: 1px solid hsl(0 0% 100% / 0.05);
    vertical-align: top;
  }

  th {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: hsl(var(--muted-foreground));
  }

  .row-title {
    font-weight: 600;
  }

  @media (max-width: 1100px) {
    .stats-grid {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
  }

  @media (max-width: 700px) {
    .stats-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
</style>
