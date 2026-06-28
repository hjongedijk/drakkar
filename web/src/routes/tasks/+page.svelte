<script lang="ts">
  import { onMount } from 'svelte';
  import Play from '@lucide/svelte/icons/play';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import Clock3 from '@lucide/svelte/icons/clock-3';
  import CheckCircle2 from '@lucide/svelte/icons/check-circle-2';
  import AlertTriangle from '@lucide/svelte/icons/alert-triangle';
  import PageHeader from '$lib/components/PageHeader.svelte';
  import Panel from '$lib/components/Panel.svelte';
  import Button from '$lib/components/Button.svelte';
  import StatusPill from '$lib/components/StatusPill.svelte';
  import { toastError, toastSuccess } from '$lib/toast';
  import { api, subscribeEvents } from '$lib/api';
  import type { TaskSchedule } from '$lib/types';

  type TaskResult = { ok: boolean; detail: string; ranAt: string };
  type TaskDef = {
    id: string;
    label: string;
    description: string;
    group: string;
    interval: string;
    manual: boolean;
    run: () => Promise<unknown>;
  };

  let running: Record<string, boolean> = {};
  let results: Record<string, TaskResult> = {};
  let schedules: TaskSchedule[] = [];

  const tasks: TaskDef[] = [
    { id: 'seerr_sync', label: 'Sync Seerr Requests', description: 'Import new and updated requests from Seerr.', group: 'Indexing', interval: '10m', manual: true, run: async () => { const r = await api.syncRequests(); return `seen ${r.seen}, created ${r.created}`; } },
    { id: 'pending_queue_push', label: 'Dispatch Pending Queue', description: 'Push pending library rows into the bounded background work queue.', group: 'Indexing', interval: '30s', manual: false, run: async () => '' },
    { id: 'stale-queue-reset', label: 'Reset Stale Queue Items', description: 'Reset queue rows that have been stuck too long in transitional states.', group: 'Indexing', interval: '5m', manual: false, run: async () => '' },
    { id: 'backlog_search', label: 'Backlog Search', description: 'Search missing library items — one search per show+season per batch, 1-hour cooldown per item. Same logic as Sonarr/Radarr.', group: 'Indexing', interval: '30m', manual: true, run: async () => { await api.searchPendingLibrary(); return 'started in background'; } },
    { id: 'search_upgrades', label: 'Search Quality Upgrades', description: 'Re-search available items whose quality profile allows upgrades and replace them when a better release is found.', group: 'Indexing', interval: '6h', manual: true, run: async () => { await api.searchUpgrades(); return 'started in background'; } },
    { id: 'retry_failed_queue', label: 'Retry Failed Queue', description: 'Retry failed queue rows using current fallback policy.', group: 'Indexing', interval: '15m', manual: true, run: async () => { const r = await api.retryFailedQueue(); return `processed ${r.processed}, retried ${r.retried}`; } },
    { id: 'fill_missing_episodes', label: 'Fill Missing Episodes', description: 'Use TMDB episode lists to create library items for episodes not yet tracked, then queue them for search.', group: 'Indexing', interval: '6h', manual: true, run: async () => { const r = await api.fillMissingEpisodes(); return `processed ${r.showsProcessed} shows, found ${r.episodesFound} episodes, created ${r.itemsCreated} new items`; } },
    { id: 'backfill_metadata', label: 'Backfill Metadata', description: 'Re-enrich movies and TV shows with new TMDB fields (tagline, status, content rating, release date, trailer).', group: 'Indexing', interval: 'Manual', manual: true, run: async () => { const r = await api.backfillMetadata(); return `enriched ${r.enriched} items (movies: ${r.processedMovies}, shows: ${r.processedShows}, failed: ${r.failed})`; } },
    { id: 'republish_pending', label: 'Republish Pending', description: 'Republish library items with a selected release but no current publication.', group: 'Publishing', interval: '30m', manual: true, run: async () => { await api.republishPendingLibrary(); return 'started in background'; } },
    { id: 'reset_orphaned_available', label: 'Reset Orphaned Available Items', description: 'Reset available items with no symlink and no recoverable source back to pending so they are re-searched and re-downloaded.', group: 'Publishing', interval: '30m', manual: true, run: async () => { await api.resetOrphanedAvailableItems(); return 'started in background'; } },
    { id: 'health_check', label: 'Symlink Health Check', description: 'Verify published symlinks still point to valid VFS targets. Runs every 15 minutes; also runs deep NZB article probe in background.', group: 'Maintenance', interval: '15m', manual: true, run: async () => { const r = await api.runHealthCheck(); return `checked ${r.checked}, healthy ${r.healthy}`; } },
    { id: 'article_health_check', label: 'Article Health Check', description: 'Probe first NNTP segment of every direct-NZB item. Resets items with expired or missing articles.', group: 'Maintenance', interval: '6h', manual: false, run: async () => '' },
    { id: 'nzb_health_check', label: 'Deep NZB Article Check', description: 'Full NNTP article scan — probes first/last segments for all published items and resets missing-article or sample-only publications.', group: 'Maintenance', interval: '168h', manual: false, run: async () => '' },
    { id: 'library-cleanup', label: 'Library Cleanup', description: 'Remove orphaned VFS content, broken media symlinks, and stale history entries. Combines three maintenance passes into one.', group: 'Maintenance', interval: '6h', manual: false, run: async () => '' },
    { id: 'cache_prune', label: 'Prune Block Cache', description: 'Delete oldest decoded articles from disk cache.', group: 'Maintenance', interval: '6h', manual: true, run: async () => { const r = await api.pruneCache(); return `deleted ${r.deletedFiles} files`; } }
  ];

  let schedulesLoading = true;

  async function loadSchedules() {
    try {
      schedules = (await api.taskSchedules()).items ?? [];
    } catch {
      // silently ignore — UI shows "—" when schedules unavailable
    } finally {
      schedulesLoading = false;
    }
  }

  async function runTask(task: TaskDef) {
    running = { ...running, [task.id]: true };
    const ranAt = new Date().toISOString();
    try {
      const detail = String(await task.run());
      results = { ...results, [task.id]: { ok: true, detail, ranAt } };
      toastSuccess(`${task.label}: ${detail}`);
      await loadSchedules();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      results = { ...results, [task.id]: { ok: false, detail, ranAt } };
      toastError(`${task.label} failed: ${detail}`);
    } finally {
      running = { ...running, [task.id]: false };
    }
  }

  function fmtTime(iso: string) {
    return new Date(iso).toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
  }

  function scheduleFor(task: TaskDef) {
    return schedules.find((item) => item.id === task.id);
  }

  $: groups = [...new Set(tasks.map((task) => task.group))];
  $: runningCount = Object.values(running).filter(Boolean).length;
  $: lastRunCount = Object.keys(results).length;

  // Map SSE event kinds to human-readable task completion messages.
  const backgroundKinds: Record<string, (e: Record<string, unknown>) => string> = {
    'library.republish_pending': (e) => `Republish Pending complete: processed ${e.processed}, republished ${e.republished}, failed ${e.failed}`,
    'library.reset_orphaned': (e) => `Reset Orphaned complete: found ${e.found}, reset ${e.reset}, failed ${e.failed}`,
    'library.search_pending': (e) => `Search Pending complete: processed ${e.processed}, searched ${e.searched}, selected ${e.selected}`,
    'library.search_upgrades': (e) => `Search Upgrades complete: checked ${e.checked}, upgraded ${e.upgraded}, failed ${e.failed}`,
  };

  onMount(() => {
    void loadSchedules();
    const t = window.setInterval(() => void loadSchedules(), 30000);
    const unsub = subscribeEvents((event) => {
      if (!event) return;
      const fmt = backgroundKinds[event.kind as string];
      if (fmt) {
        toastSuccess(fmt(event));
        void loadSchedules();
      }
    });
    return () => { window.clearInterval(t); unsub(); };
  });
</script>

<svelte:head><title>Tasks — Drakkar</title></svelte:head>

<PageHeader title="Tasks" subtitle="Scheduled-job style control plane for indexing, publishing, and maintenance work.">
  <StatusPill tone="neutral">{tasks.length} tasks</StatusPill>
  <StatusPill tone={runningCount > 0 ? 'warn' : 'ok'}>{runningCount} running</StatusPill>
</PageHeader>

<section class="summary-grid">
  <div class="summary-card">
    <div class="summary-value">{tasks.filter((task) => task.group === 'Indexing').length}</div>
    <div class="summary-label">Indexing tasks</div>
  </div>
  <div class="summary-card">
    <div class="summary-value">{tasks.filter((task) => task.group === 'Publishing').length}</div>
    <div class="summary-label">Publishing tasks</div>
  </div>
  <div class="summary-card">
    <div class="summary-value">{tasks.filter((task) => task.group === 'Maintenance').length}</div>
    <div class="summary-label">Maintenance tasks</div>
  </div>
  <div class="summary-card">
    <div class="summary-value">{lastRunCount}</div>
    <div class="summary-label">Executed this session</div>
  </div>
</section>

<Panel title="Scheduled" subtitle="Reference-style scheduled tasks table, backed by current callable operations.">
  <div class="table-wrap">
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Interval</th>
          <th>Status</th>
          <th>Last Execution</th>
          <th>Action</th>
        </tr>
      </thead>
      <tbody>
        {#each groups as group}
          <tr class="group-row">
            <td colspan="5">{group}</td>
          </tr>
          {#each tasks.filter((task) => task.group === group) as task}
            {@const busy = running[task.id]}
            {@const result = results[task.id]}
            {@const schedule = scheduleFor(task)}
            <tr>
              <td>
                <div class="row-title">{task.label}</div>
                <div class="row-sub">{task.description}</div>
                {#if result}
                  <div class={`result ${result.ok ? 'ok' : 'fail'}`}>
                    <svelte:component this={result.ok ? CheckCircle2 : AlertTriangle} size={12} />
                    <span>{result.detail}</span>
                  </div>
                {/if}
              </td>
              <td class="muted">{schedule?.interval ?? task.interval}</td>
              <td>
                {#if busy}
                  <StatusPill tone="warn">Running</StatusPill>
                {:else if schedule?.automated}
                  <StatusPill tone="ok">Automated</StatusPill>
                {:else if result?.ok}
                  <StatusPill tone="ok">Success</StatusPill>
                {:else if result && !result.ok}
                  <StatusPill tone="danger">Failed</StatusPill>
                {:else}
                  <StatusPill tone="neutral">Idle</StatusPill>
                {/if}
              </td>
              <td class="muted">
                {#if result}
                  <span class="time-cell"><Clock3 size={12} /> {fmtTime(result.ranAt)}</span>
                {:else if schedule?.lastRunAt}
                  <span class="time-cell"><Clock3 size={12} /> {fmtTime(schedule.lastRunAt)}</span>
                {:else if schedulesLoading}
                  <span class="time-cell dim">—</span>
                {:else}
                  <span class="time-cell dim">Never</span>
                {/if}
              </td>
              <td>
                <Button kind="secondary" on:click={() => runTask(task)} disabled={busy || !task.manual}>
                  {#if busy}
                    <RefreshCw size={14} class="spin" />
                    Running…
                  {:else}
                    <Play size={14} />
                    Run
                  {/if}
                </Button>
              </td>
            </tr>
          {/each}
        {/each}
      </tbody>
    </table>
  </div>
</Panel>

<style>
  .summary-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 14px;
    margin-bottom: 20px;
  }

  .summary-card {
    padding: 18px 20px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 20px;
    background: hsl(var(--card) / 0.82);
  }

  .summary-value {
    font-size: 2rem;
    font-weight: 700;
    line-height: 1;
  }

  .summary-label,
  .row-sub,
  .muted {
    margin-top: 8px;
    color: hsl(var(--muted-foreground));
    font-size: 13px;
  }

  .table-wrap {
    overflow-x: auto;
  }

  table {
    width: 100%;
    min-width: 880px;
    border-collapse: collapse;
  }

  th,
  td {
    padding: 14px 10px;
    border-bottom: 1px solid hsl(0 0% 100% / 0.05);
    text-align: left;
    vertical-align: top;
  }

  th {
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.18em;
    color: hsl(var(--muted-foreground));
  }

  .group-row td {
    padding-top: 20px;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: hsl(var(--primary));
    background: transparent;
  }

  .row-title {
    font-weight: 600;
  }

  .result,
  .time-cell {
    display: inline-flex;
    align-items: center;
    gap: 6px;
  }

  .time-cell.dim { opacity: 0.4; }

  .result {
    margin-top: 10px;
    font-size: 12px;
    font-family: 'JetBrains Mono', monospace;
  }

  .result.ok { color: hsl(141 80% 68%); }
  .result.fail { color: hsl(0 96% 82%); }

  .time-cell {
    color: hsl(var(--muted-foreground));
    font-size: 12px;
  }

  :global(.spin) {
    animation: spin 1s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }

  @media (max-width: 900px) {
    .summary-grid {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
</style>
