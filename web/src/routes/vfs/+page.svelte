<script lang="ts">
  import { onMount } from 'svelte';
  import Folder from '@lucide/svelte/icons/folder';
  import File from '@lucide/svelte/icons/file';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import ChevronRight from '@lucide/svelte/icons/chevron-right';
  import Copy from '@lucide/svelte/icons/copy';
  import MonitorPlay from '@lucide/svelte/icons/monitor-play';
  import HardDrive from '@lucide/svelte/icons/hard-drive';
  import Activity from '@lucide/svelte/icons/activity';
  import X from '@lucide/svelte/icons/x';
  import Home from '@lucide/svelte/icons/home';
  import PageHeader from '$lib/components/PageHeader.svelte';
  import Panel from '$lib/components/Panel.svelte';
  import Button from '$lib/components/Button.svelte';
  import { api } from '$lib/api';
  import { toastError, toastSuccess } from '$lib/toast';

  type VFSEntry = { name: string; path: string; isDir: boolean; size: number };
  type StreamItem = {
    sessionId?: string;
    sessionID?: string;
    fileName?: string;
    filePath?: string;
    currentOffset?: number;
    fileSize?: number;
    fileSizeBytes?: number;
  };

  let currentPath = '/';
  let entries: VFSEntry[] = [];
  let streams: StreamItem[] = [];
  let metrics: Record<string, number> = {};
  let loading = false;

  async function browse(path: string) {
    loading = true;
    try {
      const [listing, nextMetrics, nextStreams] = await Promise.all([
        fetch(`/api/vfs?path=${encodeURIComponent(path)}`).then(async (r) => {
          if (!r.ok) throw new Error(`${r.status} ${r.statusText}`);
          return r.json();
        }),
        api.metrics(),
        api.streams()
      ]);
      entries = listing.entries ?? [];
      currentPath = path;
      metrics = nextMetrics;
      streams = nextStreams.sessions ?? [];
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
    } finally {
      loading = false;
    }
  }

  function crumbs() {
    const parts = currentPath.split('/').filter(Boolean);
    const result: { label: string; path: string; isHome: boolean }[] = [
      { label: 'vfs', path: '/', isHome: true }
    ];
    let acc = '';
    for (const part of parts) {
      acc += `/${part}`;
      result.push({ label: part, path: acc, isHome: false });
    }
    return result;
  }

  function fmtBytes(bytes: number): string {
    if (!bytes) return '—';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let value = bytes;
    let unit = 0;
    while (value >= 1024 && unit < units.length - 1) {
      value /= 1024;
      unit++;
    }
    return `${value >= 10 || unit === 0 ? value.toFixed(0) : value.toFixed(1)} ${units[unit]}`;
  }

  async function copyPath(entryPath: string) {
    const full = `/mnt/drakkar/vfs${entryPath === '/' ? '' : entryPath}`;
    try {
      await navigator.clipboard.writeText(full);
      toastSuccess('Path copied');
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
    }
  }

  async function stopStream(sessionId: string) {
    try {
      await api.stopStream(sessionId);
      toastSuccess('Stream stopped');
      void browse(currentPath);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    }
  }

  function streamProgress(stream: StreamItem): number {
    const offset = stream.currentOffset ?? 0;
    const size = stream.fileSizeBytes ?? stream.fileSize ?? 0;
    if (!size) return 0;
    return Math.min(100, (offset / size) * 100);
  }

  function streamLabel(stream: StreamItem): string {
    const name = stream.fileName ?? stream.filePath ?? '';
    if (!name) return 'Unknown stream';
    const parts = name.split(/[\\/]/);
    return parts[parts.length - 1] || name;
  }

  $: sorted = [...entries].sort(
    (a, b) => Number(b.isDir) - Number(a.isDir) || a.name.localeCompare(b.name)
  );

  onMount(() => {
    void browse('/');
  });
</script>

<svelte:head><title>VFS — Drakkar</title></svelte:head>

<PageHeader title="VFS Browser" subtitle="Browse the mounted virtual filesystem, inspect paths, and monitor active stream sessions.">
  <Button kind="secondary" on:click={() => browse(currentPath)} disabled={loading}>
    <RefreshCw size={14} class={loading ? 'spin' : ''} />
    Refresh
  </Button>
</PageHeader>

<div class="layout">
  <!-- ── File Browser ─────────────────────────────────────────────── -->
  <div class="browser-col">
    <!-- Toolbar -->
    <div class="toolbar">
      <nav class="breadcrumb" aria-label="Directory path">
        {#each crumbs() as crumb, i}
          {#if i > 0}
            <span class="sep" aria-hidden="true"><ChevronRight size={13} /></span>
          {/if}
          <button
            class="crumb-btn"
            class:active={crumb.path === currentPath}
            on:click={() => browse(crumb.path)}
            aria-current={crumb.path === currentPath ? 'page' : undefined}
          >
            {#if crumb.isHome}<Home size={13} />{/if}
            {crumb.label}
          </button>
        {/each}
      </nav>
    </div>

    <!-- File table -->
    <div class="table-card">
      {#if loading}
        <div class="loading-bar" aria-label="Loading"></div>
      {/if}
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th class="col-name">Name</th>
              <th class="col-size">Size</th>
              <th class="col-actions">Actions</th>
            </tr>
          </thead>
          <tbody>
            {#each sorted as entry (entry.path)}
              <tr class="file-row" class:is-dir={entry.isDir}>
                <td class="col-name">
                  <div class="name-cell">
                    <span class="file-icon" class:dir-icon={entry.isDir}>
                      {#if entry.isDir}
                        <Folder size={16} />
                      {:else}
                        <File size={16} />
                      {/if}
                    </span>
                    {#if entry.isDir}
                      <button class="name-btn dir-link" on:click={() => browse(entry.path)}>
                        {entry.name}
                      </button>
                    {:else}
                      <span class="name-btn">{entry.name}</span>
                    {/if}
                  </div>
                </td>
                <td class="col-size">
                  <span class="size-val">
                    {entry.isDir ? '—' : fmtBytes(entry.size)}
                  </span>
                </td>
                <td class="col-actions">
                  <button
                    class="action-btn"
                    title="Copy VFS path"
                    on:click={() => copyPath(entry.path)}
                  >
                    <Copy size={13} />
                    <span>Copy path</span>
                  </button>
                </td>
              </tr>
            {/each}

            {#if !loading && sorted.length === 0}
              <tr>
                <td colspan="3">
                  <div class="empty-state">
                    <Folder size={32} />
                    <p>This directory is empty</p>
                  </div>
                </td>
              </tr>
            {/if}
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- ── Sidebar ─────────────────────────────────────────────────── -->
  <aside class="sidebar">
    <!-- Mount metrics -->
    <Panel flush>
      <svelte:fragment slot="actions" />
      <div class="sidebar-section">
        <div class="sidebar-heading">
          <HardDrive size={14} />
          <span>Mount</span>
        </div>
        <div class="metric-rows">
          <div class="metric-row">
            <span class="metric-label">Mount path</span>
            <code class="metric-val mono">/mnt/drakkar/vfs</code>
          </div>
          <div class="metric-row">
            <span class="metric-label">Active streams</span>
            <span class="metric-val accent">{metrics.active_streams ?? 0}</span>
          </div>
          <div class="metric-row">
            <span class="metric-label">Active NNTP</span>
            <span class="metric-val">{metrics.active_nntp_connections ?? 0}</span>
          </div>
          <div class="metric-row">
            <span class="metric-label">Idle NNTP</span>
            <span class="metric-val">{metrics.idle_nntp_connections ?? 0}</span>
          </div>
          <div class="metric-row">
            <span class="metric-label">Cache used</span>
            <span class="metric-val">{fmtBytes(metrics.disk_cache_used_bytes ?? 0)}</span>
          </div>
        </div>
      </div>
    </Panel>

    <!-- Active stream sessions -->
    <Panel flush>
      <div class="sidebar-section">
        <div class="sidebar-heading">
          <Activity size={14} />
          <span>Streams</span>
          {#if streams.length > 0}
            <span class="stream-badge">{streams.length}</span>
          {/if}
        </div>

        {#if streams.length === 0}
          <div class="streams-empty">
            <MonitorPlay size={24} />
            <p>No active sessions</p>
          </div>
        {:else}
          <div class="stream-list">
            {#each streams as stream}
              {@const sid = stream.sessionId ?? stream.sessionID ?? ''}
              {@const pct = streamProgress(stream)}
              {@const totalSize = stream.fileSizeBytes ?? stream.fileSize ?? 0}
              <div class="stream-card">
                <div class="stream-header">
                  <span class="stream-name" title={stream.fileName ?? stream.filePath ?? ''}>
                    {streamLabel(stream)}
                  </span>
                  {#if sid}
                    <button
                      class="stop-btn"
                      title="Stop stream"
                      on:click={() => stopStream(sid)}
                    >
                      <X size={12} />
                    </button>
                  {/if}
                </div>

                <div class="progress-wrap">
                  <div class="progress-track">
                    <div class="progress-fill" style="width: {pct}%"></div>
                  </div>
                  <div class="progress-labels">
                    <span>{fmtBytes(stream.currentOffset ?? 0)}</span>
                    <span>{fmtBytes(totalSize)}</span>
                  </div>
                </div>

                {#if sid}
                  <div class="stream-id mono">{sid.slice(0, 20)}&hellip;</div>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
      </div>
    </Panel>
  </aside>
</div>

<style>
  /* ── Layout ─────────────────────────────────────────────────────── */
  .layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 280px;
    gap: 20px;
    align-items: start;
  }

  @media (max-width: 900px) {
    .layout {
      grid-template-columns: 1fr;
    }
  }

  .browser-col {
    display: flex;
    flex-direction: column;
    gap: 12px;
    min-width: 0;
  }

  .sidebar {
    display: flex;
    flex-direction: column;
    gap: 14px;
  }

  /* ── Toolbar ─────────────────────────────────────────────────────── */
  .toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 10px 16px;
    background: hsl(var(--card) / 0.6);
    border: 1px solid hsl(var(--border) / 0.7);
    border-radius: 16px;
    backdrop-filter: blur(12px);
  }

  .breadcrumb {
    display: flex;
    align-items: center;
    gap: 4px;
    flex-wrap: wrap;
    flex: 1;
    min-width: 0;
  }

  .sep {
    color: hsl(var(--muted-foreground) / 0.45);
    display: flex;
    align-items: center;
    flex-shrink: 0;
  }

  .crumb-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 4px 10px;
    border-radius: 8px;
    border: 1px solid transparent;
    background: transparent;
    color: hsl(var(--muted-foreground));
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: background 0.12s, color 0.12s, border-color 0.12s;
    white-space: nowrap;
  }

  .crumb-btn:hover {
    background: hsl(0 0% 100% / 0.06);
    color: hsl(var(--foreground));
  }

  .crumb-btn.active {
    background: hsl(var(--primary) / 0.15);
    border-color: hsl(var(--primary) / 0.35);
    color: hsl(var(--primary));
    cursor: default;
  }

  /* ── Table card ──────────────────────────────────────────────────── */
  .table-card {
    position: relative;
    border: 1px solid hsl(var(--border) / 0.9);
    border-radius: 20px;
    background: hsl(var(--card) / 0.82);
    box-shadow: 0 20px 60px hsl(190 80% 6% / 0.24);
    backdrop-filter: blur(20px);
    overflow: hidden;
  }

  .loading-bar {
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    height: 2px;
    background: linear-gradient(
      90deg,
      transparent 0%,
      hsl(var(--primary)) 40%,
      hsl(var(--primary) / 0.4) 70%,
      transparent 100%
    );
    background-size: 200% 100%;
    animation: shimmer 1.2s linear infinite;
    z-index: 2;
  }

  @keyframes shimmer {
    0%   { background-position: 200% center; }
    100% { background-position: -200% center; }
  }

  .table-scroll {
    overflow-x: auto;
  }

  table {
    width: 100%;
    border-collapse: collapse;
  }

  thead tr {
    border-bottom: 1px solid hsl(0 0% 100% / 0.06);
  }

  th {
    padding: 11px 16px;
    text-align: left;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.10em;
    text-transform: uppercase;
    color: hsl(var(--muted-foreground) / 0.7);
    white-space: nowrap;
  }

  td {
    padding: 0;
    border-bottom: 1px solid hsl(0 0% 100% / 0.04);
    vertical-align: middle;
  }

  tbody tr:last-child td {
    border-bottom: none;
  }

  .file-row {
    transition: background 0.1s;
  }

  .file-row:hover {
    background: hsl(0 0% 100% / 0.03);
  }

  .file-row:hover .action-btn {
    opacity: 1;
  }

  /* ── Columns ─────────────────────────────────────────────────────── */
  .col-name {
    width: 100%;
  }

  .col-size {
    min-width: 80px;
    text-align: right;
  }

  .col-actions {
    min-width: 110px;
    padding-right: 12px;
    text-align: right;
  }

  /* ── Name cell ───────────────────────────────────────────────────── */
  .name-cell {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 11px 16px;
  }

  .file-icon {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 28px;
    height: 28px;
    border-radius: 8px;
    flex-shrink: 0;
    color: hsl(var(--muted-foreground) / 0.7);
    background: hsl(0 0% 100% / 0.04);
  }

  .dir-icon {
    color: hsl(var(--primary) / 0.85);
    background: hsl(var(--primary) / 0.1);
  }

  .name-btn {
    font-size: 14px;
    font-weight: 500;
    color: hsl(var(--foreground));
    background: none;
    border: none;
    padding: 0;
    text-align: left;
    cursor: default;
    word-break: break-all;
    line-height: 1.4;
  }

  .dir-link {
    cursor: pointer;
    color: hsl(var(--foreground));
    transition: color 0.12s;
  }

  .dir-link:hover {
    color: hsl(var(--primary));
    text-decoration: underline;
    text-underline-offset: 3px;
  }

  .size-val {
    font-size: 13px;
    color: hsl(var(--muted-foreground));
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
    padding: 0 16px;
    display: block;
  }

  /* ── Action button ───────────────────────────────────────────────── */
  .action-btn {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 5px 10px;
    border-radius: 8px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.05);
    color: hsl(var(--muted-foreground));
    font-size: 12px;
    font-weight: 500;
    cursor: pointer;
    opacity: 0;
    transition: opacity 0.12s, background 0.12s, color 0.12s;
    white-space: nowrap;
  }

  .action-btn:hover {
    background: hsl(var(--primary) / 0.12);
    border-color: hsl(var(--primary) / 0.3);
    color: hsl(var(--primary));
  }

  /* ── Empty state ─────────────────────────────────────────────────── */
  .empty-state {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 10px;
    padding: 52px 16px;
    color: hsl(var(--muted-foreground) / 0.5);
    text-align: center;
  }

  .empty-state p {
    margin: 0;
    font-size: 14px;
  }

  /* ── Sidebar sections ────────────────────────────────────────────── */
  .sidebar-section {
    display: flex;
    flex-direction: column;
    gap: 12px;
    padding: 14px 16px 16px;
  }

  .sidebar-heading {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: hsl(var(--muted-foreground) / 0.7);
  }

  .stream-badge {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 18px;
    height: 18px;
    padding: 0 5px;
    border-radius: 999px;
    background: hsl(var(--primary) / 0.2);
    color: hsl(var(--primary));
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0;
  }

  /* ── Metric rows ─────────────────────────────────────────────────── */
  .metric-rows {
    display: flex;
    flex-direction: column;
    gap: 2px;
  }

  .metric-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    gap: 8px;
    padding: 8px 12px;
    border-radius: 10px;
    background: hsl(0 0% 100% / 0.025);
  }

  .metric-row:hover {
    background: hsl(0 0% 100% / 0.04);
  }

  .metric-label {
    font-size: 12px;
    color: hsl(var(--muted-foreground));
    white-space: nowrap;
  }

  .metric-val {
    font-size: 13px;
    font-weight: 600;
    color: hsl(var(--foreground));
    text-align: right;
    word-break: break-all;
  }

  .metric-val.accent {
    color: hsl(var(--primary));
  }

  /* ── Stream cards ────────────────────────────────────────────────── */
  .streams-empty {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 8px;
    padding: 28px 0 12px;
    color: hsl(var(--muted-foreground) / 0.4);
    text-align: center;
  }

  .streams-empty p {
    margin: 0;
    font-size: 13px;
  }

  .stream-list {
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .stream-card {
    padding: 11px 12px;
    border: 1px solid hsl(0 0% 100% / 0.07);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.03);
    display: flex;
    flex-direction: column;
    gap: 8px;
  }

  .stream-header {
    display: flex;
    align-items: flex-start;
    gap: 8px;
  }

  .stream-name {
    font-size: 12px;
    font-weight: 600;
    color: hsl(var(--foreground));
    word-break: break-word;
    flex: 1;
    line-height: 1.4;
  }

  .stop-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    flex-shrink: 0;
    border-radius: 6px;
    border: 1px solid hsl(var(--danger) / 0.25);
    background: hsl(var(--danger) / 0.08);
    color: hsl(0 96% 82% / 0.8);
    cursor: pointer;
    transition: background 0.12s, color 0.12s;
  }

  .stop-btn:hover {
    background: hsl(var(--danger) / 0.22);
    color: hsl(0 96% 82%);
  }

  /* ── Progress bar ────────────────────────────────────────────────── */
  .progress-wrap {
    display: flex;
    flex-direction: column;
    gap: 4px;
  }

  .progress-track {
    height: 4px;
    border-radius: 999px;
    background: hsl(0 0% 100% / 0.08);
    overflow: hidden;
  }

  .progress-fill {
    height: 100%;
    border-radius: 999px;
    background: linear-gradient(90deg, hsl(var(--primary) / 0.7), hsl(var(--primary)));
    transition: width 0.3s ease;
    min-width: 3px;
  }

  .progress-labels {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: hsl(var(--muted-foreground) / 0.6);
    font-variant-numeric: tabular-nums;
  }

  .stream-id {
    font-size: 10px;
    color: hsl(var(--muted-foreground) / 0.45);
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  /* ── Spin animation for loading icon ─────────────────────────────── */
  :global(.spin) {
    animation: spin 0.8s linear infinite;
  }

  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>
