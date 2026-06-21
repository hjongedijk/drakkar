<script lang="ts">
  import { onMount } from 'svelte';
  import { goto } from '$app/navigation';
  import { page } from '$app/state';
  import SearchIcon from '@lucide/svelte/icons/search';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import SearchCheck from '@lucide/svelte/icons/search-check';
  import Play from '@lucide/svelte/icons/play';
  import Pagination from '$lib/components/Pagination.svelte';
  import PosterCard from '$lib/components/PosterCard.svelte';
  import MetricCard from '$lib/components/MetricCard.svelte';
  import Button from '$lib/components/Button.svelte';
  import { api, subscribeEvents } from '$lib/api';
  import { toastError, toastSuccess } from '$lib/toast';
  import type { LibraryItem, LibraryPage, Status } from '$lib/types';

  let items: LibraryItem[] = [];
  let libraryPage: LibraryPage = { items: [], page: 1, pageSize: 40, total: 0, totalPages: 1, totalMonitored: 0, sumAvailable: 0, sumMissing: 0, countActive: 0 };
  let status: Status | null = null;
  let loading = true;
  let working = false;
  let query = '';
  let kind = 'all';
  let stateFilter = 'all';
  let currentPage = 1;
  const pageSize = 40;

  async function loadLibrary() {
    loading = true;
    try {
      const [statusResult, result] = await Promise.all([
        api.status(),
        api.library({ page: currentPage, pageSize, q: query.trim(), kind, state: stateFilter })
      ]);
      status = statusResult;
      libraryPage = result;
      items = result.items;
    } catch (err) {
      toastError(err instanceof Error ? err.message : String(err));
    } finally {
      loading = false;
    }
  }

  async function syncRequests() {
    working = true;
    try {
      const r = await api.syncRequests();
      toastSuccess(`Synced — ${r.created} new`);
      await loadLibrary();
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { working = false; }
  }

  async function processPending() {
    working = true;
    try {
      const r = await api.searchPendingLibrary();
      toastSuccess(`Searched ${r.searched} — selected ${r.selected}`);
      await loadLibrary();
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { working = false; }
  }

  // Sync current state back to the URL for bookmarking / browser history.
  function syncUrl() {
    const url = new URL(page.url);
    if (kind === 'all') url.searchParams.delete('kind');
    else url.searchParams.set('kind', kind);
    if (query.trim()) url.searchParams.set('q', query.trim());
    else url.searchParams.delete('q');
    if (stateFilter === 'all') url.searchParams.delete('state');
    else url.searchParams.set('state', stateFilter);
    if (currentPage <= 1) url.searchParams.delete('page');
    else url.searchParams.set('page', String(currentPage));
    void goto(`${url.pathname}?${url.searchParams.toString()}`, { replaceState: true, noScroll: true, keepFocus: true });
  }

  function updateFilters(next: { kind?: string; q?: string; state?: string }) {
    kind = next.kind ?? kind;
    query = next.q ?? query;
    stateFilter = next.state ?? stateFilter;
    currentPage = 1;
    void loadLibrary();
    syncUrl();
  }

  function changePage(nextPage: number) {
    currentPage = nextPage;
    void loadLibrary();
    syncUrl();
  }

  onMount(() => {
    // Seed state from URL so bookmarked/shared links work.
    kind = page.url.searchParams.get('kind') ?? 'all';
    query = page.url.searchParams.get('q') ?? '';
    stateFilter = page.url.searchParams.get('state') ?? 'all';
    currentPage = Number(page.url.searchParams.get('page') ?? '1') || 1;

    void loadLibrary();

    const unsub = subscribeEvents(() => { if (!working) void loadLibrary(); });
    const t = window.setInterval(() => void loadLibrary(), 30000);
    return () => { window.clearInterval(t); unsub(); };
  });

  $: seerrReady = status?.integrations?.seerr?.configured ?? false;
  $: hydraReady = status?.integrations?.nzbhydra2?.configured ?? false;

  $: totalPages = Math.max(1, libraryPage.totalPages || 1);
  $: pagedItems = items;
  $: rangeStart = libraryPage.total ? (libraryPage.page - 1) * libraryPage.pageSize + 1 : 0;
  $: rangeEnd = Math.min(libraryPage.page * libraryPage.pageSize, libraryPage.total);

  $: totalAvailable = libraryPage.sumAvailable;
  $: totalMissing   = libraryPage.sumMissing;
  $: activeCount    = libraryPage.countActive;
</script>

<svelte:head><title>Library — Drakkar</title></svelte:head>

<div class="page">
  <!-- Page header -->
  <div class="page-head">
    <div>
      <h1>Library</h1>
      <p>All monitored titles — requested from Seerr, queued, and available.</p>
    </div>
    <div class="actions">
      <button class="icon-btn" on:click={loadLibrary} disabled={loading || working} title="Refresh">
        <RefreshCw size={15} />
      </button>
      <Button kind="secondary" on:click={syncRequests} disabled={loading || working || !seerrReady}
        title={!seerrReady ? 'Seerr not configured' : ''}>
        <SearchCheck size={14} /> Sync Seerr
      </Button>
      <Button kind="secondary" on:click={processPending} disabled={loading || working || !hydraReady}
        title={!hydraReady ? 'NZBHydra2 not configured' : ''}>
        <Play size={14} /> Search Pending
      </Button>
    </div>
  </div>

  <!-- Metric band (clickable filter tiles) -->
  <div class="metric-band">
    <button class="metric-wrap" class:active={stateFilter === 'all'} on:click={() => void updateFilters({ state: 'all' })}>
      <MetricCard label="Monitored" value={libraryPage.totalMonitored} detail="titles tracked" />
    </button>
    <button class="metric-wrap metric-available" class:active={stateFilter === 'available'} on:click={() => void updateFilters({ state: 'available' })}>
      <MetricCard label="Available" value={totalAvailable} detail="movies + episodes" accent />
    </button>
    <button class="metric-wrap metric-active" class:active={stateFilter === 'active'} on:click={() => void updateFilters({ state: 'active' })}>
      <MetricCard label="Downloading" value={activeCount} detail="in queue" />
    </button>
    <button class="metric-wrap metric-missing" class:active={stateFilter === 'missing'} on:click={() => void updateFilters({ state: 'missing' })}>
      <MetricCard label="Missing" value={totalMissing} detail="movies + episodes" />
    </button>
  </div>

  <!-- Status legend — matches reference Library.tsx groupStatus() -->
  <div class="legend">
    <span class="legend-item"><span class="dot dot-available"></span>Completed</span>
    <span class="legend-item"><span class="dot dot-active"></span>Downloading</span>
    <span class="legend-item"><span class="dot dot-unreleased"></span>Queued</span>
    <span class="legend-item"><span class="dot dot-missing"></span>Missing</span>
  </div>

  <!-- Filter bar -->
  <div class="filter-bar">
    <div class="search-wrap">
      <SearchIcon size={14} class="search-icon" />
      <input
        bind:value={query}
        placeholder="Search titles…"
        on:change={() => void updateFilters({ q: query })}
        on:keydown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault();
            void updateFilters({ q: query });
          }
        }}
      />
    </div>
    <div class="kind-tabs">
      <button class:on={kind === 'all'}   on:click={() => void updateFilters({ kind: 'all' })}>All</button>
      <button class:on={kind === 'movie'} on:click={() => void updateFilters({ kind: 'movie' })}>Movies</button>
      <button class:on={kind === 'tv'}    on:click={() => void updateFilters({ kind: 'tv' })}>TV</button>
    </div>
  </div>

  <!-- Poster grid -->
  {#if pagedItems.length > 0}
    <div class="pager">
      <div class="pager-copy">Showing {rangeStart}-{rangeEnd} of {libraryPage.total}</div>
      <div class="pager-actions">
        <Pagination page={currentPage} {totalPages} showFirstLast={false} on:change={(e) => void changePage(e.detail)} />
      </div>
    </div>
    <div class="poster-grid">
      {#each pagedItems as item}
        <PosterCard {item} />
      {/each}
    </div>
  {:else if loading}
    <div class="empty">Loading library…</div>
  {:else}
    <div class="empty">No titles match the current filter.</div>
  {/if}
</div>

<style>
  .page { display: flex; flex-direction: column; gap: 20px; }

  .page-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    flex-wrap: wrap;
  }

  p { margin: 6px 0 0; color: hsl(var(--muted-foreground)); font-size: 14px; }

  .actions { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }

  .icon-btn {
    display: grid; place-items: center;
    width: 38px; height: 38px;
    border-radius: var(--radius-md, 0.5rem);
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--muted-foreground));
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
  }
  .icon-btn:hover { background: hsl(0 0% 100% / 0.1); color: hsl(var(--foreground)); }
  .icon-btn:disabled { opacity: 0.45; cursor: default; }

  /* Metric band */
  .metric-band {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
  }

  .metric-wrap {
    border: none;
    background: transparent;
    padding: 0;
    cursor: pointer;
    border-radius: var(--radius-lg, 0.75rem);
    outline: 2px solid transparent;
    outline-offset: 2px;
    transition: outline-color 0.15s;
    text-align: left;
  }
  .metric-wrap.active,
  .metric-wrap:hover { outline-color: hsl(var(--primary) / 0.45); }

  /* Filter bar */
  .filter-bar {
    display: flex;
    gap: 10px;
    align-items: center;
    flex-wrap: wrap;
    padding: 10px 14px;
    border-radius: var(--radius-xl, 1rem);
    border: 1px solid hsl(0 0% 100% / 0.07);
    background: hsl(var(--card) / 0.6);
  }

  .search-wrap {
    flex: 1;
    min-width: 160px;
    position: relative;
    display: flex;
    align-items: center;
  }

  .search-wrap :global(.search-icon) {
    position: absolute;
    left: 10px;
    color: hsl(var(--muted-foreground));
    pointer-events: none;
  }

  input {
    width: 100%;
    height: 38px;
    padding: 0 12px 0 32px;
    border-radius: var(--radius-md, 0.5rem);
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.03);
    color: hsl(var(--foreground));
    font-size: 13px;
  }
  input::placeholder { color: hsl(var(--muted-foreground)); }

  .kind-tabs { display: flex; gap: 3px; }

  .kind-tabs button {
    height: 36px; padding: 0 14px;
    border-radius: var(--radius-md, 0.5rem);
    border: 1px solid transparent;
    background: transparent;
    color: hsl(var(--muted-foreground));
    font-size: 13px; font-weight: 600;
    cursor: pointer;
    transition: all 0.15s;
  }
  .kind-tabs button.on,
  .kind-tabs button:hover {
    background: hsl(0 0% 100% / 0.08);
    color: hsl(var(--foreground));
    border-color: hsl(0 0% 100% / 0.06);
  }

  /* Poster grid — 2 → 4 → 5 → 6 → 8 → 10 columns */
  .pager {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    color: hsl(var(--muted-foreground));
    font-size: 13px;
  }

  .pager-actions {
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }

  .poster-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
  }

  @media (min-width: 480px)  { .poster-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
  @media (min-width: 700px)  { .poster-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); } }
  @media (min-width: 900px)  { .poster-grid { grid-template-columns: repeat(5, minmax(0, 1fr)); } }
  @media (min-width: 1100px) { .poster-grid { grid-template-columns: repeat(6, minmax(0, 1fr)); } }
  @media (min-width: 1400px) { .poster-grid { grid-template-columns: repeat(8, minmax(0, 1fr)); } }
  @media (min-width: 1700px) { .poster-grid { grid-template-columns: repeat(10, minmax(0, 1fr)); } }

  @media (max-width: 700px) { .metric-band { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
  @media (max-width: 700px) {
    .pager {
      flex-direction: column;
      align-items: flex-start;
    }
  }

  /* Legend */
  .legend {
    display: flex;
    flex-wrap: wrap;
    gap: 14px;
    font-size: 12px;
    color: hsl(var(--muted-foreground));
  }
  .legend-item { display: flex; align-items: center; gap: 6px; }
  .dot {
    width: 10px; height: 10px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  .dot-available   { background: hsl(var(--status-available)); }
  .dot-active      { background: hsl(var(--status-downloading)); }
  .dot-unreleased  { background: hsl(var(--status-unreleased)); }
  .dot-missing     { background: hsl(var(--status-missing)); }

  .empty {
    padding: 32px;
    border-radius: var(--radius-xl, 1rem);
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.02);
    color: hsl(var(--muted-foreground));
    text-align: center;
    font-size: 14px;
  }
</style>
