<script lang="ts">
  import { onMount } from 'svelte';
  import ChevronLeft from '@lucide/svelte/icons/chevron-left';
  import ChevronRight from '@lucide/svelte/icons/chevron-right';
  import Activity from '@lucide/svelte/icons/activity';
  import Database from '@lucide/svelte/icons/database';
  import Layers from '@lucide/svelte/icons/layers';
  import HardDrive from '@lucide/svelte/icons/hard-drive';
  import CheckCircle from '@lucide/svelte/icons/check-circle';
  import AlertCircle from '@lucide/svelte/icons/alert-circle';
  import MediaRow from '$lib/components/MediaRow.svelte';
  import { api, subscribeEvents } from '$lib/api';
  import { detailsHref } from '$lib/detailsHref';
  import { toastError } from '$lib/toast';
  import type { DashboardHome, LibraryItem, Status } from '$lib/types';

  let home: DashboardHome | null = null;
  let status: Status | null = null;
  let loading = true;
  let heroIndex = 0;
  let heroTimer: number;

  function fmt(bytes: number) {
    if (!bytes) return '—';
    if (bytes >= 1e9) return `${(bytes / 1e9).toFixed(1)} GB`;
    if (bytes >= 1e6) return `${(bytes / 1e6).toFixed(0)} MB`;
    return `${(bytes / 1e3).toFixed(0)} KB`;
  }

  async function loadAll() {
    loading = true;
    try {
      const [dashboard, appStatus] = await Promise.all([api.dashboardHome(), api.status()]);
      home = dashboard;
      status = appStatus;
    }
    catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { loading = false; }
  }

  function startCarousel(items: LibraryItem[]) {
    clearInterval(heroTimer);
    if (items.length > 1) {
      heroTimer = window.setInterval(() => {
        heroIndex = (heroIndex + 1) % items.length;
      }, 7000);
    }
  }

  onMount(() => {
    void loadAll();
    const unsub = subscribeEvents(() => void loadAll());
    const t = window.setInterval(() => void loadAll(), 120000);
    return () => { window.clearInterval(t); window.clearInterval(heroTimer); unsub(); };
  });

  $: heroItems = ((home?.recentlyAdded ?? []).length > 0
    ? (home?.recentlyAdded ?? [])
    : [...(home?.trendingMovies ?? []), ...(home?.trendingTv ?? [])]).slice(0, 8);
  $: { if (heroItems.length) startCarousel(heroItems); }
  $: hero = heroItems[heroIndex] ?? heroItems[0];
  $: integrations = status?.integrations;
  $: heroKind = hero?.mediaType === 'episode' ? 'tv' : hero?.mediaType;
  $: heroStatus = hero?.available ? 'available' : hero?.queueState || 'tracked';
</script>

<svelte:head><title>Dashboard — Drakkar</title></svelte:head>

<!-- Hero carousel (reference: 420px/500px, full-width with backdrop+gradient) -->
{#if hero}
  <section class="hero">
    {#if hero.backdropUrl}
      <img class="hero-bg" src={hero.backdropUrl} alt="" />
    {/if}
    <div class="hero-gradient"></div>
    <div class="hero-content">
      <div class="hero-tags">
        <span class="tag">{heroKind}</span>
        {#if hero.year}<span class="tag">{hero.year}</span>{/if}
        <span class="tag">{heroStatus}</span>
        {#if hero.availableCount || hero.missingCount}
          <span class="tag">{hero.availableCount ?? 0} avail / {hero.missingCount ?? 0} miss</span>
        {/if}
      </div>
      <h1 class="hero-title">{hero.title}</h1>
      {#if hero.overview}
        <p class="hero-overview">{hero.overview}</p>
      {/if}
      <div class="hero-actions">
        <a class="hero-btn primary" href={detailsHref(hero)}>More Info</a>
        {#if hero.id}
          <a class="hero-btn secondary" href={`/library/${hero.id}`}>Open Library</a>
        {/if}
        <a class="hero-btn secondary" href={heroKind === 'tv' ? '/discover/tv' : '/discover/movie'}>Browse {heroKind === 'tv' ? 'TV' : 'Movies'}</a>
      </div>
    </div>
    {#if heroItems.length > 1}
      <button class="hero-nav left" aria-label="Previous hero" on:click={() => { heroIndex = (heroIndex - 1 + heroItems.length) % heroItems.length; }}>
        <ChevronLeft size={18} />
      </button>
      <button class="hero-nav right" aria-label="Next hero" on:click={() => { heroIndex = (heroIndex + 1) % heroItems.length; }}>
        <ChevronRight size={18} />
      </button>
      <div class="hero-dots">
        {#each heroItems as _, i}
          <button aria-label={`Hero ${i + 1}`} class:active={i === heroIndex} on:click={() => (heroIndex = i)}></button>
        {/each}
      </div>
    {/if}
  </section>
{:else if loading}
  <div class="hero-placeholder">Loading dashboard…</div>
{/if}

<div class="dashboard-body">
  <!-- System status tiles -->
  {#if status}
    <div class="stat-band">
      <div class="stat-tile">
        <HardDrive size={14} />
        <div>
          <div class="st-label">Disk Cache</div>
          <div class="st-value">{fmt(status.diskCacheLimitBytes)} limit</div>
        </div>
      </div>
      <div class="stat-tile">
        <Layers size={14} />
        <div>
          <div class="st-label">Read-ahead</div>
          <div class="st-value">{fmt(status.readAheadLimitBytes)}</div>
        </div>
      </div>
      <div class="stat-tile">
        <Database size={14} />
        <div>
          <div class="st-label">Hot Cache</div>
          <div class="st-value">{fmt(status.memoryHotCacheBytes)}</div>
        </div>
      </div>
      <div class="stat-tile">
        <Activity size={14} />
        <div>
          <div class="st-label">FUSE Mount</div>
          <div class="st-value ellipsis">{status.fuseMountPath || '—'}</div>
        </div>
      </div>
    </div>

    <!-- Integration health -->
    {#if integrations}
      <div class="integrations">
        {#each Object.entries(integrations) as [name, info]}
          {#if name !== 'subtitleProviders' && typeof info === 'object' && info !== null && !Array.isArray(info) && 'enabled' in info}
            <div class="int-chip" class:disabled={!info.enabled}>
              <svelte:component
                this={info.enabled && info.configured ? CheckCircle : AlertCircle}
                size={12}
                class={info.enabled && info.configured ? 'ok' : 'warn'}
              />
              <span>{name}</span>
              <span class="int-status" class:ok={info.enabled && info.configured}>
                {!info.enabled ? 'off' : info.configured ? 'ok' : 'not configured'}
              </span>
            </div>
          {/if}
        {/each}
      </div>
    {/if}
  {/if}

  {#if (home?.recentlyAdded ?? []).length > 0}
    <MediaRow
      title="Recently Added"
      subtitle="Newest linked media from local library state."
      items={home?.recentlyAdded ?? []}
      href="/library"
      linkLabel="View Library →"
      itemWidth={140}
    />
  {/if}

  {#if (home?.trendingMovies ?? []).length > 0}
    <MediaRow
      title="Trending Movies"
      subtitle="Popular movies from TMDB."
      items={home?.trendingMovies ?? []}
      href="/discover/movie"
      linkLabel="Browse →"
      itemWidth={140}
    />
  {/if}

  {#if (home?.trendingTv ?? []).length > 0}
    <MediaRow
      title="Trending TV Shows"
      subtitle="Popular TV shows from TMDB."
      items={home?.trendingTv ?? []}
      href="/discover/tv"
      linkLabel="Browse →"
      itemWidth={140}
    />
  {/if}

  {#if !loading && !home?.recentlyAdded?.length && !(home?.trendingMovies ?? []).length}
    <div class="empty-state">No media yet. Sync Seerr requests to get started.</div>
  {/if}
</div>

<style>
  /* ── Hero ─────────────────────────────────────────────────────────────────── */
  .hero {
    position: relative;
    height: 420px;
    overflow: hidden;
    border-radius: var(--radius-2xl, 1.5rem);
    border: 1px solid hsl(0 0% 100% / 0.07);
    margin-bottom: 28px;
  }

  @media (min-width: 900px) { .hero { height: 500px; } }

  .hero-bg {
    position: absolute; inset: 0;
    width: 100%; height: 100%;
    object-fit: cover;
  }

  .hero-gradient {
    position: absolute; inset: 0;
    background:
      linear-gradient(90deg, hsl(215 36% 4% / 0.95) 0%, hsl(215 36% 4% / 0.65) 50%, transparent 100%),
      linear-gradient(0deg, hsl(215 36% 4% / 0.6) 0%, transparent 50%);
  }

  .hero-content {
    position: relative; z-index: 1;
    display: flex; flex-direction: column; justify-content: flex-end;
    height: 100%; max-width: 680px; padding: 32px 36px 32px 72px;
  }

  .hero-tags { display: flex; gap: 8px; margin-bottom: 12px; }

  .tag {
    padding: 4px 10px; border-radius: 999px;
    border: 1px solid hsl(0 0% 100% / 0.15);
    background: hsl(0 0% 100% / 0.08);
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.06em;
  }

  .hero-title {
    font-size: clamp(1.8rem, 4vw, 3rem);
    font-weight: 700; line-height: 1.05;
    margin: 0 0 10px;
  }

  .hero-overview {
    margin: 0 0 20px;
    font-size: 14px; line-height: 1.65;
    color: hsl(var(--foreground) / 0.8);
    max-width: 560px;
    display: -webkit-box; line-clamp: 3; -webkit-line-clamp: 3; -webkit-box-orient: vertical;
    overflow: hidden;
  }

  .hero-actions {
    display: flex;
    flex-wrap: wrap;
    gap: 10px;
    margin-top: 2px;
  }

  .hero-btn {
    display: inline-flex; align-items: center;
    height: 42px; padding: 0 20px;
    border-radius: var(--radius-lg, 0.75rem);
    font-size: 13px; font-weight: 700;
    text-decoration: none;
    transition: opacity 0.15s;
  }
  .hero-btn.primary {
    background: hsl(var(--primary));
    color: hsl(var(--primary-foreground));
  }
  .hero-btn.secondary {
    background: hsl(0 0% 100% / 0.08);
    border: 1px solid hsl(0 0% 100% / 0.1);
    color: hsl(var(--foreground));
  }
  .hero-btn:hover { opacity: 0.88; }

  .hero-nav {
    position: absolute; top: 50%; z-index: 2;
    transform: translateY(-50%);
    width: 40px; height: 40px;
    display: grid; place-items: center;
    border-radius: 999px;
    border: 1px solid hsl(0 0% 100% / 0.15);
    background: hsl(0 0% 0% / 0.4);
    color: hsl(var(--foreground)); cursor: pointer;
  }
  .hero-nav.left  { left: 14px; }
  .hero-nav.right { right: 14px; }

  .hero-dots {
    position: absolute; bottom: 16px; left: 50%; z-index: 2;
    transform: translateX(-50%);
    display: flex; gap: 6px;
    padding: 8px 12px; border-radius: 999px;
    background: hsl(0 0% 0% / 0.35);
    border: 1px solid hsl(0 0% 100% / 0.1);
  }
  .hero-dots button {
    width: 14px; height: 4px; border-radius: 999px;
    background: hsl(0 0% 100% / 0.3); border: 0; cursor: pointer;
    transition: all 0.2s;
  }
  .hero-dots button.active { width: 28px; background: hsl(var(--primary)); }

  .hero-placeholder {
    height: 200px; display: grid; place-items: center;
    color: hsl(var(--muted-foreground)); font-size: 14px;
    margin-bottom: 28px;
  }

  /* ── Dashboard body ───────────────────────────────────────────────────────── */
  .dashboard-body { display: flex; flex-direction: column; gap: 24px; }

  /* Stat band */
  .stat-band {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
  }
  @media (max-width: 700px) { .stat-band { grid-template-columns: repeat(2, minmax(0, 1fr)); } }

  .stat-tile {
    display: flex; align-items: center; gap: 12px;
    padding: 14px 16px;
    border-radius: var(--radius-lg, 0.75rem);
    border: 1px solid hsl(0 0% 100% / 0.07);
    background: hsl(var(--card) / 0.8);
    color: hsl(var(--primary));
  }
  .st-label { font-size: 11px; color: hsl(var(--muted-foreground)); font-weight: 600; margin-bottom: 3px; }
  .st-value { font-size: 13px; font-weight: 700; color: hsl(var(--foreground)); }
  .ellipsis { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 120px; font-size: 11px; }

  /* Integrations */
  .integrations { display: flex; flex-wrap: wrap; gap: 8px; }

  .int-chip {
    display: flex; align-items: center; gap: 6px;
    padding: 6px 12px;
    border-radius: var(--radius-lg, 0.75rem);
    border: 1px solid hsl(0 0% 100% / 0.07);
    background: hsl(var(--card) / 0.7);
    font-size: 12px; font-weight: 600;
  }
  .int-chip.disabled { opacity: 0.5; }
  .int-status { font-size: 11px; color: hsl(var(--muted-foreground)); }
  .int-status.ok { color: hsl(var(--status-available, 142 70% 45%)); }

  .empty-state {
    padding: 32px; text-align: center;
    border-radius: var(--radius-xl, 1rem);
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.02);
    color: hsl(var(--muted-foreground));
    font-size: 14px;
  }

</style>
