<script lang="ts">
  import { page } from '$app/state';
  import Search from '@lucide/svelte/icons/search';
  import RotateCcw from '@lucide/svelte/icons/rotate-ccw';
  import Languages from '@lucide/svelte/icons/languages';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import Tv from '@lucide/svelte/icons/tv';
  import Download from '@lucide/svelte/icons/download';
  import X from '@lucide/svelte/icons/x';
  import Button from '$lib/components/Button.svelte';
  import PosterCard from '$lib/components/PosterCard.svelte';
  import StatusPill from '$lib/components/StatusPill.svelte';
  import { api } from '$lib/api';
  import { idFromSlug } from '$lib/detailsHref';
  import { toastError, toastSuccess } from '$lib/toast';
  import type { DiscoverDetails, GrabHistoryEntry, LibraryDetail, LibraryItem, ReleaseItem, SubtitleCandidate, SubtitleFile } from '$lib/types';

  let detail: DiscoverDetails | null = null;
  let libraryMatch: LibraryItem | null = null;
  let localDetail: LibraryDetail | null = null;
  let subtitles: SubtitleFile[] = [];
  let subtitleCandidates: SubtitleCandidate[] = [];
  let grabHistory: GrabHistoryEntry[] = [];
  let releaseCandidates: ReleaseItem[] = [];
  let showReleasePicker = false;
  let loading = true;
  let working = false;
  let activeKey = '';

  function fmtBytes(b: number): string {
    if (b >= 1e9) return (b / 1e9).toFixed(1) + ' GB';
    return Math.round(b / 1e6) + ' MB';
  }

  function qualityTags(title: string): string[] {
    const rules: [RegExp, string][] = [
      [/\b2160p\b/i, '2160p'], [/\b4k\b/i, '4K'], [/\b1080p\b/i, '1080p'],
      [/\b720p\b/i, '720p'], [/\b480p\b/i, '480p'],
      [/bluray|bdremux|bdrip/i, 'BluRay'], [/\bweb[- ]?dl\b/i, 'WEB-DL'],
      [/\bwebrip\b/i, 'WEBRip'], [/hevc|x265|h\.265/i, 'x265'],
      [/\bx264\b|h\.264/i, 'x264'], [/\bhdr\b/i, 'HDR'],
      [/dolby.?vision|\bDV\b/, 'DV'],
    ];
    const seen = new Set<string>();
    const out: string[] = [];
    for (const [re, label] of rules) {
      if (re.test(title) && !seen.has(label)) { seen.add(label); out.push(label); }
    }
    return out;
  }

  function normalizeTitle(value: string) {
    return value.toLowerCase().replace(/['’]/g, '').replace(/[^a-z0-9]+/g, ' ').trim();
  }

  function sameIdentity(item: LibraryItem, mediaType: string, title: string, year?: number, tmdbId?: number, imdbId?: string) {
    const mapped = item.mediaType === 'episode' ? 'tv' : item.mediaType;
    if (mapped !== mediaType) return false;
    if (tmdbId && item.tmdbId === tmdbId) return true;
    if (imdbId && item.imdbId === imdbId) return true;
    return normalizeTitle(item.title) === normalizeTitle(title) && (!!year ? item.year === year : true);
  }

  async function loadDetail() {
    loading = true;
    try {
      const mediaType = page.params.mediaType === 'tv' ? 'tv' : 'movie';
      const tmdbSlug = idFromSlug(page.params.idSlug);
      const tmdbId = tmdbSlug && /^\d+$/.test(tmdbSlug) ? Number(tmdbSlug) : undefined;
      const imdbId = tmdbSlug && /^tt/i.test(tmdbSlug) ? tmdbSlug : undefined;
      const title = page.url.searchParams.get('title') ?? undefined;
      const year = page.url.searchParams.get('year') ? Number(page.url.searchParams.get('year')) : undefined;
      const [discover, library] = await Promise.all([
        api.discoverDetails(mediaType, { title, year, tmdbId, imdbId }),
        api.library()
      ]);
      detail = discover;
      libraryMatch = library.items.find((item) => sameIdentity(item, mediaType, discover.title, discover.year, discover.tmdbId, discover.imdbId)) ?? null;
      if (libraryMatch) {
        const [detailResult, subtitleResult, candidateResult, historyResult] = await Promise.all([
          api.libraryDetail(libraryMatch.id),
          api.subtitles(libraryMatch.id),
          api.subtitleCandidates(libraryMatch.id),
          api.grabHistory(libraryMatch.id).catch(() => ({ items: [] }))
        ]);
        localDetail = detailResult;
        subtitles = subtitleResult.items ?? [];
        subtitleCandidates = candidateResult.items ?? [];
        grabHistory = historyResult.items ?? [];
      } else {
        localDetail = null;
        subtitles = [];
        subtitleCandidates = [];
        grabHistory = [];
      }
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
      detail = null;
      libraryMatch = null;
      localDetail = null;
      subtitles = [];
      subtitleCandidates = [];
    } finally {
      loading = false;
    }
  }

  $: {
    const nextKey = `${page.params.mediaType}:${page.params.idSlug}:${page.url.search}`;
    if (nextKey !== activeKey) {
      activeKey = nextKey;
      void loadDetail();
    }
  }

  async function runLocalSearch() {
    if (!libraryMatch) return;
    working = true;
    releaseCandidates = [];
    try {
      await api.searchLibrary(libraryMatch.id);
      const result = await api.releases(libraryMatch.id);
      releaseCandidates = (result.items ?? []).sort((a, b) => b.score - a.score);
      showReleasePicker = true;
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
    } finally {
      working = false;
    }
  }

  async function pickRelease(candidateId: number) {
    working = true;
    try {
      await api.selectRelease(candidateId);
      showReleasePicker = false;
      await loadDetail();
      toastSuccess('Release selected');
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
    } finally {
      working = false;
    }
  }

  async function runRepublish() {
    if (!libraryMatch) return;
    working = true;
    try {
      await api.republishLibrary(libraryMatch.id);
      toastSuccess('republished library item');
      await loadDetail();
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
    } finally {
      working = false;
    }
  }

  async function runSubtitleSearch(forLibraryItemId?: number) {
    const itemId = forLibraryItemId ?? libraryMatch?.id;
    if (!itemId) return;
    working = true;
    try {
      const result = await api.searchSubtitles(itemId, ['nl', 'en']);
      toastSuccess(`subtitle candidates=${result.candidateCount}`);
      await loadDetail();
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
    } finally {
      working = false;
    }
  }

  async function downloadSubtitle(candidateID: number) {
    if (!libraryMatch) return;
    working = true;
    try {
      await api.downloadSubtitleCandidate(candidateID);
      toastSuccess('subtitle downloaded');
      await loadDetail();
    } catch (error) {
      toastError(error instanceof Error ? error.message : String(error));
    } finally {
      working = false;
    }
  }
</script>

<svelte:head><title>{detail?.title ?? 'Details'} — Drakkar</title></svelte:head>

{#if loading}
  <div class="empty">Loading details…</div>
{:else if detail}
  <div class="page">
    <section class="hero">
      {#if detail.backdropUrl}<img class="hero-bg" src={detail.backdropUrl} alt="" />{/if}
      <div class="hero-shade"></div>
      <div class="hero-grid">
        <div class="poster">
          {#if detail.posterUrl}
            <img src={detail.posterUrl} alt="" />
          {:else}
            <div class="poster-fallback"><Tv size={28} /></div>
          {/if}
        </div>
        <div class="copy">
          <div class="badge-row">
            <StatusPill tone="neutral">{detail.mediaType}</StatusPill>
            {#if detail.year}<StatusPill tone="neutral">{detail.year}</StatusPill>{/if}
            {#if detail.originalLanguage}<StatusPill tone="neutral">{detail.originalLanguage.toUpperCase()}</StatusPill>{/if}
            {#if libraryMatch}
              <StatusPill tone={libraryMatch.available ? 'ok' : libraryMatch.queueState === 'failed' ? 'danger' : 'neutral'}>
                {libraryMatch.available ? 'in library' : libraryMatch.queueState}
              </StatusPill>
            {:else}
              <StatusPill tone="neutral">not in library</StatusPill>
            {/if}
          </div>
          <h1>{detail.title}</h1>
          {#if detail.tagline}<div class="tagline">{detail.tagline}</div>{/if}
          {#if detail.overview}<p>{detail.overview}</p>{/if}
          <div class="action-row">
            {#if libraryMatch}
              <Button kind="secondary" on:click={runLocalSearch} disabled={working}>
                <Search size={15} />
                Search
              </Button>
              <Button kind="secondary" on:click={() => runSubtitleSearch()} disabled={working}>
                <Languages size={15} />
                Subs
              </Button>
              <Button kind="secondary" on:click={runRepublish} disabled={working}>
                <RotateCcw size={15} />
                Republish
              </Button>
            {/if}
            <a class="link-btn secondary" href="/search">Back To Search</a>
            <Button kind="ghost" on:click={loadDetail} disabled={working || loading}>
              <RefreshCw size={15} />
              Refresh
            </Button>
          </div>
        </div>
      </div>
    </section>

    <section class="grid">
      <div class="main">
        <section class="panel stats">
          <h2>Details</h2>
          <div class="stat-grid">
            <div><span>Rating</span><strong>{detail.voteAverage ? detail.voteAverage.toFixed(1) : '—'}</strong></div>
            <div><span>Votes</span><strong>{detail.voteCount || '—'}</strong></div>
            <div><span>Runtime</span><strong>{detail.runtimeMinutes ? `${detail.runtimeMinutes}m` : '—'}</strong></div>
            <div><span>Status</span><strong>{detail.status || '—'}</strong></div>
            <div><span>Language</span><strong>{detail.originalLanguage?.toUpperCase() || '—'}</strong></div>
            <div><span>Companies</span><strong>{detail.productionCompanies?.length || '—'}</strong></div>
          </div>
          {#if detail.genres?.length}
            <div class="chips genre-chips">{#each detail.genres as genre}<StatusPill tone="neutral">{genre}</StatusPill>{/each}</div>
          {/if}
        </section>

        {#if localDetail?.mediaType !== 'movie' && localDetail?.seasons?.length}
          <section class="panel">
            <h2>Local Seasons</h2>
            <div class="season-stack">
              {#each localDetail.seasons as season}
                <details class="season-panel" open={season.missingCount > 0}>
                  <summary>
                    <strong>{season.name}</strong>
                    <div class="summary-meta">{season.availableCount}/{season.episodeCount} available · {season.missingCount} missing</div>
                  </summary>
                  <div class="episode-list">
                    {#each season.episodes as episode}
                      <div class="episode-row">
                        <div class="ep-info">
                          <span class="ep-code">E{String(episode.episodeNumber).padStart(2, '0')}</span>
                          {#if episode.title}
                            <span class="ep-title">{episode.title}</span>
                          {/if}
                        </div>
                        <div class="ep-right">
                          <StatusPill tone={episode.status === 'available' ? 'ok' : 'neutral'}>{episode.status}</StatusPill>
                          {#if episode.status === 'available' && episode.libraryItemId}
                            {@const epId = episode.libraryItemId}
                            <button
                              class="ep-sub-btn"
                              title="Download subtitle for this episode"
                              disabled={working}
                              on:click={() => runSubtitleSearch(epId)}
                            >🌐 Subs</button>
                          {/if}
                        </div>
                      </div>
                    {/each}
                  </div>
                </details>
              {/each}
            </div>
          </section>
        {/if}

        {#if detail.cast?.length}
          <section class="panel">
            <h2>Cast</h2>
            <div class="drag-scroll media-strip">
              {#each detail.cast.slice(0, 12) as person}
                <div class="person-slot">
                  <div class="person-card">
                    <div class="person-photo">{#if person.profileUrl}<img src={person.profileUrl} alt="" />{/if}</div>
                    <strong>{person.name}</strong>
                    <span>{person.character || 'cast'}</span>
                  </div>
                </div>
              {/each}
            </div>
          </section>
        {/if}

        {#if detail.recommendations?.length}
          <section class="panel">
            <h2>Recommendations</h2>
            <div class="drag-scroll media-strip">
              {#each detail.recommendations as item}
                <div class="poster-slot">
                  <PosterCard item={{ id:0, mediaType:item.mediaType, title:item.title, year:item.year, overview:item.overview, posterUrl:item.posterUrl, backdropUrl:item.backdropUrl, available:false, requestedAt:'', queueState:'requested', failureReason:'', tmdbId:item.tmdbId, imdbId:item.imdbId }} showStatus={false} href={`/details/${item.mediaType === 'tv' ? 'tv' : 'movie'}/${item.tmdbId}-${item.title.toLowerCase().replace(/[^a-z0-9]+/g,'-')}`} compact />
                </div>
              {/each}
            </div>
          </section>
        {/if}

        {#if detail.similar?.length}
          <section class="panel">
            <h2>Similar</h2>
            <div class="drag-scroll media-strip">
              {#each detail.similar as item}
                <div class="poster-slot">
                  <PosterCard item={{ id:0, mediaType:item.mediaType, title:item.title, year:item.year, overview:item.overview, posterUrl:item.posterUrl, backdropUrl:item.backdropUrl, available:false, requestedAt:'', queueState:'requested', failureReason:'', tmdbId:item.tmdbId, imdbId:item.imdbId }} showStatus={false} href={`/details/${item.mediaType === 'tv' ? 'tv' : 'movie'}/${item.tmdbId}-${item.title.toLowerCase().replace(/[^a-z0-9]+/g,'-')}`} compact />
                </div>
              {/each}
            </div>
          </section>
        {/if}
      </div>

      <aside class="side">
        <section class="panel">
          <h2>Library State</h2>
          {#if libraryMatch}
            <div class="kv">
              <div><span>Presence</span><strong>{libraryMatch.available ? 'Available' : 'Tracked'}</strong></div>
              <div><span>Queue</span><strong>{libraryMatch.queueState || '—'}</strong></div>
              <div><span>Available</span><strong>{libraryMatch.availableCount ?? 0}</strong></div>
              <div><span>Missing</span><strong>{libraryMatch.missingCount ?? 0}</strong></div>
            </div>
            {#if libraryMatch.failureReason}
              <div class="failure-box">{libraryMatch.failureReason.replaceAll('_', ' ')}</div>
            {/if}
            {#if localDetail?.tvShowId}
              <div class="monitoring-row">
                <label for="monitoring-select">Monitoring</label>
                <select
                  id="monitoring-select"
                  value={localDetail.monitoringMode ?? 'all'}
                  on:change={async (e) => {
                    if (!localDetail?.tvShowId) return;
                    const mode = (e.currentTarget as HTMLSelectElement).value;
                    try {
                      await api.setTVShowMonitoring(localDetail.tvShowId, mode);
                      localDetail = { ...localDetail, monitoringMode: mode };
                    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
                  }}
                >
                  <option value="all">All episodes</option>
                  <option value="future">Future only</option>
                  <option value="missing">Missing only</option>
                  <option value="recent">Recent (30d)</option>
                  <option value="pilot">Pilot only</option>
                  <option value="none">None (paused)</option>
                </select>
              </div>
            {/if}
          {:else}
            <div class="empty-side">No local library item linked yet.</div>
          {/if}
        </section>

        <section class="panel">
          <h2>Source</h2>
          <div class="kv">
            <div><span>TMDB</span><strong>{detail.tmdbId || '—'}</strong></div>
            <div><span>IMDb</span><strong>{detail.imdbId || '—'}</strong></div>
            <div><span>Network</span><strong>{detail.network || '—'}</strong></div>
            <div><span>Seasons</span><strong>{detail.numberOfSeasons || '—'}</strong></div>
          </div>
        </section>

        {#if libraryMatch}
          <section class="panel">
            <h2>Subtitles</h2>
            {#if subtitles.length > 0}
              <div class="stack-list">
                {#each subtitles as subtitle}
                  <div class="stack-item">
                    <div>
                      <strong>{subtitle.language.toUpperCase()}</strong>
                      <span>{subtitle.provider}</span>
                    </div>
                    <StatusPill tone="neutral">published</StatusPill>
                  </div>
                {/each}
              </div>
            {:else}
              <div class="empty-side">No published subtitles yet.</div>
            {/if}
          </section>

          {#if subtitleCandidates.length > 0}
            <section class="panel">
              <h2>Subtitle Candidates</h2>
              <div class="stack-list">
                {#each subtitleCandidates.slice(0, 8) as candidate}
                  <div class="stack-item candidate">
                    <div>
                      <strong>{candidate.language.toUpperCase()} · {candidate.provider}</strong>
                      <span>{candidate.releaseName || candidate.title}</span>
                    </div>
                    <Button kind="secondary" on:click={() => downloadSubtitle(candidate.id)} disabled={working}>
                      <Languages size={14} />
                      Get
                    </Button>
                  </div>
                {/each}
              </div>
            </section>
          {/if}

          {#if grabHistory.length > 0}
            <section class="panel">
              <h2>Grab History</h2>
              <div class="stack-list">
                {#each grabHistory as entry}
                  <div class="stack-item">
                    <div class="gh-info">
                      <strong class="gh-title">{entry.title}</strong>
                      <span class="gh-meta">
                        {entry.indexerName}{entry.resolution ? ` · ${entry.resolution}` : ''} · score {entry.score}
                      </span>
                      <span class="gh-date">{new Date(entry.grabbedAt).toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' })}</span>
                    </div>
                  </div>
                {/each}
              </div>
            </section>
          {/if}
        {/if}
      </aside>
    </section>
  </div>
{:else}
  <div class="empty">No details found.</div>
{/if}

{#if showReleasePicker}
  <div class="modal-backdrop" on:click={() => (showReleasePicker = false)} role="presentation">
    <div class="rel-modal" on:click|stopPropagation role="dialog" aria-modal="true" aria-label="Select release">
      <div class="rel-header">
        <h2>Select Release</h2>
        <button class="close-btn" on:click={() => (showReleasePicker = false)} aria-label="Close">
          <X size={18} />
        </button>
      </div>
      {#if releaseCandidates.length === 0}
        <div class="rel-empty">No candidates found.</div>
      {:else}
        <div class="rel-list">
          {#each releaseCandidates as c}
            {@const tags = qualityTags(c.title)}
            <div class="rel-row" class:rel-selected={c.selected} class:rel-rejected={c.rejected && !c.selected}>
              <div class="rel-info">
                <div class="rel-title">{c.title}</div>
                <div class="rel-meta">
                  {#if c.indexerName}<span class="rel-pill">{c.indexerName}</span>{/if}
                  <span class="rel-pill mono">{fmtBytes(c.sizeBytes)}</span>
                  <span class="rel-pill mono">score {c.score}</span>
                  {#each tags as tag}<span class="rel-pill rel-quality">{tag}</span>{/each}
                  {#if c.selected}<span class="rel-pill rel-pill-ok">selected</span>{/if}
                  {#if c.rejected && !c.selected}<span class="rel-pill rel-pill-danger">{c.rejectReason || 'rejected'}</span>{/if}
                  {#if c.failureCount > 0}<span class="rel-pill rel-pill-warn">{c.failureCount}× failed</span>{/if}
                </div>
              </div>
              <Button kind={c.selected ? 'primary' : 'secondary'} on:click={() => pickRelease(c.releaseCandidateId)} disabled={working}>
                <Download size={14} />
                {c.selected ? 'Re-grab' : 'Download'}
              </Button>
            </div>
          {/each}
        </div>
      {/if}
    </div>
  </div>
{/if}

<style>
  .page { display: grid; gap: 22px; }
  .hero {
    position: relative; overflow: hidden; border-radius: 28px;
    border: 1px solid hsl(0 0% 100% / 0.08);
  }
  .hero-bg, .hero-shade { position: absolute; inset: 0; }
  .hero-bg { width: 100%; height: 100%; object-fit: cover; }
  .hero-shade { background: linear-gradient(180deg, hsl(0 0% 0% / 0.2), hsl(0 0% 0% / 0.86)); }
  .hero-grid {
    position: relative; z-index: 1; min-height: 420px;
    display: grid; grid-template-columns: 220px minmax(0,1fr);
    gap: 24px; align-items: end; padding: 24px;
  }
  .poster { aspect-ratio: 2 / 3; overflow: hidden; border-radius: 20px; border: 1px solid hsl(0 0% 100% / 0.1); background: hsl(var(--muted)); }
  .poster img, .person-photo img { width: 100%; height: 100%; object-fit: cover; }
  .poster-fallback, .person-photo { display: grid; place-items: center; width: 100%; height: 100%; color: hsl(var(--muted-foreground)); }
  .copy { min-width: 0; display: grid; gap: 12px; align-content: end; }
  .copy h1 { margin: 8px 0 0; font-size: clamp(2rem, 5vw, 3.7rem); line-height: 1.04; }
  .copy p { max-width: 900px; color: hsl(var(--foreground) / 0.8); line-height: 1.65; }
  .tagline { margin-top: 10px; color: hsl(var(--foreground) / 0.82); font-weight: 700; }
  .badge-row, .action-row, .chips { display: flex; flex-wrap: wrap; gap: 10px; }
  .genre-chips { margin-top: 18px; }
  .action-row { align-items: center; }
  .action-row :global(button) { min-height: 42px; }
  .action-row :global(button),
  .action-row .link-btn { flex: 0 0 auto; }
  .link-btn {
    display: inline-flex; align-items: center; justify-content: center;
    min-height: 42px; padding: 0 14px; border-radius: 14px;
    border: 1px solid hsl(0 0% 100% / 0.08); text-decoration: none;
  }
  .link-btn.secondary {
    background: hsl(0 0% 100% / 0.05); color: hsl(var(--foreground));
  }
  .grid { display: grid; grid-template-columns: minmax(0,1.7fr) minmax(300px,0.8fr); gap: 20px; align-items: start; }
  .main, .side { display: grid; gap: 18px; }
  .panel {
    border-radius: 24px; border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(var(--card) / 0.82); padding: 18px;
    min-width: 0;
  }
  .panel h2 { margin: 0 0 14px; font-size: 18px; }
  .stat-grid, .kv { display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; }
  .stat-grid div, .kv div {
    display: grid; gap: 4px; padding: 12px; border-radius: 14px;
    border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03);
  }
  .failure-box, .empty-side {
    margin-top: 14px;
    padding: 12px 14px;
    border-radius: 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03);
    color: hsl(var(--muted-foreground));
    font-size: 13px;
  }
  .stack-list { display: grid; gap: 10px; }
  .stack-item {
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
    padding: 12px 14px; border-radius: 14px;
    border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03);
  }
  .stack-item strong, .stack-item span { display: block; }
  .stack-item span {
    margin-top: 4px; color: hsl(var(--muted-foreground)); font-size: 12px;
  }
  .candidate { align-items: flex-start; }
  .stat-grid span, .kv span, .summary-meta, .person-card span { color: hsl(var(--muted-foreground)); font-size: 12px; }
  .season-stack, .episode-list { display: grid; gap: 12px; }
  .season-panel { border-radius: 18px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.02); overflow: hidden; }
  .season-panel summary { list-style: none; cursor: pointer; padding: 14px 16px; display: grid; gap: 6px; }
  .episode-row {
    display: flex; align-items: center; justify-content: space-between; gap: 12px;
    padding: 10px 16px; border-top: 1px solid hsl(0 0% 100% / 0.05);
  }
  .ep-info { flex: 1; min-width: 0; display: grid; gap: 2px; }
  .ep-code {
    font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 700;
    color: hsl(var(--foreground));
  }
  .ep-title {
    font-size: 12px; color: hsl(var(--muted-foreground));
    overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
  }
  .ep-right { display: flex; align-items: center; gap: 8px; flex-shrink: 0; }
  .ep-sub-btn {
    display: inline-flex; align-items: center; gap: 4px; padding: 3px 9px;
    border-radius: 8px; border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.04); color: hsl(var(--muted-foreground));
    font-size: 11px; cursor: pointer; flex-shrink: 0;
  }
  .ep-sub-btn:hover { background: hsl(var(--primary) / 0.15); color: hsl(var(--primary)); border-color: hsl(var(--primary) / 0.3); }
  .media-strip { padding-bottom: 4px; }
  .person-slot { width: 146px; flex: 0 0 auto; }
  .poster-slot { width: 146px; flex: 0 0 auto; }
  .person-card {
    display: grid; gap: 8px; padding: 10px; border-radius: 16px;
    border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03);
    min-height: 100%;
  }
  .person-photo { aspect-ratio: 2 / 3; overflow: hidden; border-radius: 12px; background: hsl(var(--muted)); }
  .empty {
    padding: 28px; text-align: center; color: hsl(var(--muted-foreground));
    border-radius: 20px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.02);
  }
  @media (max-width: 1200px) {
    .grid { grid-template-columns: 1fr; }
    .stat-grid, .kv { grid-template-columns: repeat(2, minmax(0,1fr)); }
  }

  @media (max-width: 980px) {
    .hero-grid, .grid { grid-template-columns: 1fr; }
    .poster { max-width: 220px; }
    .copy { align-content: start; }
  }

  @media (max-width: 700px) {
    .stat-grid, .kv { grid-template-columns: 1fr; }
    .hero-grid { padding: 18px; gap: 18px; }
    .action-row { align-items: stretch; }
  }
  .gh-info { display: grid; gap: 2px; min-width: 0; }
  .gh-title { font-size: 12px; font-weight: 600; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .gh-meta { font-size: 11px; color: hsl(var(--muted-foreground)); font-family: 'JetBrains Mono', monospace; }
  .gh-date { font-size: 11px; color: hsl(var(--muted-foreground)); }
  .monitoring-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-top: 12px; padding-top: 12px; border-top: 1px solid hsl(0 0% 100% / 0.06); }
  .monitoring-row label { font-size: 12px; font-weight: 600; color: hsl(var(--muted-foreground)); white-space: nowrap; }
  .monitoring-row select { flex: 1; min-width: 0; height: 32px; border-radius: 8px; border: 1px solid hsl(0 0% 100% / 0.1); background: hsl(0 0% 100% / 0.05); color: inherit; font-size: 12px; padding: 0 8px; cursor: pointer; }

  /* Release picker modal */
  .modal-backdrop {
    position: fixed; inset: 0; z-index: 900;
    background: hsl(0 0% 0% / 0.65); backdrop-filter: blur(4px);
    display: flex; align-items: center; justify-content: center; padding: 24px;
  }
  .rel-modal {
    background: hsl(var(--card)); border: 1px solid hsl(0 0% 100% / 0.1);
    border-radius: 24px; width: 100%; max-width: 780px;
    max-height: 82vh; display: flex; flex-direction: column; overflow: hidden;
  }
  .rel-header {
    display: flex; align-items: center; justify-content: space-between;
    padding: 20px 22px 16px; border-bottom: 1px solid hsl(0 0% 100% / 0.07); flex-shrink: 0;
  }
  .rel-header h2 { margin: 0; font-size: 18px; }
  .close-btn {
    display: flex; align-items: center; justify-content: center;
    width: 32px; height: 32px; border-radius: 10px;
    border: 1px solid hsl(0 0% 100% / 0.08); background: transparent;
    color: hsl(var(--muted-foreground)); cursor: pointer;
  }
  .close-btn:hover { background: hsl(0 0% 100% / 0.06); color: hsl(var(--foreground)); }
  .rel-list { overflow-y: auto; padding: 12px; display: grid; gap: 8px; }
  .rel-empty { padding: 36px; text-align: center; color: hsl(var(--muted-foreground)); font-size: 14px; }
  .rel-row {
    display: flex; align-items: flex-start; gap: 14px; padding: 14px 16px;
    border-radius: 16px; border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03);
  }
  .rel-row:hover { background: hsl(0 0% 100% / 0.055); }
  .rel-selected { border-color: hsl(var(--primary) / 0.4); background: hsl(var(--primary) / 0.06); }
  .rel-rejected { opacity: 0.5; }
  .rel-info { flex: 1; min-width: 0; display: grid; gap: 7px; }
  .rel-title { font-size: 13px; font-weight: 600; line-height: 1.4; word-break: break-word; }
  .rel-meta { display: flex; flex-wrap: wrap; gap: 5px; align-items: center; }
  .rel-pill {
    font-size: 11px; padding: 2px 7px; border-radius: 6px;
    background: hsl(0 0% 100% / 0.07); border: 1px solid hsl(0 0% 100% / 0.09);
    color: hsl(var(--muted-foreground)); white-space: nowrap;
  }
  .rel-pill.mono { font-family: 'JetBrains Mono', monospace; }
  .rel-quality { color: hsl(var(--foreground) / 0.85); font-weight: 600; }
  .rel-pill-ok { background: hsl(142 70% 45% / 0.15); border-color: hsl(142 70% 45% / 0.3); color: hsl(142 60% 55%); font-weight: 600; }
  .rel-pill-danger { background: hsl(0 70% 50% / 0.15); border-color: hsl(0 70% 50% / 0.25); color: hsl(0 70% 60%); }
  .rel-pill-warn { background: hsl(40 90% 50% / 0.15); border-color: hsl(40 90% 50% / 0.25); color: hsl(40 80% 60%); }
</style>
