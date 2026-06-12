<script lang="ts">
  import { onMount } from 'svelte';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import Wrench from '@lucide/svelte/icons/wrench';
  import X from '@lucide/svelte/icons/x';
  import Search from '@lucide/svelte/icons/search';
  import Tv from '@lucide/svelte/icons/tv';
  import PlugZap from '@lucide/svelte/icons/plug-zap';
  import Wifi from '@lucide/svelte/icons/wifi';
  import Settings2 from '@lucide/svelte/icons/settings-2';
  import FolderTree from '@lucide/svelte/icons/folder-tree';
  import ShieldAlert from '@lucide/svelte/icons/shield-alert';
  import ClipboardList from '@lucide/svelte/icons/clipboard-list';
  import SlidersHorizontal from '@lucide/svelte/icons/sliders-horizontal';
  import ScrollText from '@lucide/svelte/icons/scroll-text';
  import Library from '@lucide/svelte/icons/library';
  import Button from '$lib/components/Button.svelte';
  import PageHeader from '$lib/components/PageHeader.svelte';
  import Panel from '$lib/components/Panel.svelte';
  import StatusPill from '$lib/components/StatusPill.svelte';
  import { api, subscribeEvents } from '$lib/api';
  import { bytes, dateTime } from '$lib/format';
  import { toastError, toastSuccess } from '$lib/toast';
  import type { BlocklistItem, IntegrationProbeReport, MaintenanceResult, PolicySettings, QualityProfile, Status } from '$lib/types';

  type SettingsTab = 'integrations' | 'providers' | 'queue' | 'library' | 'rules' | 'quality' | 'logs' | 'tasks' | 'plex' | 'system';

  const tabs: { id: SettingsTab; label: string; short: string; icon: typeof PlugZap }[] = [
    { id: 'integrations', label: 'Integrations', short: 'Apps',    icon: PlugZap },
    { id: 'providers',    label: 'Providers',    short: 'Feeds',   icon: Wifi },
    { id: 'queue',        label: 'Queue',        short: 'Queue',   icon: Settings2 },
    { id: 'library',      label: 'Library',      short: 'Names',   icon: Library },
    { id: 'rules',        label: 'Rules',        short: 'Rules',   icon: ShieldAlert },
    { id: 'quality',      label: 'Quality',      short: 'Quality', icon: SlidersHorizontal },
    { id: 'logs',         label: 'Logs',         short: 'Logs',    icon: ScrollText },
    { id: 'tasks',        label: 'Tasks',        short: 'Tasks',   icon: ClipboardList },
    { id: 'plex',         label: 'Plex',         short: 'Plex',    icon: Tv },
    { id: 'system',       label: 'System',       short: 'System',  icon: FolderTree }
  ];

  const REASONS = ['manual', 'archive_rejected', 'missing_articles', 'nzb_parse_failed', 'unsupported_archive', 'no_video_content', 'wrong_title', 'quality_rejected'] as const;

  let status: Status | null = null;
  let loading = true;
  let working = false;
  let blocklist: BlocklistItem[] = [];
  let lastProbe: IntegrationProbeReport | null = null;
  let profiles: QualityProfile[] = [];
  let policySettings: PolicySettings | null = null;
  let lastMaintenance: MaintenanceResult | null = null;
  let lastCachePrune: { root: string; filesBefore: number; filesAfter: number; bytesBefore: number; bytesAfter: number; deletedFiles: number; deletedBytes: number; limitBytes: number } | null = null;
  let activeTab: SettingsTab = 'integrations';

  // Blocklist filters
  let blockQuery = '';
  let blockReasonFilter = 'all';

  const queueDecisionRows = [
    ['grabbedSeriesIdMismatch',     'Found matching series via grab history, but release was matched to series by ID. Automatic import is not possible.'],
    ['grabbedMovieIdMismatch',      'Found matching movie via grab history, but release was matched to movie by ID. Manual import required.'],
    ['episodeMissingInRelease',     'Episode was not found in the grabbed release'],
    ['unexpectedEpisodes',          'Episode(s) were unexpected considering the folder name'],
    ['notEpisodeUpgrade',           'Not an upgrade for existing episode file(s)'],
    ['notMovieUpgrade',             'Not an upgrade for existing movie file'],
    ['notCustomFormatUpgrade',      'Not a Custom Format upgrade'],
    ['noEligibleFiles',             'No files found are eligible for import'],
    ['episodeAlreadyImported',      'Episode file already imported'],
    ['noAudioTracks',               'No audio tracks detected'],
    ['invalidSeasonEpisode',        'Invalid season or episode'],
    ['singleEpisodeContainsSeason', 'Single episode file contains all episodes in seasons'],
    ['unableToDetermineSample',     'Unable to determine if file is a sample'],
    ['sample',                      'Sample'],
    ['archiveNeedsExtraction',      'Found archive file, might need to be extracted'],
    ['missingArticles',             'Missing articles / expired Usenet parts']
  ] as const;

  const queueDecisionLabels: Record<string, string> = {
    do_nothing:              'Do Nothing',
    remove:                  'Remove',
    remove_and_blocklist:    'Remove and Blocklist',
    remove_blocklist_and_search: 'Remove, Blocklist, and Search',
    search_again:            'Search Again'
  };

  function readTabFromURL(): SettingsTab {
    if (typeof window === 'undefined') return 'integrations';
    const raw = new URL(window.location.href).searchParams.get('tab');
    return tabs.some((t) => t.id === raw) ? (raw as SettingsTab) : 'integrations';
  }

  function setActiveTab(tab: SettingsTab) {
    activeTab = tab;
    if (typeof window === 'undefined') return;
    const url = new URL(window.location.href);
    url.searchParams.set('tab', tab);
    window.history.replaceState({}, '', url);
  }

  async function loadAll() {
    loading = true;
    try {
      const [s, bl, pr, pol] = await Promise.all([
        api.status(),
        api.blocklist(),
        api.listProfiles(),
        api.policies()
      ]);
      status = s;
      blocklist = bl.items ?? [];
      profiles = pr.profiles;
      policySettings = pol;
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      loading = false;
    }
  }

  async function clearBlocklist(id: number) {
    working = true;
    try {
      await api.clearBlocklist(id);
      toastSuccess('Blocklist item cleared');
      blocklist = blocklist.filter((b) => b.id !== id);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function clearAllBlocklist() {
    working = true;
    try {
      const r = await api.clearAllBlocklist();
      toastSuccess(`Cleared ${r.cleared} blocklist entr${r.cleared === 1 ? 'y' : 'ies'}`);
      blocklist = [];
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function runMaintenance(task: 'orphaned-content' | 'broken-media-symlinks' | 'orphaned-completed-symlinks') {
    working = true;
    try {
      lastMaintenance = await api.maintenance(task);
      toastSuccess(`${lastMaintenance.taskName} completed`);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function pruneCache() {
    working = true;
    try {
      lastCachePrune = await api.pruneCache();
      toastSuccess(`Cache pruned: ${lastCachePrune.deletedFiles} files removed`);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function probeIntegrations() {
    working = true;
    try {
      lastProbe = await api.probeIntegrations();
      const ok = lastProbe.results.filter((r) => r.ok).length;
      toastSuccess(`Probes: ${ok}/${lastProbe.results.length} OK`);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function savePolicies() {
    if (!policySettings) return;
    working = true;
    try {
      policySettings = await api.savePolicies(policySettings);
      toastSuccess('Queue policy saved');
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  onMount(() => {
    activeTab = readTabFromURL();
    void loadAll();
    const unsub = subscribeEvents(() => { if (!working) void loadAll(); });
    const timer = window.setInterval(() => void loadAll(), 30000);
    return () => { window.clearInterval(timer); unsub(); };
  });

  $: integrationEntries = status ? Object.entries(status.integrations).filter(([n]) => n !== 'subtitleProviders') : [];
  $: subtitleProviderEntries = status ? Object.entries(status.integrations.subtitleProviders) : [];
  $: usenetSettings = (status?.settings.usenet as {
    maxDownloadConnections?: number;
    streamingPriorityPercent?: number;
    articleBufferSize?: number;
    providers?: { name: string; host: string; port: number; tls: boolean; username: string; maxConnections: number; enabled: boolean }[];
  } | undefined) ?? { providers: [] };

  $: filteredBlocklist = (blocklist ?? []).filter((b) => {
    const matchReason = blockReasonFilter === 'all' || b.reason === blockReasonFilter;
    const matchQ = !blockQuery || `${b.key} ${b.reason}`.toLowerCase().includes(blockQuery.toLowerCase());
    return matchReason && matchQ;
  });

  $: blocklistStats = {
    total: (blocklist ?? []).length,
    byReason: (blocklist ?? []).reduce<Record<string, number>>((acc, b) => { acc[b.reason] = (acc[b.reason] ?? 0) + 1; return acc; }, {})
  };

  $: configuredCount = integrationEntries.filter(([, v]) => v.configured).length;
  $: enabledProviders = (usenetSettings.providers ?? []).filter((p) => p.enabled).length;
  $: defaultProfile = profiles.find((p) => p.isDefault)?.name ?? '—';
</script>

<svelte:head><title>Settings — Drakkar</title></svelte:head>

<PageHeader title="Settings" subtitle="Integrations, providers, queue policy, rules and system configuration.">
  <Button kind="secondary" on:click={loadAll} disabled={loading || working}>
    <RefreshCw size={16} />
    Refresh
  </Button>
  <Button kind="secondary" on:click={probeIntegrations} disabled={loading || working}>
    <Wrench size={16} />
    Probe
  </Button>
</PageHeader>

{#if status}
  <div class="summary-strip">
    <div class="summary-card">
      <strong>{configuredCount}</strong>
      <span>configured integrations</span>
    </div>
    <div class="summary-card">
      <strong>{enabledProviders}</strong>
      <span>enabled providers</span>
    </div>
    <div class="summary-card">
      <strong>{defaultProfile}</strong>
      <span>default quality profile</span>
    </div>
    <div class="summary-card">
      <strong>{status.backgroundQueueDepth}</strong>
      <span>background queue depth</span>
    </div>
  </div>
{/if}

<div class="settings-shell">
  <aside class="tab-rail">
    {#each tabs as tab}
      <button class="tab-btn" class:active={activeTab === tab.id} on:click={() => setActiveTab(tab.id)} type="button">
        <tab.icon size={16} />
        <span class="tab-label">
          <strong>{tab.label}</strong>
          <small>{tab.short}</small>
        </span>
      </button>
    {/each}
  </aside>

  <div class="tab-content">

    <!-- INTEGRATIONS -->
    {#if activeTab === 'integrations'}
      <div class="grid-2">
        <Panel title="Integration Readiness" subtitle="Config-derived readiness for external services.">
          {#if status}
            <div class="int-list">
              {#each integrationEntries as [name, value]}
                <div class="int-row">
                  <div class="int-info">
                    <strong>{name}</strong>
                    <span>{value.detail || 'no detail'}</span>
                  </div>
                  <StatusPill tone={value.configured ? 'ok' : value.enabled ? 'warn' : 'neutral'}>
                    {value.configured ? 'configured' : value.enabled ? 'incomplete' : 'disabled'}
                  </StatusPill>
                </div>
              {/each}
              {#each subtitleProviderEntries as [name, value]}
                <div class="int-row nested">
                  <div class="int-info">
                    <strong>subtitle: {name}</strong>
                    <span>{value.detail || 'no detail'}</span>
                  </div>
                  <StatusPill tone={value.configured ? 'ok' : value.enabled ? 'warn' : 'neutral'}>
                    {value.configured ? 'configured' : value.enabled ? 'incomplete' : 'disabled'}
                  </StatusPill>
                </div>
              {/each}
            </div>
          {:else}
            <div class="empty">Loading…</div>
          {/if}
        </Panel>

        <Panel title="Integration Probes" subtitle="Live reachability and auth checks. Click Probe to run.">
          {#if lastProbe && lastProbe.results.length > 0}
            <div class="int-list">
              {#each lastProbe.results as item}
                <div class="int-row">
                  <div class="int-info">
                    <strong>{item.name}</strong>
                    <span>{item.detail}</span>
                  </div>
                  <StatusPill tone={item.ok ? 'ok' : 'danger'}>
                    {item.ok ? `${item.durationMs} ms` : 'failed'}
                  </StatusPill>
                </div>
              {/each}
            </div>
          {:else}
            <div class="empty">No probe results yet. Click Probe above.</div>
          {/if}
        </Panel>
      </div>
      <div class="config-hint">
        <ShieldAlert size={14} />
        Credentials (API keys, passwords) are configured in <code>settings.json</code> on the server — not editable via the UI to protect secrets.
      </div>

    <!-- PROVIDERS -->
    {:else if activeTab === 'providers'}
      <Panel title="Usenet Providers" subtitle="Live runtime NNTP pool configuration loaded from settings.json.">
        <div class="stat-row">
          <div class="stat-cell"><span>Max connections</span><strong>{usenetSettings.maxDownloadConnections ?? '—'}</strong></div>
          <div class="stat-cell"><span>Streaming priority</span><strong>{usenetSettings.streamingPriorityPercent ?? '—'}%</strong></div>
          <div class="stat-cell"><span>Article buffer</span><strong>{usenetSettings.articleBufferSize ?? '—'}</strong></div>
        </div>
        <div class="provider-list">
          {#each usenetSettings.providers ?? [] as p}
            <div class="provider-card">
              <div class="provider-head">
                <div>
                  <strong>{p.name}</strong>
                  <span class="mono">{p.host}:{p.port}</span>
                </div>
                <StatusPill tone={p.enabled ? 'ok' : 'neutral'}>{p.enabled ? 'enabled' : 'disabled'}</StatusPill>
              </div>
              <div class="provider-meta mono">
                <span>TLS: {p.tls ? 'on' : 'off'}</span>
                <span>User: {p.username || '—'}</span>
                <span>Pool: {p.maxConnections} conns</span>
              </div>
            </div>
          {/each}
          {#if !(usenetSettings.providers ?? []).length}
            <div class="empty">No providers configured.</div>
          {/if}
        </div>
        <div class="config-hint">
          <ShieldAlert size={14} />
          Edit providers, credentials, and connection limits in <code>settings.json</code> and restart.
        </div>
      </Panel>

    <!-- QUEUE -->
    {:else if activeTab === 'queue'}
      <div class="grid-2">
        <Panel title="Connection Budget" subtitle="Current streaming and background pool limits.">
          <div class="kv-list">
            <div><span>Max connections</span><strong>{usenetSettings.maxDownloadConnections ?? '—'}</strong></div>
            <div><span>Streaming priority</span><strong>{usenetSettings.streamingPriorityPercent ?? '—'}%</strong></div>
            <div><span>Article buffer</span><strong>{usenetSettings.articleBufferSize ?? '—'}</strong></div>
            <div><span>Background queue depth</span><strong>{status?.backgroundQueueDepth ?? '—'}</strong></div>
          </div>
        </Panel>

        <Panel title="Queue Behavior" subtitle="Playback vs background split.">
          <div class="kv-list">
            <div><span>Playback lane</span><strong>{usenetSettings.streamingPriorityPercent ?? '—'}% of pool</strong></div>
            <div><span>Background lane</span><strong>{status?.backgroundQueueDepth ?? 0} queued</strong></div>
            <div><span>Retry path</span><strong>candidate fallback first</strong></div>
            <div><span>Seek prefetch</span><strong>deferred until first read</strong></div>
          </div>
        </Panel>
      </div>

      <Panel title="Automatic Queue Management" subtitle="Nzbdav-style actions for failed releases. Applies only to known Drakkar failure reasons.">
        {#if policySettings}
          <div class="queue-rules">
            {#each queueDecisionRows as [key, label]}
              <label class="rule-row">
                <span class="rule-label">{label}</span>
                <select bind:value={policySettings.queueDecisionActions[key]}>
                  {#each Object.entries(queueDecisionLabels) as [v, text]}
                    <option value={v}>{text}</option>
                  {/each}
                </select>
              </label>
            {/each}
          </div>
          <div class="actions-row">
            <Button kind="secondary" on:click={savePolicies} disabled={loading || working}>
              <Settings2 size={16} />
              Save Queue Rules
            </Button>
          </div>
        {:else}
          <div class="empty">Policy settings unavailable.</div>
        {/if}
      </Panel>

    <!-- LIBRARY -->
    {:else if activeTab === 'library'}
      <div class="grid-2">
        <Panel title="Root Folders" subtitle="Host-side directories where Drakkar publishes media symlinks.">
          <div class="root-folders">
            <div class="root-row">
              <div class="root-path mono">/mnt/drakkar/media/movies</div>
              <StatusPill tone="ok">Movies</StatusPill>
            </div>
            <div class="root-row">
              <div class="root-path mono">/mnt/drakkar/media/tv</div>
              <StatusPill tone="ok">TV Shows</StatusPill>
            </div>
          </div>
          <div class="config-hint">
            <ShieldAlert size={14} />
            Root folder paths are set in <code>settings.json</code> on the server.
          </div>
        </Panel>

        <Panel title="Symlinks Instead of Copies" subtitle="How Drakkar publishes media — no disk duplication, instant availability.">
          <div class="hardlink-box">
            <div class="hardlink-icon">🔗</div>
            <div>
              <strong>Drakkar uses symlinks</strong>
              <p>Instead of copying or hardlinking files, Drakkar creates a lightweight symlink pointing to the virtual VFS path. This means <em>zero disk usage</em> for published media — the content stays remote on Usenet and is fetched on demand.</p>
              <p>Plex and Jellyfin follow the symlink into the FUSE mount transparently.</p>
            </div>
          </div>
        </Panel>
      </div>

      <div class="grid-2">
        <Panel title="Movie Naming" subtitle="Format used for published movie folders and files.">
          <div class="naming-section">
            <div class="naming-row">
              <span class="naming-label">Folder Format</span>
              <code class="naming-token">&#123;Movie Title&#125; (&#123;Release Year&#125;) &#123;tmdb-&#123;TmdbId&#125;&#125;</code>
            </div>
            <div class="naming-example mono">Dune (2021) &#123;tmdb-438631&#125;/</div>
            <div class="naming-row">
              <span class="naming-label">File Format</span>
              <code class="naming-token">&#123;Movie Title&#125; (&#123;Release Year&#125;).&#123;ext&#125;</code>
            </div>
            <div class="naming-example mono">Dune (2021).mkv</div>
            <div class="naming-row">
              <span class="naming-label">Colon Replacement</span>
              <code class="naming-token">Smart Replace</code>
            </div>
            <div class="naming-row">
              <span class="naming-label">Illegal Characters</span>
              <code class="naming-token">Replaced automatically</code>
            </div>
          </div>
        </Panel>

        <Panel title="Episode Naming" subtitle="Format used for published TV show folders and episode files.">
          <div class="naming-section">
            <div class="naming-row">
              <span class="naming-label">Series Folder</span>
              <code class="naming-token">&#123;Series Title&#125; (&#123;Series Year&#125;) &#123;tvdb-&#123;TvdbId&#125;&#125;</code>
            </div>
            <div class="naming-example mono">Loki (2021) &#123;tvdb-362472&#125;/</div>
            <div class="naming-row">
              <span class="naming-label">Season Folder</span>
              <code class="naming-token">Season &#123;season:00&#125;</code>
            </div>
            <div class="naming-example mono">Season 02/</div>
            <div class="naming-row">
              <span class="naming-label">Episode Format</span>
              <code class="naming-token">&#123;Series Title&#125; - S&#123;season:00&#125;E&#123;episode:00&#125;.&#123;ext&#125;</code>
            </div>
            <div class="naming-example mono">Loki (2021) - S02E01.mkv</div>
          </div>
        </Panel>
      </div>

    <!-- RULES -->
    {:else if activeTab === 'rules'}
      <!-- Blocklist -->
      <Panel title="Blocklist" subtitle="Durable release blocks from manual, archive or metadata rejects.">
        <div class="bl-toolbar">
          <div class="bl-search">
            <Search size={14} />
            <input bind:value={blockQuery} placeholder="Search by key or reason…" />
          </div>
          <select bind:value={blockReasonFilter} class="bl-reason-select">
            <option value="all">All reasons</option>
            {#each REASONS as r}
              <option value={r}>{r}</option>
            {/each}
          </select>
          <div class="bl-stats mono">
            {filteredBlocklist.length} / {blocklist.length} entries
          </div>
          {#if blocklist.length > 0}
            <Button kind="ghost" on:click={clearAllBlocklist} disabled={loading || working}>
              <X size={14} />
              Clear all
            </Button>
          {/if}
        </div>

        {#if filteredBlocklist.length > 0}
          <div class="bl-table-wrap">
            <table class="bl-table">
              <thead>
                <tr>
                  <th>Reason</th>
                  <th>Key / URL</th>
                  <th>Expires</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {#each filteredBlocklist as item (item.id)}
                  <tr>
                    <td><span class="reason-badge reason-{item.reason.split('_')[0]}">{item.reason}</span></td>
                    <td class="bl-key mono">{item.key}</td>
                    <td class="muted mono">{item.expiresAt ? new Date(item.expiresAt).toLocaleDateString() : '—'}</td>
                    <td class="bl-action">
                      <button class="clear-btn" on:click={() => clearBlocklist(item.id)} disabled={working} title="Clear this entry">
                        <X size={13} />
                      </button>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {:else if blocklist.length > 0}
          <div class="empty">No entries match the current filter.</div>
        {:else}
          <div class="empty">No active blocklist entries.</div>
        {/if}
      </Panel>

      <div class="grid-2" style="margin-top:16px">
        <!-- Ignored Patterns -->
        <Panel title="Ignored File Patterns" subtitle="Patterns skipped from imported NZBs and library processing.">
          {#if policySettings}
            <textarea class="pattern-box" value={policySettings.ignoredPatterns.join('\n')} on:change={(e) => {
              const t = e.currentTarget as HTMLTextAreaElement;
              const cur = policySettings;
              if (!cur) return;
              policySettings = { queueDecisionActions: { ...cur.queueDecisionActions }, ignoredPatterns: t.value.split('\n').map((l) => l.trim()).filter(Boolean) };
            }}></textarea>
            <div class="actions-row">
              <Button kind="secondary" on:click={savePolicies} disabled={loading || working}>
                <Settings2 size={16} />
                Save Patterns
              </Button>
            </div>
          {:else}
            <div class="empty">Unavailable.</div>
          {/if}
        </Panel>

        <!-- Maintenance -->
        <Panel title="Maintenance" subtitle="Operator cleanup and cache controls.">
          <div class="maint-list">
            <Button kind="secondary" on:click={pruneCache} disabled={loading || working}>
              <Wrench size={16} />
              Prune Block Cache
            </Button>
            <Button kind="secondary" on:click={() => runMaintenance('orphaned-content')} disabled={loading || working}>
              <Wrench size={16} />
              Remove Orphaned Content
            </Button>
            <Button kind="secondary" on:click={() => runMaintenance('broken-media-symlinks')} disabled={loading || working}>
              <Wrench size={16} />
              Remove Broken Symlinks
            </Button>
            <Button kind="secondary" on:click={() => runMaintenance('orphaned-completed-symlinks')} disabled={loading || working}>
              <Wrench size={16} />
              Remove Orphaned Completed
            </Button>
          </div>
          {#if lastMaintenance}
            <div class="result-box">
              <strong>{lastMaintenance.taskName}</strong>
              <div class="result-grid mono">
                <span>scanned files: {lastMaintenance.scannedFiles}</span>
                <span>scanned rows: {lastMaintenance.scannedRows}</span>
                <span>deleted files: {lastMaintenance.deletedFiles}</span>
                <span>deleted rows: {lastMaintenance.deletedRows}</span>
              </div>
            </div>
          {/if}
          {#if lastCachePrune}
            <div class="result-box">
              <strong>cache-prune</strong>
              <div class="result-grid mono">
                <span>limit: {bytes(lastCachePrune.limitBytes)}</span>
                <span>deleted: {lastCachePrune.deletedFiles} files</span>
                <span>before: {lastCachePrune.filesBefore}</span>
                <span>after: {lastCachePrune.filesAfter}</span>
              </div>
            </div>
          {/if}
        </Panel>
      </div>

    <!-- QUALITY -->
    {:else if activeTab === 'quality'}
      <Panel title="Quality Profiles" subtitle="Full management is available on the dedicated Profiles page.">
        <div class="actions-row" style="margin-bottom:14px">
          <a class="action-link" href="/profiles">Open Profiles page →</a>
        </div>
        <div class="profile-list">
          {#each profiles as p}
            <div class="profile-card">
              <div class="profile-head">
                <div>
                  <strong>{p.name}</strong>
                  <span>{p.isDefault ? 'default profile' : 'custom profile'}</span>
                </div>
                <StatusPill tone={p.isDefault ? 'ok' : 'neutral'}>{p.isDefault ? 'default' : 'saved'}</StatusPill>
              </div>
              <div class="profile-meta mono">
                <span>Res: {p.resolutions.join(', ') || '—'}</span>
                <span>Src: {p.sources.join(', ') || '—'}</span>
                <span>Codec: {p.codecs.join(', ') || '—'}</span>
              </div>
            </div>
          {/each}
          {#if profiles.length === 0}
            <div class="empty">No quality profiles.</div>
          {/if}
        </div>
      </Panel>

    <!-- LOGS -->
    {:else if activeTab === 'logs'}
      <div class="logs-redirect">
        <p>Full log viewer is on the dedicated <a href="/logs">Logs page</a>.</p>
        <a href="/logs">
          <Button kind="secondary">
            <ScrollText size={16} />
            Open Logs
          </Button>
        </a>
      </div>

    <!-- TASKS -->
    {:else if activeTab === 'tasks'}
      <div class="tasks-redirect">
        <p>Scheduled tasks and manual triggers are on the <a href="/tasks">Tasks page</a>.</p>
        <a href="/tasks">
          <Button kind="secondary">
            <ClipboardList size={16} />
            Open Tasks
          </Button>
        </a>
      </div>

    <!-- PLEX -->
    {:else if activeTab === 'plex'}
      <Panel title="Plex Media Server" subtitle="Configure Plex integration. Drakkar triggers a library scan automatically after publishing new media.">
        <div class="plex-info">
          <p>Add the following to your <code>settings.json</code> to enable Plex integration:</p>
          <pre class="plex-snippet">{`"plex": {
  "url": "http://your-plex-server:32400",
  "token": "your-plex-token",
  "sectionKey": ""
}`}</pre>
          <p>Leave <code>sectionKey</code> empty to refresh all libraries. Set it to a specific section number (e.g. "1") to only refresh that library.</p>
          <p>To find your Plex token, open Plex web, play any media, and look for <code>X-Plex-Token</code> in the network requests.</p>
        </div>
        <div class="actions-row" style="margin-top:14px">
          <Button kind="secondary" on:click={async () => {
            working = true;
            try {
              const r = await api.plexTest();
              if (r.ok) {
                toastSuccess(`Plex connected: ${r.serverName} (${r.libraries?.length ?? 0} libraries)`);
              } else {
                toastError(r.error ?? 'Plex connection failed');
              }
            } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
            finally { working = false; }
          }} disabled={working}>
            <Wrench size={16} /> Test Connection
          </Button>
          <Button kind="secondary" on:click={async () => {
            working = true;
            try {
              await api.plexRefresh();
              toastSuccess('Plex library scan triggered');
            } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
            finally { working = false; }
          }} disabled={working}>
            <RefreshCw size={16} /> Refresh Libraries Now
          </Button>
        </div>
      </Panel>

    <!-- SYSTEM -->
    {:else if activeTab === 'system'}
      <div class="grid-2">
        <Panel title="Runtime" subtitle="Service configuration from the backend.">
          {#if status}
            <div class="kv-list">
              <div><span>Started</span><strong>{dateTime(status.startedAt)}</strong></div>
              <div><span>FUSE mount</span><strong>{status.fuseMountPath}</strong></div>
              <div><span>Disk cache limit</span><strong>{bytes(status.diskCacheLimitBytes)}</strong></div>
              <div><span>Read-ahead limit</span><strong>{bytes(status.readAheadLimitBytes)}</strong></div>
              <div><span>Hot cache</span><strong>{bytes(status.memoryHotCacheBytes)}</strong></div>
              <div><span>Queue depth</span><strong>{status.backgroundQueueDepth}</strong></div>
            </div>
          {:else}
            <div class="empty">Loading runtime…</div>
          {/if}
        </Panel>

        <Panel title="Services (Redacted)" subtitle="Non-secret service configuration from /api/status.">
          {#if status}
            <div class="settings-tree">
              {#each Object.entries(status.settings) as [name, value]}
                <div class="settings-block">
                  <strong>{name}</strong>
                  <pre>{JSON.stringify(value, null, 2)}</pre>
                </div>
              {/each}
            </div>
          {:else}
            <div class="empty">No settings payload.</div>
          {/if}
        </Panel>
      </div>
    {/if}

  </div>
</div>

<style>
  /* summary strip */
  .summary-strip {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }

  .summary-card {
    padding: 14px 16px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 18px;
    background: hsl(0 0% 100% / 0.03);
  }

  .summary-card strong { display: block; font-size: 1.4rem; line-height: 1; }
  .summary-card span   { display: block; margin-top: 6px; color: hsl(var(--muted-foreground)); font-size: 13px; }

  /* shell */
  .settings-shell {
    display: grid;
    grid-template-columns: 220px minmax(0, 1fr);
    gap: 18px;
    align-items: start;
  }

  /* sidebar */
  .tab-rail {
    display: grid;
    gap: 8px;
    position: sticky;
    top: 88px;
  }

  .tab-btn {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 12px 14px;
    border-radius: 16px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03);
    color: inherit;
    cursor: pointer;
    text-align: left;
    transition: background 0.1s;
  }

  .tab-btn.active {
    background: hsl(var(--primary) / 0.15);
    border-color: hsl(var(--primary) / 0.3);
  }

  .tab-label { display: grid; gap: 1px; }
  .tab-label strong { font-size: 13px; }
  .tab-label small  { color: hsl(var(--muted-foreground)); font-size: 11px; }

  /* content area */
  .tab-content { display: grid; gap: 16px; }
  .grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }

  /* shared */
  .kv-list { display: grid; gap: 12px; }
  .kv-list div { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; padding: 10px 0; border-bottom: 1px solid hsl(0 0% 100% / 0.04); }
  .kv-list div:last-child { border-bottom: none; }
  .kv-list span { color: hsl(var(--muted-foreground)); font-size: 13px; }
  .kv-list strong { font-size: 13px; }

  .stat-row {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
    margin-bottom: 16px;
  }

  .stat-cell span { display: block; color: hsl(var(--muted-foreground)); font-size: 12px; margin-bottom: 4px; }
  .stat-cell strong { font-size: 1.1rem; }

  .int-list { display: grid; gap: 10px; }
  .int-row {
    display: flex;
    justify-content: space-between;
    align-items: start;
    gap: 12px;
    padding: 12px 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 14px;
    background: hsl(0 0% 100% / 0.03);
  }
  .int-row.nested { margin-left: 12px; }
  .int-info strong { display: block; font-size: 13px; }
  .int-info span   { display: block; margin-top: 3px; color: hsl(var(--muted-foreground)); font-size: 12px; overflow-wrap: anywhere; }

  /* providers */
  .provider-list { display: grid; gap: 12px; }
  .provider-card {
    padding: 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 14px;
    background: hsl(0 0% 100% / 0.03);
  }
  .provider-head { display: flex; justify-content: space-between; align-items: start; gap: 12px; margin-bottom: 10px; }
  .provider-head strong { display: block; font-size: 14px; }
  .provider-head span   { display: block; margin-top: 2px; color: hsl(var(--muted-foreground)); font-size: 12px; }
  .provider-meta { display: grid; grid-template-columns: repeat(3, 1fr); gap: 8px; font-size: 12px; color: hsl(var(--muted-foreground)); }

  /* blocklist */
  .bl-toolbar {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
    margin-bottom: 14px;
  }

  .bl-search {
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    min-width: 200px;
    height: 40px;
    padding: 0 12px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--muted-foreground));
  }

  .bl-search input {
    flex: 1;
    background: transparent;
    border: none;
    outline: none;
    color: hsl(var(--foreground));
    font-size: 13px;
  }

  .bl-reason-select {
    height: 40px;
    padding: 0 12px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--foreground));
    font-size: 13px;
    cursor: pointer;
  }

  .bl-stats { color: hsl(var(--muted-foreground)); font-size: 12px; white-space: nowrap; }

  .bl-table-wrap {
    overflow-x: auto;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 14px;
  }

  .bl-table { width: 100%; min-width: 560px; border-collapse: collapse; }

  .bl-table th {
    padding: 10px 14px;
    text-align: left;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    color: hsl(var(--muted-foreground));
    border-bottom: 1px solid hsl(0 0% 100% / 0.06);
  }

  .bl-table td {
    padding: 10px 14px;
    border-bottom: 1px solid hsl(0 0% 100% / 0.04);
    font-size: 13px;
    vertical-align: middle;
  }

  .bl-table tr:last-child td { border-bottom: none; }

  .bl-key { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: hsl(var(--muted-foreground)); font-size: 11px; }
  .bl-action { width: 40px; text-align: right; }

  .clear-btn {
    display: inline-grid;
    place-items: center;
    width: 28px;
    height: 28px;
    border-radius: 8px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: transparent;
    color: hsl(var(--muted-foreground));
    cursor: pointer;
  }
  .clear-btn:hover { background: hsl(0 72% 51% / 0.15); color: hsl(0 96% 82%); border-color: hsl(0 72% 51% / 0.3); }

  .reason-badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 8px;
    font-size: 11px;
    font-family: 'JetBrains Mono', monospace;
    background: hsl(0 0% 100% / 0.06);
    color: hsl(var(--muted-foreground));
  }
  .reason-badge.reason-manual   { background: hsl(271 75% 65% / 0.15); color: hsl(271 75% 82%); }
  .reason-badge.reason-missing  { background: hsl(0 72% 51% / 0.15);  color: hsl(0 96% 82%); }
  .reason-badge.reason-archive  { background: hsl(38 96% 55% / 0.15); color: hsl(38 100% 72%); }

  /* queue rules */
  .queue-rules { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-bottom: 14px; }
  @media (max-width: 900px) { .queue-rules { grid-template-columns: 1fr; } }
  .rule-row { display: grid; gap: 6px; }
  .rule-label { color: hsl(var(--muted-foreground)); font-size: 13px; }
  .rule-row select, .pattern-box {
    width: 100%;
    padding: 10px 12px;
    border-radius: 12px;
    border: 1px solid hsl(0 0% 100% / 0.15);
    background: hsl(0 0% 100% / 0.06);
    color: hsl(var(--foreground));
    font-size: 13px;
    cursor: pointer;
    appearance: auto;
    transition: border-color 0.15s, background 0.15s;
  }
  .rule-row select:hover, .rule-row select:focus {
    border-color: hsl(var(--primary) / 0.5);
    background: hsl(0 0% 100% / 0.09);
    outline: none;
  }
  .rule-row select option { background: hsl(215 36% 10%); }
  .pattern-box {
    min-height: 160px; resize: vertical;
    font-family: 'JetBrains Mono', monospace; font-size: 12px;
    cursor: text;
  }
  .pattern-box:focus { border-color: hsl(var(--primary) / 0.5); outline: none; }

  /* maintenance */
  .maint-list { display: grid; gap: 10px; margin-bottom: 14px; }
  .result-box {
    margin-top: 14px;
    padding: 12px 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.03);
  }
  .result-box strong { display: block; margin-bottom: 8px; font-size: 13px; }
  .result-grid { display: grid; grid-template-columns: repeat(2, 1fr); gap: 6px; }
  .result-grid span { color: hsl(var(--muted-foreground)); font-size: 12px; }

  /* profiles */
  .profile-list { display: grid; gap: 10px; }
  .profile-card {
    padding: 12px 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.03);
  }
  .profile-head { display: flex; justify-content: space-between; align-items: start; gap: 12px; margin-bottom: 8px; }
  .profile-head strong { display: block; font-size: 13px; }
  .profile-head span   { display: block; margin-top: 2px; color: hsl(var(--muted-foreground)); font-size: 12px; }
  .profile-meta { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; font-size: 11px; color: hsl(var(--muted-foreground)); }

  /* example stack */
  .example-stack { display: grid; gap: 8px; }
  .example-stack div { padding: 8px 12px; border-radius: 10px; background: hsl(0 0% 100% / 0.04); font-size: 12px; color: hsl(var(--muted-foreground)); }

  /* settings tree */
  .settings-tree { display: grid; gap: 14px; }
  .settings-block { }
  .settings-block strong { display: block; margin-bottom: 6px; font-size: 13px; }
  pre { margin: 0; white-space: pre-wrap; overflow-wrap: anywhere; color: hsl(var(--muted-foreground)); font-size: 11px; font-family: 'JetBrains Mono', monospace; }

  /* config hint */
  .config-hint {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 14px;
    padding: 10px 14px;
    border-radius: 12px;
    border: 1px solid hsl(38 96% 55% / 0.2);
    background: hsl(38 96% 55% / 0.08);
    color: hsl(38 100% 72%);
    font-size: 12px;
  }
  .config-hint code { font-family: 'JetBrains Mono', monospace; background: hsl(0 0% 100% / 0.1); padding: 1px 6px; border-radius: 4px; }

  /* actions */
  .actions-row { display: flex; justify-content: flex-end; margin-top: 14px; }
  .action-link { font-size: 13px; font-weight: 600; color: hsl(var(--primary)); text-decoration: none; }
  .action-link:hover { text-decoration: underline; }

  /* redirect panels */
  .logs-redirect, .tasks-redirect {
    padding: 32px;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 16px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 18px;
    background: hsl(0 0% 100% / 0.03);
  }
  .logs-redirect p, .tasks-redirect p { color: hsl(var(--muted-foreground)); }
  .logs-redirect a, .tasks-redirect a { display: contents; }

  /* utils */
  .mono { font-family: 'JetBrains Mono', monospace; }
  .muted { color: hsl(var(--muted-foreground)); }
  .empty { color: hsl(var(--muted-foreground)); padding: 12px 0; }

  /* root folders */
  .root-folders { display: grid; gap: 10px; }
  .root-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 12px 14px;
    border-radius: 12px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03);
  }
  .root-path { font-size: 12px; color: hsl(var(--foreground)); }

  /* hardlink/symlink box */
  .hardlink-box {
    display: flex;
    gap: 14px;
    padding: 14px;
    border-radius: 14px;
    border: 1px solid hsl(var(--primary) / 0.2);
    background: hsl(var(--primary) / 0.06);
  }
  .hardlink-icon { font-size: 1.8rem; flex-shrink: 0; }
  .hardlink-box strong { display: block; font-size: 14px; margin-bottom: 6px; }
  .hardlink-box p { margin: 0 0 8px; font-size: 13px; color: hsl(var(--muted-foreground)); line-height: 1.6; }
  .hardlink-box em { color: hsl(var(--primary)); font-style: normal; font-weight: 600; }

  /* naming patterns */
  .naming-section { display: grid; gap: 10px; }
  .naming-row { display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; }
  .naming-label { font-size: 12px; color: hsl(var(--muted-foreground)); min-width: 120px; flex-shrink: 0; }
  .naming-token {
    padding: 3px 8px;
    border-radius: 7px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.05);
    font-size: 12px;
    font-family: 'JetBrains Mono', monospace;
    color: hsl(var(--primary));
  }
  .naming-example {
    font-size: 12px;
    color: hsl(var(--muted-foreground));
    padding-left: 130px;
  }

  /* Plex section */
  .plex-info { display:grid; gap:10px; font-size:13px; color:hsl(var(--muted-foreground)); line-height:1.6; }
  .plex-info p { margin:0; }
  .plex-info code { font-family:'JetBrains Mono',monospace; background:hsl(0 0% 100%/.08); padding:1px 6px; border-radius:5px; color:hsl(var(--foreground)); }
  .plex-snippet { margin:0; padding:14px; border-radius:12px; border:1px solid hsl(0 0% 100%/.08); background:hsl(0 0% 100%/.04); font-size:12px; font-family:'JetBrains Mono',monospace; white-space:pre; overflow-x:auto; color:hsl(var(--foreground)); }

  /* responsive */
  @media (max-width: 1200px) {
    .settings-shell { grid-template-columns: 1fr; }
    .tab-rail { position: static; grid-template-columns: repeat(3, minmax(0, 1fr)); }
  }

  @media (max-width: 900px) {
    .summary-strip, .grid-2, .stat-row, .provider-meta, .profile-meta, .result-grid { grid-template-columns: 1fr; }
  }

  @media (max-width: 600px) {
    .tab-rail { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
</style>
