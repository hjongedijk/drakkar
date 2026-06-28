<script lang="ts">
  import { onMount } from 'svelte';
  import RefreshCw from '@lucide/svelte/icons/refresh-cw';
  import Wrench from '@lucide/svelte/icons/wrench';
  import X from '@lucide/svelte/icons/x';
  import Plus from '@lucide/svelte/icons/plus';
  import Search from '@lucide/svelte/icons/search';
  import Save from '@lucide/svelte/icons/save';
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
  import Play from '@lucide/svelte/icons/play';
  import Clock3 from '@lucide/svelte/icons/clock-3';
  import CheckCircle2 from '@lucide/svelte/icons/check-circle-2';
  import AlertTriangle from '@lucide/svelte/icons/alert-triangle';
  import Star from '@lucide/svelte/icons/star';
  import Ban from '@lucide/svelte/icons/ban';
  import ChevronUp from '@lucide/svelte/icons/chevron-up';
  import ChevronDown from '@lucide/svelte/icons/chevron-down';
  import Trash2 from '@lucide/svelte/icons/trash-2';
  import Pencil from '@lucide/svelte/icons/pencil';
  import ExternalLink from '@lucide/svelte/icons/external-link';
  import Copy from '@lucide/svelte/icons/copy';
  import Check from '@lucide/svelte/icons/check';
  import Webhook from '@lucide/svelte/icons/webhook';
  import Languages from '@lucide/svelte/icons/languages';
  import Button from '$lib/components/Button.svelte';
  import PageHeader from '$lib/components/PageHeader.svelte';
  import Pagination from '$lib/components/Pagination.svelte';
  import Panel from '$lib/components/Panel.svelte';
  import StatusPill from '$lib/components/StatusPill.svelte';
  import { api, subscribeEvents } from '$lib/api';
  import { bytes, dateTime } from '$lib/format';
  import { toastError, toastSuccess } from '$lib/toast';
  import type { BlocklistItem, BlocklistMutation, BlockTestResult, CustomFormat, FullSettings, IndexerPolicy, IntegrationProbeReport, MaintenanceResult, PolicySettings, QualityDefinition, QualityProfile, ReleaseBlockRule, Status, SubtitleProfile, TaskSchedule, UsenetProvider } from '$lib/types';

  type SettingsTab = 'integrations' | 'providers' | 'indexers' | 'queue' | 'library' | 'rules' | 'quality' | 'formats' | 'filtering' | 'subtitle-profiles' | 'notifications' | 'logs' | 'tasks' | 'media-players' | 'system';

  const tabs: { id: SettingsTab; label: string; short: string; icon: typeof PlugZap }[] = [
    { id: 'integrations',  label: 'Integrations',  short: 'Apps',     icon: PlugZap },
    { id: 'providers',     label: 'Providers',     short: 'Feeds',    icon: Wifi },
    { id: 'indexers',      label: 'Indexers',      short: 'Indexers', icon: Search },
    { id: 'queue',         label: 'Queue',         short: 'Queue',    icon: Settings2 },
    { id: 'library',       label: 'Library',       short: 'Names',    icon: Library },
    { id: 'rules',         label: 'Rules',         short: 'Rules',    icon: ShieldAlert },
    { id: 'quality',       label: 'Quality',       short: 'Quality',  icon: SlidersHorizontal },
    { id: 'formats',       label: 'Custom Formats',    short: 'Formats',   icon: Star },
    { id: 'filtering',        label: 'Release Filtering', short: 'Filtering', icon: Ban },
    { id: 'subtitle-profiles', label: 'Subtitle Profiles', short: 'Subtitles', icon: Languages },
    { id: 'notifications', label: 'Notifications',     short: 'Notify',    icon: Webhook },
    { id: 'logs',          label: 'Logs',          short: 'Logs',     icon: ScrollText },
    { id: 'tasks',         label: 'Tasks',         short: 'Tasks',    icon: ClipboardList },
    { id: 'media-players', label: 'Media Players', short: 'Players',  icon: Tv },
    { id: 'system',        label: 'System',        short: 'System',   icon: FolderTree }
  ];

  const REASONS = ['manual', 'archive_rejected', 'missing_articles', 'nzb_parse_failed', 'unsupported_archive', 'no_video_content', 'wrong_title', 'quality_rejected'] as const;

  let status: Status | null = null;
  let fullSettings: FullSettings | null = null;
  let draft: FullSettings | null = null;
  let loading = true;
  let working = false;
  let blocklist: BlocklistItem[] = [];
  let blPage = 1;
  let blPageSize = 50;
  let blTotal = 0;
  let blTotalPages = 1;
  let blLoading = false;
  let blStats: { total: number; active: number; expired: number; byReason: Record<string, number> } | null = null;
  let lastProbe: IntegrationProbeReport | null = null;
  let profiles: QualityProfile[] = [];
  let qualityDefs: QualityDefinition[] = [];
  let qualityDefsDirty: Set<number> = new Set();
  let qualityDefsSaving: Set<number> = new Set();
  let qualitySubTab: 'profiles' | 'definitions' = 'profiles';
  let policySettings: PolicySettings | null = null;
  let lastMaintenance: MaintenanceResult | null = null;
  let lastCachePrune: { root: string; filesBefore: number; filesAfter: number; bytesBefore: number; bytesAfter: number; deletedFiles: number; deletedBytes: number; limitBytes: number } | null = null;
  let activeTab: SettingsTab = 'integrations';

  let blockQuery = '';
  let blockReasonFilter = 'all';
  let blockSortCol: 'reason' | 'key' | 'expires' | 'createdAt' = 'createdAt';
  let blockSortDir: 'asc' | 'desc' = 'desc';
  let blShowAllReasons = false;
  const BL_REASON_CHIP_LIMIT = 10;
  type BlocklistEditor = {
    id?: number;
    keyType: 'raw' | 'external_url' | 'release_signature';
    key: string;
    externalUrl: string;
    releaseTitle: string;
    indexerName: string;
    sizeMb: number;
    postedDate: string;
    reason: string;
    expiresAt: string;
  };

  function blankBlocklistEditor(): BlocklistEditor {
    return {
      keyType: 'external_url',
      key: '',
      externalUrl: '',
      releaseTitle: '',
      indexerName: '',
      sizeMb: 0,
      postedDate: '',
      reason: 'manual',
      expiresAt: '',
    };
  }

  let blockEditor: BlocklistEditor = blankBlocklistEditor();

  // ── Seerr webhook ───────────────────────────────────────────────────────────
  let webhookCopied = false;
  $: webhookUrl = (typeof window !== 'undefined' ? window.location.origin : '') + '/api/webhooks/seerr';

  async function copyWebhookUrl() {
    await navigator.clipboard.writeText(webhookUrl);
    webhookCopied = true;
    setTimeout(() => { webhookCopied = false; }, 2000);
  }

  // ── Logs tab state ──────────────────────────────────────────────────────────
  type LogEntry = { level: string; service: string; message: string; time: string; raw: string };
  let logEntries: LogEntry[] = [];
  let logLoading = false;
  let logLevelFilter = 'all';
  let logTerm = '';
  let logError = '';

  async function loadLogs() {
    logLoading = true;
    logError = '';
    try {
      const data = await api.logs({ limit: 500, level: logLevelFilter !== 'all' ? logLevelFilter : undefined });
      logEntries = (data.lines ?? []).map(({ raw }) => {
        try {
          const obj = JSON.parse(raw);
          return { level: obj.level ?? '', service: obj.service ?? obj.component ?? obj.module ?? '', message: obj.message ?? obj.msg ?? raw, time: obj.time ?? '', raw };
        } catch { return { level: '', service: '', message: raw, time: '', raw }; }
      });
    } catch (e) { logError = e instanceof Error ? e.message : String(e); }
    finally { logLoading = false; }
  }

  function fmtLogDate(iso: string) {
    if (!iso) return '';
    try { return new Date(iso).toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' }); } catch { return iso; }
  }

  $: filteredLogs = logEntries
    .filter(e => (logLevelFilter === 'all' || e.level === logLevelFilter) && (!logTerm || `${e.service} ${e.message} ${e.raw}`.toLowerCase().includes(logTerm.toLowerCase())))
    .sort((a, b) => b.time.localeCompare(a.time));

  // ── Tasks tab state ─────────────────────────────────────────────────────────
  type TaskResult = { ok: boolean; detail: string; ranAt: string };
  type TaskDef = { id: string; label: string; description: string; group: string; interval: string; manual: boolean; run: () => Promise<unknown> };

  let taskRunning: Record<string, boolean> = {};
  let taskResults: Record<string, TaskResult> = {};
  let taskSchedules: TaskSchedule[] = [];
  let taskSchedulesLoading = true;

  const taskDefs: TaskDef[] = [
    { id: 'seerr_sync', label: 'Sync Seerr Requests', description: 'Import new and updated requests from Seerr.', group: 'Indexing', interval: '10m', manual: true, run: async () => { const r = await api.syncRequests(); return `seen ${r.seen}, created ${r.created}`; } },
    { id: 'seerr_push_library', label: 'Push Library to Seerr', description: 'Push library items that are missing from Seerr as new requests.', group: 'Indexing', interval: 'Manual', manual: true, run: async () => { const r = await api.pushMissingToSeerr(); return `movies pushed ${r.moviesPushed}, shows pushed ${r.showsPushed}`; } },
    { id: 'pending_queue_push', label: 'Dispatch Pending Queue', description: 'Push pending library rows into the bounded background work queue.', group: 'Indexing', interval: '30s', manual: false, run: async () => '' },
    { id: 'stale-queue-reset', label: 'Reset Stale Queue Items', description: 'Reset queue rows that have been stuck too long in transitional states.', group: 'Indexing', interval: '5m', manual: false, run: async () => '' },
    { id: 'backlog_search', label: 'Backlog Search', description: 'Search missing library items — one search per show+season per batch, 1-hour cooldown per item.', group: 'Indexing', interval: '30m', manual: true, run: async () => { await api.searchPendingLibrary(); return 'started in background'; } },
    { id: 'search_upgrades', label: 'Search Quality Upgrades', description: 'Re-search available items whose quality profile allows upgrades and replace them when a better release is found.', group: 'Indexing', interval: '6h', manual: true, run: async () => { await api.searchUpgrades(); return 'started in background'; } },
    { id: 'retry_failed_queue', label: 'Retry Failed Queue', description: 'Retry failed queue rows using current fallback policy.', group: 'Indexing', interval: '15m', manual: true, run: async () => { const r = await api.retryFailedQueue(); return `processed ${r.processed}, retried ${r.retried}`; } },
    { id: 'fill_missing_episodes', label: 'Fill Missing Episodes', description: 'Use TMDB episode lists to create library items for episodes not yet tracked, then queue them for search.', group: 'Indexing', interval: '6h', manual: true, run: async () => { const r = await api.fillMissingEpisodes(); return `processed ${r.showsProcessed} shows, found ${r.episodesFound} episodes, created ${r.itemsCreated} new items`; } },
    { id: 'backfill_metadata', label: 'Backfill Metadata', description: 'Re-enrich movies and TV shows with new TMDB fields.', group: 'Indexing', interval: 'Manual', manual: true, run: async () => { const r = await api.backfillMetadata(); return `enriched ${r.enriched} items`; } },
    { id: 'republish_pending', label: 'Republish Pending', description: 'Republish library items with a selected release but no current publication.', group: 'Publishing', interval: '30m', manual: true, run: async () => { await api.republishPendingLibrary(); return 'started in background'; } },
    { id: 'reset_orphaned_available', label: 'Reset Orphaned Available Items', description: 'Reset available items with no symlink and no recoverable source back to pending so they are re-searched and re-downloaded.', group: 'Publishing', interval: '30m', manual: true, run: async () => { await api.resetOrphanedAvailableItems(); return 'started in background'; } },
    { id: 'health_check', label: 'Symlink Health Check', description: 'Verify published symlinks still point to valid VFS targets. Also runs deep NZB article probe in background.', group: 'Maintenance', interval: '15m', manual: true, run: async () => { const r = await api.runHealthCheck(); return `checked ${r.checked}, healthy ${r.healthy}`; } },
    { id: 'article_health_check', label: 'Article Health Check', description: 'Probe first NNTP segment of every direct-NZB item. Resets items with expired or missing articles.', group: 'Maintenance', interval: '6h', manual: false, run: async () => '' },
    { id: 'nzb_health_check', label: 'Deep NZB Article Check', description: 'Full NNTP article scan — probes first/last segments for all published items and resets missing-article or sample-only publications.', group: 'Maintenance', interval: '168h', manual: false, run: async () => '' },
    { id: 'library-cleanup', label: 'Library Cleanup', description: 'Remove orphaned VFS content, broken media symlinks, and stale history entries.', group: 'Maintenance', interval: '6h', manual: false, run: async () => '' },
    { id: 'cache_prune', label: 'Prune Block Cache', description: 'Delete oldest decoded articles from disk cache.', group: 'Maintenance', interval: '6h', manual: true, run: async () => { const r = await api.pruneCache(); return `deleted ${r.deletedFiles} files`; } }
  ];

  async function loadTaskSchedules() {
    try { taskSchedules = (await api.taskSchedules()).items ?? []; } catch { /* ignore */ }
    finally { taskSchedulesLoading = false; }
  }

  async function runTask(task: TaskDef) {
    taskRunning = { ...taskRunning, [task.id]: true };
    const ranAt = new Date().toISOString();
    try {
      const detail = String(await task.run());
      taskResults = { ...taskResults, [task.id]: { ok: true, detail, ranAt } };
      toastSuccess(`${task.label}: ${detail}`);
      await loadTaskSchedules();
    } catch (err) {
      const detail = err instanceof Error ? err.message : String(err);
      taskResults = { ...taskResults, [task.id]: { ok: false, detail, ranAt } };
      toastError(`${task.label} failed: ${detail}`);
    } finally {
      taskRunning = { ...taskRunning, [task.id]: false };
    }
  }

  function taskScheduleFor(task: TaskDef) { return taskSchedules.find((s) => s.id === task.id); }
  function fmtTaskTime(iso: string) { return new Date(iso).toLocaleString('en-GB', { month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' }); }
  $: taskGroups = [...new Set(taskDefs.map((t) => t.group))];
  $: taskRunningCount = Object.values(taskRunning).filter(Boolean).length;

  // ── Quality Profiles tab state ──────────────────────────────────────────────
  const ALL_RESOLUTIONS = ['2160p', '1080p', '720p', '576p', '480p'];
  const ALL_SOURCES     = ['BluRay', 'Remux', 'WEB-DL', 'WEBRip', 'HDTV', 'DVDRip'];
  const ALL_CODECS      = ['x265', 'HEVC', 'x264', 'AVC', 'AV1', 'VP9'];
  const ALL_LANGUAGES   = ['nl', 'en', 'de', 'fr', 'es', 'pt', 'it', 'ja', 'ko', 'zh', 'multi'];
  const ALL_AUDIO       = ['Atmos', 'TrueHD', 'DTS-HD', 'DTS', 'DD+', 'AC3', 'AAC', 'FLAC', 'MP3'];
  const ALL_HDR         = ['DV', 'HDR10+', 'HDR10', 'HLG', 'SDR'];

  let selectedProfile: QualityProfile | null = null;
  let profileSaving = false;

  // ── Indexer Policies tab state ──────────────────────────────────────────────
  let indexerPolicies: IndexerPolicy[] = [];
  let editingPolicy: IndexerPolicy | null = null;
  let ipSaving = false;

  async function loadIndexerPolicies() {
    try {
      const res = await api.listIndexerPolicies();
      indexerPolicies = res.items ?? [];
    } catch { /* ignore */ }
  }

  async function saveIndexerPolicy() {
    if (!editingPolicy) return;
    ipSaving = true;
    try {
      let saved: IndexerPolicy;
      if (editingPolicy.id) {
        saved = await api.updateIndexerPolicy(editingPolicy);
        indexerPolicies = indexerPolicies.map(p => p.id === saved.id ? saved : p);
      } else {
        saved = await api.upsertIndexerPolicy(editingPolicy);
        const existing = indexerPolicies.findIndex(p => p.indexerName === saved.indexerName);
        if (existing >= 0) {
          indexerPolicies = indexerPolicies.map(p => p.indexerName === saved.indexerName ? saved : p);
        } else {
          indexerPolicies = [...indexerPolicies, saved];
        }
      }
      editingPolicy = null;
      toastSuccess('Saved');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
    finally { ipSaving = false; }
  }

  async function deleteIndexerPolicy(id: number) {
    try {
      await api.deleteIndexerPolicy(id);
      indexerPolicies = indexerPolicies.filter(p => p.id !== id);
      editingPolicy = null;
      toastSuccess('Deleted');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
  }

  // ── Subtitle Profiles tab state ─────────────────────────────────────────────
  let subtitleProfiles: SubtitleProfile[] = [];
  let editingSubtitleProfile: SubtitleProfile | null = null;
  let spSaving = false;

  async function loadSubtitleProfiles() {
    try {
      const res = await api.listSubtitleProfiles();
      subtitleProfiles = res.items ?? [];
    } catch { /* ignore */ }
  }

  async function saveSubtitleProfile() {
    if (!editingSubtitleProfile) return;
    spSaving = true;
    try {
      let saved: SubtitleProfile;
      if (editingSubtitleProfile.id) {
        saved = await api.updateSubtitleProfile(editingSubtitleProfile);
        subtitleProfiles = subtitleProfiles.map(p => p.id === saved.id ? saved : p);
      } else {
        saved = await api.createSubtitleProfile(editingSubtitleProfile);
        subtitleProfiles = [...subtitleProfiles, saved];
      }
      editingSubtitleProfile = null;
      toastSuccess('Saved');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
    finally { spSaving = false; }
  }

  async function deleteSubtitleProfile(id: number) {
    try {
      await api.deleteSubtitleProfile(id);
      subtitleProfiles = subtitleProfiles.filter(p => p.id !== id);
      editingSubtitleProfile = null;
      toastSuccess('Deleted');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
  }

  // ── Custom Formats tab state ────────────────────────────────────────────────
  let customFormats: CustomFormat[] = [];
  let cfSaving = false;
  let cfImportOpen = false;
  let cfImportJson = '';
  let cfImporting = false;

  function blankFormat(): CustomFormat {
    return { name: '', pattern: '', score: 0, enabled: true, source: 'custom' };
  }

  let editingFormat: CustomFormat | null = null;

  async function importCustomFormats() {
    cfImporting = true;
    try {
      const parsed = JSON.parse(cfImportJson);
      const formats: CustomFormat[] = Array.isArray(parsed) ? parsed : [parsed];
      const result = await api.importCustomFormats(formats);
      toastSuccess(`Imported ${result.imported} of ${result.total} custom formats`);
      cfImportOpen = false;
      cfImportJson = '';
      await loadCustomFormats();
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
    finally { cfImporting = false; }
  }

  async function loadCustomFormats() {
    try {
      const res = await api.listCustomFormats();
      customFormats = res.items ?? [];
    } catch { /* ignore */ }
  }

  // ── Release block rules ──────────────────────────────────────────────────
  let blockRules: ReleaseBlockRule[] = [];
  let bfSaving = false;
  let editingRule: ReleaseBlockRule | null = null;
  let testTitle = '';
  let testMediaType: 'movie' | 'tv' | 'both' = 'both';
  let testResult: BlockTestResult | null = null;
  let testRunning = false;

  function blankRule(): ReleaseBlockRule {
    return { type: 'release_group', pattern: '', mediaType: 'both', action: 'block', scorePenalty: 0, enabled: true, source: 'custom', note: '' };
  }

  async function loadBlockRules() {
    try {
      const res = await api.listReleaseBlockRules();
      blockRules = res.items ?? [];
    } catch { /* ignore */ }
  }

  async function saveBlockRule() {
    if (!editingRule) return;
    bfSaving = true;
    try {
      let saved: ReleaseBlockRule;
      if (editingRule.id) {
        saved = await api.updateReleaseBlockRule(editingRule);
        blockRules = blockRules.map(r => r.id === saved.id ? saved : r);
      } else {
        saved = await api.createReleaseBlockRule(editingRule);
        blockRules = [...blockRules, saved];
      }
      editingRule = null;
      toastSuccess('Saved');
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { bfSaving = false; }
  }

  async function deleteBlockRule(id: number) {
    try {
      await api.deleteReleaseBlockRule(id);
      blockRules = blockRules.filter(r => r.id !== id);
      editingRule = null;
      toastSuccess('Deleted');
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
  }

  async function runBlockTest() {
    if (!testTitle.trim()) return;
    testRunning = true;
    testResult = null;
    try {
      testResult = await api.testReleaseBlockRule(testTitle.trim(), testMediaType);
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { testRunning = false; }
  }

  $: blockRuleGroups = {
    release_group: blockRules.filter(r => r.type === 'release_group'),
    title_pattern: blockRules.filter(r => r.type === 'title_pattern'),
    regex: blockRules.filter(r => r.type === 'regex'),
    missing_release_group: blockRules.filter(r => r.type === 'missing_release_group'),
  };

  async function saveFormat() {
    if (!editingFormat) return;
    cfSaving = true;
    try {
      if (editingFormat.id) {
        const updated = await api.updateCustomFormat(editingFormat);
        customFormats = customFormats.map(f => f.id === updated.id ? updated : f);
      } else {
        const created = await api.createCustomFormat(editingFormat);
        customFormats = [...customFormats, created];
      }
      editingFormat = null;
      toastSuccess('Custom format saved');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
    finally { cfSaving = false; }
  }

  async function deleteFormat(id: number) {
    try {
      await api.deleteCustomFormat(id);
      customFormats = customFormats.filter(f => f.id !== id);
      if (editingFormat?.id === id) editingFormat = null;
      toastSuccess('Custom format deleted');
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
  }

  function blankProfile(): QualityProfile {
    return { name: 'New Profile', isDefault: false, resolutions: ['1080p', '2160p', '720p'], sources: ['WEB-DL', 'BluRay', 'WEBRip'], codecs: ['x265', 'x264'], languages: ['nl', 'en'], audioFormats: ['TrueHD', 'DTS-HD', 'DTS', 'DD+', 'AC3', 'AAC'], hdrFormats: ['HDR10', 'SDR'], excludePatterns: [], preferProper: true, preferRepack: true, rejectCam: true, allowUpgrade: false, minimumUpgradeCustomFormatScore: 0, cutoffResolution: '', minimumAgeHours: 0, minMbPerMinute: 0, maxMbPerMinute: 0 };
  }

  async function saveSelectedProfile() {
    if (!selectedProfile) return;
    profileSaving = true;
    try {
      const saved = await api.saveProfile(selectedProfile);
      toastSuccess(`Profile "${saved.name}" saved`);
      const pr = await api.listProfiles();
      profiles = pr.profiles ?? [];
      const found = profiles.find(p => p.name === saved.name);
      if (found) selectedProfile = { ...found };
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
    finally { profileSaving = false; }
  }

  async function deleteSelectedProfile(p: QualityProfile) {
    if (!p.id || p.isDefault) return;
    try {
      await api.deleteProfile(p.id);
      toastSuccess(`Profile "${p.name}" deleted`);
      if (selectedProfile?.id === p.id) selectedProfile = null;
      const pr = await api.listProfiles();
      profiles = pr.profiles ?? [];
    } catch (err) { toastError(err instanceof Error ? err.message : String(err)); }
  }

  function profileMoveUp(arr: string[], i: number): string[] { if (i === 0) return arr; const n = [...arr]; [n[i-1], n[i]] = [n[i], n[i-1]]; return n; }
  function profileMoveDown(arr: string[], i: number): string[] { if (i >= arr.length - 1) return arr; const n = [...arr]; [n[i], n[i+1]] = [n[i+1], n[i]]; return n; }
  function profileToggle(arr: string[], v: string): string[] { return arr.includes(v) ? arr.filter(x => x !== v) : [...arr, v]; }

  // ── Plex OAuth state ────────────────────────────────────────────────────────
  type PlexPin = { pinId: number; code: string; authUrl: string };
  let plexPin: PlexPin | null = null;
  let plexPollInterval: number | undefined;

  async function startPlexOAuth() {
    try {
      const pin = await api.plexOauthStart();
      plexPin = pin;
      window.open(pin.authUrl, '_blank', 'noopener,noreferrer');
      window.clearInterval(plexPollInterval);
      plexPollInterval = window.setInterval(async () => {
        if (!plexPin) { window.clearInterval(plexPollInterval); return; }
        try {
          const result = await api.plexOauthPoll(plexPin.pinId);
          if (result.authorized && result.token) {
            window.clearInterval(plexPollInterval);
            plexPin = null;
            if (draft) { draft.plex.token = result.token; }
            toastSuccess('Plex token retrieved — save to apply');
          }
        } catch { /* retry next tick */ }
      }, 3000);
    } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
  }

  function cancelPlexOAuth() {
    window.clearInterval(plexPollInterval);
    plexPin = null;
  }

  const queueDecisionRows = [
    // ── Drakkar-native failures ───────────────────────────────────────────────
    ['allCandidatesWrongTitle',     'All candidates matched search but had wrong titles'],
    ['allCandidatesRejected',       'All candidates were rejected (bad source, size, quality)'],
    ['noReleaseFound',              'No NZBs returned by indexers'],
    ['preflightFailed',             'Archive inspection (preflight) failed'],
    ['nzbFetch4xx',                 'NZB fetch returned a permanent HTTP error (401/404/410/451)'],
    ['nzbFetch403',                 'NZB fetch returned 403 (quota or rate-limit)'],
    ['nzbFetchFailed',              'NZB fetch failed for a transient reason'],
    ['publishFailed',               'Publishing (FUSE/symlink) failed'],
    ['badSource',                   'Bad source detected (CAM, TS, etc.)'],
    ['interruptedByRestart',        'Download was interrupted by a process restart'],
    // ── Sonarr/Radarr-compatible failures ────────────────────────────────────
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
    if (tab === 'logs' && logEntries.length === 0) void loadLogs();
  }

  function cloneSettings(s: FullSettings): FullSettings {
    return JSON.parse(JSON.stringify(s));
  }

  function emptyProvider(): UsenetProvider {
    return { name: '', host: '', port: 563, tls: true, username: '', password: '', maxConnections: 10, priority: 0, retentionDays: 0, backup: false, enabled: true };
  }

  async function loadAll() {
    loading = true;
    try {
      const [s, pr, qdRes, pol, fs] = await Promise.all([
        api.status(),
        api.listProfiles(),
        api.listQualityDefinitions(),
        api.policies(),
        api.getSettings()
      ]);
      status = s;
      profiles = pr.profiles;
      qualityDefs = qdRes.definitions ?? [];
      policySettings = pol;
      fullSettings = fs;
      draft = cloneSettings(fs);
      // Apply frontend defaults for fields that may be absent from older settings.json
      if (draft && !draft.indexer) {
        draft.indexer = { tvRssSyncIntervalMinutes: 15, movieRssSyncIntervalMinutes: 30, minimumAgeMinutes: 0, retentionDays: 0, maximumSizeMB: 0, searchDelayMs: 0, backgroundSearchWorkers: 12 };
      } else if (draft?.indexer && !draft.indexer.backgroundSearchWorkers) {
        draft.indexer.backgroundSearchWorkers = 12;
      }
      if (draft && !draft.jellyfin) {
        draft.jellyfin = { url: '', apiKey: '' };
      }
      if (draft && !draft.notifications) {
        draft.notifications = { discordWebhookUrl: '', genericWebhookUrl: '', onGrab: false, onAvailable: true, onFailed: false };
      }
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      loading = false;
    }
  }

  async function saveQualityDef(d: QualityDefinition) {
    qualityDefsSaving = new Set([...qualityDefsSaving, d.id]);
    try {
      const updated = await api.updateQualityDefinition(d);
      qualityDefs = qualityDefs.map(x => x.id === updated.id ? updated : x);
      qualityDefsDirty.delete(d.id);
      qualityDefsDirty = new Set(qualityDefsDirty);
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      qualityDefsSaving.delete(d.id);
      qualityDefsSaving = new Set(qualityDefsSaving);
    }
  }

  async function saveSettings() {
    if (!draft) return;
    working = true;
    try {
      const saved = await api.saveSettings(draft);
      fullSettings = saved;
      draft = cloneSettings(saved);
      toastSuccess('Settings saved — restart Drakkar to apply connection changes');
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  function addProvider() {
    if (!draft) return;
    draft.usenet.providers = [...draft.usenet.providers, emptyProvider()];
  }

  function removeProvider(i: number) {
    if (!draft) return;
    draft.usenet.providers = draft.usenet.providers.filter((_, idx) => idx !== i);
  }

  async function loadBlocklist() {
    blLoading = true;
    try {
      const [page, stats] = await Promise.all([
        api.blocklistPaged({ page: blPage, pageSize: blPageSize, q: blockQuery || undefined, reason: blockReasonFilter !== 'all' ? blockReasonFilter : undefined, sort: blockSortCol === 'expires' ? 'expiresAt' : blockSortCol === 'createdAt' ? 'createdAt' : blockSortCol, dir: blockSortDir }),
        api.blocklistStats()
      ]);
      blocklist = page.items ?? [];
      blTotal = page.total;
      blTotalPages = page.totalPages;
      blStats = stats;
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      blLoading = false;
    }
  }

  async function clearBlocklist(id: number) {
    if (typeof window !== 'undefined' && !window.confirm('Clear this runtime blocklist entry?')) return;
    working = true;
    try {
      await api.clearBlocklist(id);
      toastSuccess('Blocklist item cleared');
      await loadBlocklist();
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function clearAllBlocklist() {
    if (typeof window !== 'undefined' && !window.confirm('Clear all active runtime blocklist entries?')) return;
    working = true;
    try {
      const r = await api.clearAllBlocklist();
      toastSuccess(`Cleared ${r.cleared} blocklist entr${r.cleared === 1 ? 'y' : 'ies'}`);
      blPage = 1;
      await loadBlocklist();
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function clearBlocklistByReason(reason: string) {
    if (typeof window !== 'undefined' && !window.confirm(`Clear all active runtime blocklist entries with reason "${reason}"?`)) return;
    working = true;
    try {
      const r = await api.clearBlocklistByReason(reason);
      toastSuccess(`Cleared ${r.cleared} ${reason} entr${r.cleared === 1 ? 'y' : 'ies'}`);
      blPage = 1;
      await loadBlocklist();
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  async function copyBlocklistKey(key: string) {
    try {
      await navigator.clipboard.writeText(key);
      toastSuccess('Blocklist key copied');
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    }
  }

  function resetBlockEditor() {
    blockEditor = blankBlocklistEditor();
  }

  function toDatetimeLocal(value?: string) {
    if (!value) return '';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return '';
    return new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 16);
  }

  function startEditBlocklist(item: BlocklistItem) {
    blockEditor = {
      id: item.id,
      keyType: 'raw',
      key: item.key,
      externalUrl: item.keyType === 'external_url' ? item.key.replace(/^external_url:/, '') : '',
      releaseTitle: item.releaseTitle || '',
      indexerName: item.indexerName || '',
      sizeMb: item.sizeBytes ? Math.round(item.sizeBytes / (1024 * 1024)) : 0,
      postedDate: item.postedAt ? item.postedAt.slice(0, 10) : '',
      reason: item.reason || 'manual',
      expiresAt: toDatetimeLocal(item.expiresAt)
    };
  }

  async function saveBlocklistEntry() {
    const payload: BlocklistMutation = {
      keyType: blockEditor.keyType,
      key: blockEditor.key.trim(),
      externalUrl: blockEditor.externalUrl.trim(),
      releaseTitle: blockEditor.releaseTitle.trim(),
      indexerName: blockEditor.indexerName.trim(),
      sizeMb: blockEditor.sizeMb,
      postedDate: blockEditor.postedDate.trim(),
      reason: blockEditor.reason.trim() || 'manual',
      expiresAt: blockEditor.expiresAt ? new Date(blockEditor.expiresAt).toISOString() : undefined
    };
    working = true;
    try {
      if (blockEditor.id) {
        await api.updateBlocklist(blockEditor.id, payload);
        toastSuccess('Runtime blocklist entry updated');
      } else {
        await api.createManualBlocklist(payload);
        toastSuccess('Runtime blocklist entry created');
      }
      resetBlockEditor();
      blPage = 1;
      await loadBlocklist();
    } catch (e) {
      toastError(e instanceof Error ? e.message : String(e));
    } finally {
      working = false;
    }
  }

  function blocklistKeyLabel(item: BlocklistItem) {
    switch (item.keyType) {
      case 'external_url': return 'URL';
      case 'release_signature': return 'Signature';
      default: return 'Key';
    }
  }

  function blocklistContext(item: BlocklistItem) {
    const parts: string[] = [];
    if (item.releaseTitle) parts.push(item.releaseTitle);
    if (item.indexerName) parts.push(item.indexerName);
    if (item.sizeBytes) parts.push(bytes(item.sizeBytes));
    return parts.join(' • ');
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
    void loadTaskSchedules();
    void loadCustomFormats();
    void loadBlockRules();
    void loadIndexerPolicies();
    void loadSubtitleProfiles();
    const unsub = subscribeEvents(() => { if (!working) void loadAll(); });
    const timer = window.setInterval(() => void loadAll(), 30000);
    const taskTimer = window.setInterval(() => void loadTaskSchedules(), 30000);
    return () => {
      window.clearInterval(timer);
      window.clearInterval(taskTimer);
      window.clearInterval(plexPollInterval);
      unsub();
    };
  });

  $: integrationEntries = status ? Object.entries(status.integrations).filter(([n]) => n !== 'subtitleProviders') : [];
  $: subtitleProviderEntries = status ? Object.entries(status.integrations.subtitleProviders) : [];

  $: if (activeTab === 'rules' && !blLoading && blStats === null) { void loadBlocklist(); }
  $: filteredBlocklist = blocklist;

  $: configuredCount = integrationEntries.filter(([, v]) => v.configured).length;
  $: enabledProviders = (draft?.usenet.providers ?? []).filter((p) => p.enabled).length;
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
        <tab.icon size={15} />
        <span>{tab.label}</span>
      </button>
    {/each}
  </aside>

  <div class="tab-content">

    <!-- INTEGRATIONS -->
    {#if activeTab === 'integrations'}
      {#if draft}
        <div class="grid-2">
          <Panel title="NZBHydra2" subtitle="Newznab aggregator for NZB indexing.">
            <div class="form-grid">
              <label class="form-field">
                <span>URL</span>
                <input type="url" bind:value={draft.nzbhydra2.url} placeholder="http://nzbhydra2:5076" />
              </label>
              <label class="form-field">
                <span>API Key</span>
                <input type="password" bind:value={draft.nzbhydra2.apiKey} placeholder="••••••••" autocomplete="off" />
              </label>
              <label class="form-field">
                <span>Search Cache TTL (s)</span>
                <input type="number" bind:value={draft.nzbhydra2.searchCacheTtlSeconds} min="0" />
              </label>
              <label class="form-field">
                <span>Feed Cache TTL (s)</span>
                <input type="number" bind:value={draft.nzbhydra2.feedCacheTtlSeconds} min="0" />
              </label>
              <label class="form-field">
                <span>Feed Max Results</span>
                <input type="number" bind:value={draft.nzbhydra2.feedMaxResults} min="0" />
              </label>
            </div>
          </Panel>

          <Panel title="Seerr" subtitle="Request management.">
            <div class="form-grid">
              <label class="form-field">
                <span>URL</span>
                <input type="url" bind:value={draft.seerr.url} placeholder="http://seerr:5055" />
              </label>
              <label class="form-field">
                <span>API Key</span>
                <input type="password" bind:value={draft.seerr.apiKey} placeholder="••••••••" autocomplete="off" />
              </label>
            </div>

            <div class="webhook-setup">
              <div class="webhook-setup__header">
                <Webhook size={15} />
                <span>Webhook setup</span>
              </div>
              <p class="webhook-setup__desc">
                Configure a webhook in Seerr so Drakkar receives instant notifications when requests
                are approved — without waiting for the next 10-minute sync.
              </p>
              <ol class="webhook-setup__steps">
                <li>In Seerr, go to <strong>Settings → Notifications → Webhook</strong></li>
                <li>Enable the webhook and paste the URL below</li>
                <li>
                  Under <strong>Notification Types</strong>, enable at minimum:<br />
                  <code>Request Approved</code>, <code>Request Auto-Approved</code>
                </li>
                <li>Leave <strong>JSON Payload</strong> at its default (Seerr standard format)</li>
                <li>Save and use <strong>Test</strong> to verify the connection</li>
              </ol>
              <div class="webhook-url-row">
                <code class="webhook-url">{webhookUrl}</code>
                <button class="copy-btn" on:click={copyWebhookUrl} title="Copy webhook URL">
                  {#if webhookCopied}
                    <Check size={14} />
                  {:else}
                    <Copy size={14} />
                  {/if}
                </button>
              </div>
            </div>
          </Panel>
        </div>

        <div class="grid-2">
          <Panel title="Metadata" subtitle="TMDB and TVDB API keys, language and cache settings.">
            <div class="form-grid">
              <label class="form-field">
                <span>TMDB API Key</span>
                <input type="password" bind:value={draft.metadata.tmdb.apiKey} placeholder="••••••••" autocomplete="off" />
              </label>
              <label class="form-field">
                <span>TVDB API Key</span>
                <input type="password" bind:value={draft.metadata.tvdb.apiKey} placeholder="••••••••" autocomplete="off" />
              </label>
              <label class="form-field">
                <span>Language</span>
                <input type="text" bind:value={draft.metadata.language} placeholder="en-US" />
              </label>
              <label class="form-field">
                <span>Cache TTL (hours)</span>
                <input type="number" bind:value={draft.metadata.cacheTtlHours} min="0" />
              </label>
            </div>
          </Panel>

          <Panel title="Subtitles" subtitle="Subtitle provider credentials and language preferences.">
            <div class="form-grid">
              <label class="form-field form-field--toggle">
                <span>Enabled</span>
                <input type="checkbox" bind:checked={draft.subtitles.enabled} />
              </label>
              <label class="form-field">
                <span>Languages (comma-separated)</span>
                <input
                  type="text"
                  value={draft.subtitles.languages.join(', ')}
                  on:change={(e) => {
                    if (!draft) return;
                    draft.subtitles.languages = (e.currentTarget as HTMLInputElement).value
                      .split(',').map(l => l.trim()).filter(Boolean);
                  }}
                  placeholder="en, nl"
                />
              </label>
            </div>
            {#each Object.entries(draft.subtitles.providers ?? {}) as [name, p]}
              <div class="sub-provider">
                <div class="sub-provider-head">
                  <strong>{name}</strong>
                  <label class="toggle-label">
                    <input type="checkbox" bind:checked={draft.subtitles.providers[name].enabled} />
                    <span>enabled</span>
                  </label>
                </div>
                <div class="form-grid form-grid--compact">
                  <label class="form-field">
                    <span>API Key</span>
                    <input type="password" bind:value={draft.subtitles.providers[name].apiKey} placeholder="••••••••" autocomplete="off" />
                  </label>
                  {#if name !== 'subdl'}
                  <label class="form-field">
                    <span>Username</span>
                    <input type="text" bind:value={draft.subtitles.providers[name].username} />
                  </label>
                  <label class="form-field">
                    <span>Password</span>
                    <input type="password" bind:value={draft.subtitles.providers[name].password} placeholder="••••••••" autocomplete="off" />
                  </label>
                  {/if}
                </div>
              </div>
            {/each}
          </Panel>
        </div>

        <Panel title="Default Quality Profiles" subtitle="Fallback profiles used when Seerr doesn't specify one.">
          <div class="form-grid">
            <label class="form-field">
              <span>Default Movie Profile</span>
              <select bind:value={draft.library.defaultMovieProfile}>
                <option value="">— none —</option>
                {#each profiles as p}
                  <option value={p.name}>{p.name}{p.isDefault ? ' (default)' : ''}</option>
                {/each}
              </select>
            </label>
            <label class="form-field">
              <span>Default TV Profile</span>
              <select bind:value={draft.library.defaultTvProfile}>
                <option value="">— none —</option>
                {#each profiles as p}
                  <option value={p.name}>{p.name}{p.isDefault ? ' (default)' : ''}</option>
                {/each}
              </select>
            </label>
          </div>
        </Panel>

        <div class="actions-row">
          <Button kind="primary" on:click={saveSettings} disabled={working}>
            <Save size={16} />
            Save Integrations
          </Button>
        </div>
      {:else}
        <div class="empty">Loading settings…</div>
      {/if}

      <Panel title="Integration Probes" subtitle="Live reachability and auth checks. Click Probe above to run.">
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

    <!-- PROVIDERS -->
    {:else if activeTab === 'providers'}
      {#if draft}
        <Panel title="Connection Budget" subtitle="Global NNTP connection limits (requires restart to take effect).">
          <div class="form-grid form-grid--3col">
            <label class="form-field">
              <span>Max Download Connections</span>
              <input type="number" bind:value={draft.usenet.maxDownloadConnections} min="1" max="500" />
            </label>
            <label class="form-field">
              <span>Streaming Priority %</span>
              <input type="number" bind:value={draft.usenet.streamingPriorityPercent} min="0" max="100" />
            </label>
            <label class="form-field">
              <span>Article Buffer Size</span>
              <input type="number" bind:value={draft.usenet.articleBufferSize} min="1" max="500" />
            </label>
          </div>
        </Panel>

        <Panel title="Usenet Providers" subtitle="NNTP server credentials and per-provider connection pools.">
          <div class="provider-forms">
            {#each draft.usenet.providers as p, i}
              <div class="provider-edit-card">
                <div class="provider-edit-head">
                  <div class="pec-title">
                    <strong>{p.name || `Provider ${i + 1}`}</strong>
                    <StatusPill tone={p.enabled ? 'ok' : 'neutral'}>{p.enabled ? 'enabled' : 'disabled'}</StatusPill>
                  </div>
                  <button class="icon-btn danger" on:click={() => removeProvider(i)} title="Remove provider">
                    <X size={14} />
                  </button>
                </div>
                <div class="form-grid form-grid--2col">
                  <label class="form-field">
                    <span>Name</span>
                    <input type="text" bind:value={p.name} placeholder="Newshosting" />
                  </label>
                  <label class="form-field">
                    <span>Host</span>
                    <input type="text" bind:value={p.host} placeholder="news.example.com" />
                  </label>
                  <label class="form-field">
                    <span>Port</span>
                    <input type="number" bind:value={p.port} min="1" max="65535" />
                  </label>
                  <label class="form-field">
                    <span>Max Connections</span>
                    <input type="number" bind:value={p.maxConnections} min="1" max="500" />
                  </label>
                  <label class="form-field">
                    <span>Priority <small class="field-hint-inline">(lower = higher priority)</small></span>
                    <input type="number" bind:value={p.priority} min="0" />
                  </label>
                  <label class="form-field">
                    <span>Retention (days) <small class="field-hint-inline">(0 = unlimited)</small></span>
                    <input type="number" bind:value={p.retentionDays} min="0" />
                  </label>
                  <label class="form-field">
                    <span>Username</span>
                    <input type="text" bind:value={p.username} autocomplete="off" />
                  </label>
                  <label class="form-field">
                    <span>Password</span>
                    <input type="password" bind:value={p.password} placeholder="••••••••" autocomplete="off" />
                  </label>
                </div>
                <div class="provider-edit-footer">
                  <label class="toggle-label">
                    <input type="checkbox" bind:checked={p.tls} />
                    <span>TLS</span>
                  </label>
                  <label class="toggle-label">
                    <input type="checkbox" bind:checked={p.backup} />
                    <span>Backup server</span>
                  </label>
                  <label class="toggle-label">
                    <input type="checkbox" bind:checked={p.enabled} />
                    <span>Enabled</span>
                  </label>
                </div>
              </div>
            {/each}
          </div>
          <button class="add-btn" on:click={addProvider}>
            <Plus size={15} />
            Add Provider
          </button>
        </Panel>

        <div class="actions-row">
          <Button kind="primary" on:click={saveSettings} disabled={working}>
            <Save size={16} />
            Save Providers
          </Button>
        </div>
      {:else}
        <div class="empty">Loading settings…</div>
      {/if}

    <!-- INDEXERS -->
    {:else if activeTab === 'indexers'}
      {#if draft}
        <Panel title="Indexer Settings" subtitle="Mirrors Sonarr/Radarr Settings → Indexers. Controls how Drakkar searches NZBHydra2.">
          <div class="form-grid form-grid--2col">
            <label class="form-field">
              <span>TV RSS Sync Interval (minutes)</span>
              <input type="number" min="15" max="120" bind:value={draft.indexer.tvRssSyncIntervalMinutes} />
              <small class="field-hint">How often to poll for new TV/episode releases. Minimum 15 min (Sonarr default). Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Movie RSS Sync Interval (minutes)</span>
              <input type="number" min="30" max="120" bind:value={draft.indexer.movieRssSyncIntervalMinutes} />
              <small class="field-hint">How often to poll for new movie releases. Minimum 30 min (Radarr default). Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Minimum Age (minutes)</span>
              <input type="number" min="0" bind:value={draft.indexer.minimumAgeMinutes} />
              <small class="field-hint">Don't grab a release posted less than this many minutes ago. Gives Usenet time to propagate. Sonarr/Radarr default: 0. Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Retention (days)</span>
              <input type="number" min="0" bind:value={draft.indexer.retentionDays} />
              <small class="field-hint">Skip releases older than this. Set to match your Usenet provider's retention. 0 = unlimited. Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Maximum Size (MB)</span>
              <input type="number" min="0" bind:value={draft.indexer.maximumSizeMB} />
              <small class="field-hint">Reject releases larger than this. 0 = no limit. Sonarr/Radarr default: 0. Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Search Delay (ms)</span>
              <input type="number" min="0" bind:value={draft.indexer.searchDelayMs} />
              <small class="field-hint">Minimum delay between consecutive NZBHydra2 API calls. 0 = no throttle (Sonarr/Radarr behaviour — NZBHydra2 handles per-indexer rate limiting). Applies immediately.</small>
            </label>
            <label class="form-field">
              <span>Background Search Workers</span>
              <input type="number" min="1" bind:value={draft.indexer.backgroundSearchWorkers} />
              <small class="field-hint">Concurrent BullMQ workers used for missing-item and backlog searches. Higher values drain big queues faster but increase Hydra/indexer load. Applies immediately.</small>
            </label>
          </div>
        </Panel>
        <div class="settings-actions">
          <Button kind="primary" on:click={saveSettings} disabled={working}>
            <Save size={14} /> {working ? 'Saving…' : 'Save Indexer Settings'}
          </Button>
        </div>
      {/if}

      <Panel title="Per-Indexer Policies" subtitle="Assign a static score modifier to releases from a specific indexer. Positive boosts, negative penalises.">
        <div class="cf-layout">
          <div class="cf-list">
            <div class="cf-list-header">
              <span>Policies</span>
              <Button kind="ghost" on:click={() => { editingPolicy = { indexerName: '', scoreModifier: 0, enabled: true, note: '' }; }}>
                <Plus size={14} /> Add
              </Button>
            </div>
            {#if indexerPolicies.length === 0}
              <div class="cf-empty">No per-indexer policies yet.</div>
            {/if}
            {#each indexerPolicies as p (p.id)}
              <button class="cf-item" class:cf-active={editingPolicy?.id === p.id} on:click={() => { editingPolicy = { ...p }; }}>
                <span class="cf-item-name">{p.indexerName}</span>
                <div style="display:flex;align-items:center;gap:4px;flex-shrink:0">
                  <span class="cf-item-score" class:cf-pos={p.scoreModifier > 0} class:cf-neg={p.scoreModifier < 0}>{p.scoreModifier > 0 ? '+' : ''}{p.scoreModifier}</span>
                  {#if !p.enabled}<span class="cf-disabled-badge">off</span>{/if}
                </div>
              </button>
            {/each}
          </div>
          <div class="cf-editor">
            {#if editingPolicy}
              <div class="field">
                <label class="field-label" for="ip-name">Indexer Name <span class="field-hint">(exact match — case-sensitive)</span></label>
                <input id="ip-name" type="text" bind:value={editingPolicy.indexerName} placeholder="e.g. NZBFinder" disabled={!!editingPolicy.id} />
              </div>
              <div class="field">
                <label class="field-label" for="ip-score">Score Modifier</label>
                <input id="ip-score" type="number" bind:value={editingPolicy.scoreModifier} placeholder="e.g. 50 or -100" />
              </div>
              <div class="field">
                <label class="field-label" for="ip-note">Note <span class="field-hint">(optional)</span></label>
                <input id="ip-note" type="text" bind:value={editingPolicy.note} placeholder="Why this modifier exists" />
              </div>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingPolicy.enabled} />
                <div><strong>Enabled</strong><span>Apply this modifier when scoring releases</span></div>
              </label>
              <div class="editor-actions" style="margin-top:16px">
                {#if editingPolicy.id}
                  <Button kind="danger" on:click={() => editingPolicy?.id && deleteIndexerPolicy(editingPolicy.id)}>
                    <Trash2 size={15} /> Delete
                  </Button>
                {/if}
                <Button kind="ghost" on:click={() => { editingPolicy = null; }}>Cancel</Button>
                <Button kind="primary" on:click={saveIndexerPolicy} disabled={ipSaving}>
                  <Save size={15} /> {ipSaving ? 'Saving…' : 'Save'}
                </Button>
              </div>
            {:else}
              <div class="cf-empty">Select a policy to edit, or add a new one.</div>
            {/if}
          </div>
        </div>
      </Panel>

    <!-- QUEUE -->
    {:else if activeTab === 'queue'}
      {#if draft}
        <div class="grid-2">
          <Panel title="Connection Budget" subtitle="Edit in the Providers tab. Shown here for reference.">
            <div class="kv-list">
              <div><span>Max connections</span><strong>{draft.usenet.maxDownloadConnections}</strong></div>
              <div><span>Streaming priority</span><strong>{draft.usenet.streamingPriorityPercent}%</strong></div>
              <div><span>Article buffer</span><strong>{draft.usenet.articleBufferSize}</strong></div>
              <div><span>Background queue depth</span><strong>{status?.backgroundQueueDepth ?? '—'}</strong></div>
            </div>
          </Panel>

          <Panel title="Queue Behavior" subtitle="Priority tiers — interactive playback always takes precedence.">
            <div class="kv-list">
              <div><span>Playback lane</span><strong>{draft.usenet.streamingPriorityPercent}% of pool</strong></div>
              <div><span>Background lane</span><strong>{status?.backgroundQueueDepth ?? 0} queued</strong></div>
              <div><span>Retry path</span><strong>candidate fallback first</strong></div>
              <div><span>Seek prefetch</span><strong>deferred until first read</strong></div>
            </div>
          </Panel>
        </div>
      {/if}

      {#if policySettings}
        <Panel title="Queue Behavior" subtitle="How Drakkar handles duplicates, video validation and import method.">
          <div class="form-grid">
            <label class="form-field">
              <span>Duplicate NZB Behavior</span>
              <select bind:value={policySettings.duplicateNzbBehavior}>
                <option value="mark_failed">Mark Failed</option>
                <option value="ignore_existing">Ignore Existing</option>
                <option value="download_again_with_suffix">Download Again (with suffix)</option>
                <option value="replace_existing">Replace Existing</option>
              </select>
            </label>
            <label class="form-field">
              <span>Import Strategy</span>
              <select bind:value={policySettings.importStrategy}>
                <option value="symlink">Symlink</option>
                <option value="strm">STRM</option>
                <option value="copy">Copy</option>
              </select>
            </label>
            <label class="form-field">
              <span>Manual Upload Category</span>
              <input type="text" bind:value={policySettings.manualUploadCategory} placeholder="e.g. manual" />
            </label>
            <label class="form-field form-field--toggle" style="align-items:center;flex-direction:row;gap:12px">
              <span>Fail NZBs Without Video</span>
              <input type="checkbox" bind:checked={policySettings.failNzbWithoutVideo} />
            </label>
            <label class="form-field">
              <span>Blocklist Expiry (days)</span>
              <input type="number" min="0" bind:value={policySettings.blocklistTtlDays} placeholder="0 = never expire" />
            </label>
          </div>
          <div class="actions-row">
            <Button kind="secondary" on:click={savePolicies} disabled={loading || working}>
              <Save size={16} />
              Save Behavior
            </Button>
          </div>
        </Panel>
      {/if}

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
              <Save size={16} />
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
            Root folder paths are compile-time defaults. Restart the container to change them via environment variables.
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
              <span class="naming-label">Episode Format</span>
              <code class="naming-token">&#123;Series Title&#125; - S&#123;season:00&#125;E&#123;episode:00&#125;.&#123;ext&#125;</code>
            </div>
            <div class="naming-example mono">Loki (2021) - S02E01.mkv</div>
          </div>
        </Panel>
      </div>

    <!-- RULES -->
    {:else if activeTab === 'rules'}
      <Panel title="Runtime Blocklist" subtitle="Operational blocks created from failed fetches, archive rejects, missing articles, and manual runtime clears. Separate from Release Filtering rules and Custom Formats scoring.">
        <div class="bl-editor">
          <div class="bl-editor-head">
            <div>
              <strong>{blockEditor.id ? `Edit Entry #${blockEditor.id}` : 'Add Manual Entry'}</strong>
              <p>Use a structured URL/signature entry or paste a raw runtime key directly.</p>
            </div>
            {#if blockEditor.id}
              <Button kind="ghost" on:click={resetBlockEditor} disabled={working}>
                <X size={14} />
                Cancel Edit
              </Button>
            {/if}
          </div>
          <div class="form-grid form-grid--3col">
            <label class="form-field">
              <span>Entry Type</span>
              <select bind:value={blockEditor.keyType} disabled={!!blockEditor.id}>
                <option value="external_url">External URL</option>
                <option value="release_signature">Release Signature</option>
                <option value="raw">Raw Key</option>
              </select>
            </label>
            <label class="form-field">
              <span>Reason</span>
              <input type="text" bind:value={blockEditor.reason} placeholder="manual" />
            </label>
            <label class="form-field">
              <span>Expires At</span>
              <input type="datetime-local" bind:value={blockEditor.expiresAt} />
            </label>
          </div>
          {#if blockEditor.keyType === 'external_url'}
            <div class="form-grid">
              <label class="form-field">
                <span>External URL</span>
                <input type="url" bind:value={blockEditor.externalUrl} placeholder="https://indexer.example/download/..." />
              </label>
            </div>
          {:else if blockEditor.keyType === 'release_signature'}
            <div class="form-grid form-grid--3col">
              <label class="form-field">
                <span>Release Title</span>
                <input type="text" bind:value={blockEditor.releaseTitle} placeholder="Dune.2021.2160p..." />
              </label>
              <label class="form-field">
                <span>Indexer Name</span>
                <input type="text" bind:value={blockEditor.indexerName} placeholder="NZB Finder" />
              </label>
              <label class="form-field">
                <span>Size (MB bucket)</span>
                <input type="number" min="0" bind:value={blockEditor.sizeMb} placeholder="7000" />
              </label>
            </div>
            <div class="form-grid">
              <label class="form-field">
                <span>Posted Date</span>
                <input type="date" bind:value={blockEditor.postedDate} />
              </label>
            </div>
          {:else}
            <div class="form-grid">
              <label class="form-field">
                <span>Raw Key</span>
                <input type="text" bind:value={blockEditor.key} placeholder="external_url:https://... or release_signature:..." />
              </label>
            </div>
          {/if}
          <div class="bl-editor-actions">
            <Button kind="primary" on:click={saveBlocklistEntry} disabled={working}>
              <Save size={14} />
              {blockEditor.id ? 'Update Entry' : 'Create Entry'}
            </Button>
          </div>
        </div>

        <!-- Stats chips -->
        {#if blStats}
          {@const sortedReasons = Object.entries(blStats.byReason).sort(([,a],[,b]) => b - a)}
          {@const visibleReasons = blShowAllReasons ? sortedReasons : sortedReasons.slice(0, BL_REASON_CHIP_LIMIT)}
          {@const hiddenCount = sortedReasons.length - BL_REASON_CHIP_LIMIT}
          <div class="bl-stats-row">
            <div class="bl-stat-chip">
              <span class="bl-stat-num">{blStats.active}</span>
              <span class="bl-stat-lbl">active</span>
            </div>
            <div class="bl-stat-chip warn">
              <span class="bl-stat-num">{blStats.expired}</span>
              <span class="bl-stat-lbl">expired</span>
            </div>
            {#each visibleReasons as [reason, count]}
              {@const label = reason.length > 40 ? reason.slice(0, 37) + '…' : reason}
              {@const colorKey = reason.split(':')[0].split('_')[0]}
              <button class="bl-reason-chip" class:active={blockReasonFilter === reason} title={reason}
                on:click={() => { blockReasonFilter = blockReasonFilter === reason ? 'all' : reason; blPage = 1; void loadBlocklist(); }}>
                <span class="reason-badge reason-{colorKey}">{label}</span>
                <span class="bl-reason-count">{count}</span>
              </button>
            {/each}
            {#if !blShowAllReasons && hiddenCount > 0}
              <button class="bl-show-more" on:click={() => blShowAllReasons = true}>+{hiddenCount} more</button>
            {:else if blShowAllReasons && sortedReasons.length > BL_REASON_CHIP_LIMIT}
              <button class="bl-show-more" on:click={() => blShowAllReasons = false}>show less</button>
            {/if}
          </div>
        {/if}

        <!-- Toolbar -->
        <div class="bl-toolbar">
          <div class="bl-search">
            <Search size={14} />
            <input bind:value={blockQuery} placeholder="Search key or reason…"
              on:input={() => { blPage = 1; void loadBlocklist(); }} />
          </div>
          {#if blockReasonFilter !== 'all'}
            <button class="bl-filter-active" on:click={() => { blockReasonFilter = 'all'; blPage = 1; void loadBlocklist(); }}>
              {blockReasonFilter} <X size={11} />
            </button>
          {/if}
          <div class="bl-stats-text mono">
            {blTotal} entr{blTotal === 1 ? 'y' : 'ies'}
          </div>
          <select class="bl-page-size" bind:value={blPageSize} on:change={() => { blPage = 1; void loadBlocklist(); }}>
            <option value={25}>25 / page</option>
            <option value={50}>50 / page</option>
            <option value={100}>100 / page</option>
          </select>
          {#if blTotal > 0}
            <Button kind="ghost" on:click={clearAllBlocklist} disabled={loading || working}>
              <X size={14} />
              Clear all
            </Button>
          {/if}
        </div>

        <!-- Table -->
        {#if blLoading}
          <div class="empty">Loading…</div>
        {:else if filteredBlocklist.length > 0}
          <div class="bl-table-wrap">
            <table class="bl-table">
              <thead>
                <tr>
                  <th class="sortable" on:click={() => { if (blockSortCol === 'reason') blockSortDir = blockSortDir === 'asc' ? 'desc' : 'asc'; else { blockSortCol = 'reason'; blockSortDir = 'asc'; } void loadBlocklist(); }}>
                    Reason {blockSortCol === 'reason' ? (blockSortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th class="sortable" on:click={() => { if (blockSortCol === 'key') blockSortDir = blockSortDir === 'asc' ? 'desc' : 'asc'; else { blockSortCol = 'key'; blockSortDir = 'asc'; } void loadBlocklist(); }}>
                    Runtime Key {blockSortCol === 'key' ? (blockSortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th>Matched Release</th>
                  <th class="sortable" on:click={() => { if (blockSortCol === 'createdAt') blockSortDir = blockSortDir === 'asc' ? 'desc' : 'asc'; else { blockSortCol = 'createdAt'; blockSortDir = 'desc'; } void loadBlocklist(); }}>
                    Added {blockSortCol === 'createdAt' ? (blockSortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th class="sortable" on:click={() => { if (blockSortCol === 'expires') blockSortDir = blockSortDir === 'asc' ? 'desc' : 'asc'; else { blockSortCol = 'expires'; blockSortDir = 'asc'; } void loadBlocklist(); }}>
                    Expires {blockSortCol === 'expires' ? (blockSortDir === 'asc' ? '↑' : '↓') : ''}
                  </th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {#each filteredBlocklist as item (item.id)}
                  <tr>
                    <td>
                      <span class="reason-badge reason-{item.reason.split('_')[0]}">{item.reason}</span>
                    </td>
                    <td class="bl-key-cell">
                      <div class="bl-key-top">
                        <span class="reason-badge neutral">{blocklistKeyLabel(item)}</span>
                        <button class="icon-btn" type="button" on:click={() => copyBlocklistKey(item.key)} title="Copy runtime key">
                          <Copy size={13} />
                        </button>
                      </div>
                      <div class="bl-key mono">{item.key}</div>
                    </td>
                    <td class="bl-context-cell">
                      {#if blocklistContext(item)}
                        <div class="bl-context-title">{item.releaseTitle || 'Matched release'}</div>
                        <div class="muted mono">{blocklistContext(item)}</div>
                        {#if item.libraryItemId || item.selectedReleaseId}
                          <div class="muted mono">
                            {#if item.libraryItemId}library #{item.libraryItemId}{/if}
                            {#if item.libraryItemId && item.selectedReleaseId} • {/if}
                            {#if item.selectedReleaseId}selected #{item.selectedReleaseId}{/if}
                          </div>
                        {/if}
                      {:else}
                        <div class="muted">No linked release metadata available.</div>
                      {/if}
                    </td>
                    <td class="muted mono">{item.createdAt ? new Date(item.createdAt).toLocaleDateString('en-GB') : '—'}</td>
                    <td class="muted mono">{item.expiresAt ? new Date(item.expiresAt).toLocaleDateString('en-GB') : 'Never'}</td>
                    <td class="bl-action">
                      <div class="bl-row-actions">
                        <button class="icon-btn" type="button" on:click={() => clearBlocklistByReason(item.reason)} disabled={working} title="Clear all with this reason">
                          <Trash2 size={13} />
                        </button>
                        <button class="icon-btn" type="button" on:click={() => startEditBlocklist(item)} disabled={working} title="Edit entry">
                          <Pencil size={13} />
                        </button>
                        <button class="clear-btn" on:click={() => clearBlocklist(item.id)} disabled={working} title="Clear this entry">
                        <X size={13} />
                        </button>
                      </div>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
          <!-- Pagination -->
          <div class="bl-pagination">
            <Pagination page={blPage} totalPages={blTotalPages} on:change={(e) => { blPage = e.detail; void loadBlocklist(); }} />
          </div>
        {:else}
          <div class="empty">{blStats?.active === 0 ? 'No active blocklist entries.' : 'No entries match the current filter.'}</div>
        {/if}
      </Panel>

      <div class="grid-2" style="margin-top:16px">
        <Panel title="Ignored File Patterns" subtitle="Patterns skipped from imported NZBs and library processing.">
          {#if policySettings}
            <textarea class="pattern-box" value={policySettings.ignoredPatterns.join('\n')} on:change={(e) => {
              const t = e.currentTarget as HTMLTextAreaElement;
              const cur = policySettings;
              if (!cur) return;
              policySettings = { ...cur, ignoredPatterns: t.value.split('\n').map((l) => l.trim()).filter(Boolean) };
            }}></textarea>
            <div class="actions-row">
              <Button kind="secondary" on:click={savePolicies} disabled={loading || working}>
                <Save size={16} />
                Save Patterns
              </Button>
            </div>
          {:else}
            <div class="empty">Unavailable.</div>
          {/if}
        </Panel>

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
      <div class="quality-sub-tabs">
        <button class="sub-tab-btn" class:active={qualitySubTab === 'profiles'} on:click={() => { qualitySubTab = 'profiles'; }} type="button">Profiles</button>
        <button class="sub-tab-btn" class:active={qualitySubTab === 'definitions'} on:click={() => { qualitySubTab = 'definitions'; }} type="button">Quality Definitions</button>
      </div>

      {#if qualitySubTab === 'definitions'}
        {@const movieDefs = qualityDefs.filter(d => d.mediaType === 'movie')}
        {@const episodeDefs = qualityDefs.filter(d => d.mediaType === 'episode')}
        <div class="qdef-shell">
          <Panel title="Movie Quality Definitions" subtitle="Per-tier size limits (MB/min) applied when ranking movie releases. Requires runtime metadata. Set 0 for no limit.">
            <table class="qdef-table">
              <thead><tr><th>Quality</th><th>Min (MB/min)</th><th>Max (MB/min)</th><th></th></tr></thead>
              <tbody>
                {#each movieDefs as d (d.id)}
                  <tr>
                    <td class="qdef-title">{d.title}</td>
                    <td><input type="number" min="0" class="qdef-input" bind:value={d.minMbPerMinute} on:input={() => { qualityDefsDirty = new Set([...qualityDefsDirty, d.id]); }} /></td>
                    <td><input type="number" min="0" class="qdef-input" bind:value={d.maxMbPerMinute} on:input={() => { qualityDefsDirty = new Set([...qualityDefsDirty, d.id]); }} /></td>
                    <td><button class="qdef-save-btn" disabled={!qualityDefsDirty.has(d.id) || qualityDefsSaving.has(d.id)} on:click={() => saveQualityDef(d)} type="button">{qualityDefsSaving.has(d.id) ? '…' : 'Save'}</button></td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </Panel>
          <Panel title="TV / Episode Quality Definitions" subtitle="Per-tier size limits (MB/min) applied when ranking TV episode releases. Set 0 for no limit.">
            <table class="qdef-table">
              <thead><tr><th>Quality</th><th>Min (MB/min)</th><th>Max (MB/min)</th><th></th></tr></thead>
              <tbody>
                {#each episodeDefs as d (d.id)}
                  <tr>
                    <td class="qdef-title">{d.title}</td>
                    <td><input type="number" min="0" class="qdef-input" bind:value={d.minMbPerMinute} on:input={() => { qualityDefsDirty = new Set([...qualityDefsDirty, d.id]); }} /></td>
                    <td><input type="number" min="0" class="qdef-input" bind:value={d.maxMbPerMinute} on:input={() => { qualityDefsDirty = new Set([...qualityDefsDirty, d.id]); }} /></td>
                    <td><button class="qdef-save-btn" disabled={!qualityDefsDirty.has(d.id) || qualityDefsSaving.has(d.id)} on:click={() => saveQualityDef(d)} type="button">{qualityDefsSaving.has(d.id) ? '…' : 'Save'}</button></td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </Panel>
        </div>
      {:else}
      <div class="qp-shell">
        <aside class="qp-list">
          {#each profiles as p (p.id ?? p.name)}
            <button class="qp-item" class:selected={selectedProfile?.id === p.id} on:click={() => { selectedProfile = { ...p }; }} type="button">
              <div class="qp-item-name">
                {#if p.isDefault}<Star size={12} class="qp-star" />{/if}
                {p.name}
              </div>
              <div class="qp-item-meta">{p.resolutions.slice(0,2).join(', ')}</div>
            </button>
          {/each}
          {#if profiles.length === 0 && !loading}<div class="empty">No profiles yet.</div>{/if}
          <Button kind="secondary" on:click={() => { selectedProfile = blankProfile(); }}>
            <Plus size={14} /> New Profile
          </Button>
        </aside>

        {#if selectedProfile}
          <div class="qp-editor">
            <Panel title={selectedProfile.id ? `Edit: ${selectedProfile.name}` : 'New Profile'} subtitle="Settings control how releases are ranked and filtered.">
              <div slot="actions">
                {#if selectedProfile.isDefault}<StatusPill tone="ok">Default</StatusPill>{/if}
              </div>

              <div class="field">
                <label class="field-label" for="pname">Profile Name</label>
                <input id="pname" class="field-input" bind:value={selectedProfile.name} placeholder="e.g. Movie HD" />
              </div>
              <div class="divider"></div>

              <!-- Resolutions ordered -->
              <div class="field">
                <div class="field-label">Resolutions <span class="field-hint">rank by priority</span></div>
                <div class="ordered-list">
                  {#each selectedProfile.resolutions as res, i}
                    <div class="ordered-row">
                      <span class="rank">{i+1}</span>
                      <span class="ordered-value">{res}</span>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, resolutions: profileMoveUp(selectedProfile!.resolutions, i) }; }} disabled={i===0}><ChevronUp size={13}/></button>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, resolutions: profileMoveDown(selectedProfile!.resolutions, i) }; }} disabled={i===selectedProfile.resolutions.length-1}><ChevronDown size={13}/></button>
                      <button type="button" class="rank-btn remove" on:click={() => { selectedProfile = { ...selectedProfile!, resolutions: selectedProfile!.resolutions.filter(v=>v!==res) }; }}>✕</button>
                    </div>
                  {/each}
                  <div class="chip-row">
                    {#each ALL_RESOLUTIONS.filter(r => !selectedProfile!.resolutions.includes(r)) as r}
                      <button type="button" class="chip add" on:click={() => { selectedProfile = { ...selectedProfile!, resolutions: [...selectedProfile!.resolutions, r] }; }}>{r} +</button>
                    {/each}
                  </div>
                </div>
              </div>

              <!-- Sources ordered -->
              <div class="field">
                <div class="field-label">Sources <span class="field-hint">rank by priority</span></div>
                <div class="ordered-list">
                  {#each selectedProfile.sources as src, i}
                    <div class="ordered-row">
                      <span class="rank">{i+1}</span><span class="ordered-value">{src}</span>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, sources: profileMoveUp(selectedProfile!.sources, i) }; }} disabled={i===0}><ChevronUp size={13}/></button>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, sources: profileMoveDown(selectedProfile!.sources, i) }; }} disabled={i===selectedProfile.sources.length-1}><ChevronDown size={13}/></button>
                      <button type="button" class="rank-btn remove" on:click={() => { selectedProfile = { ...selectedProfile!, sources: selectedProfile!.sources.filter(v=>v!==src) }; }}>✕</button>
                    </div>
                  {/each}
                  <div class="chip-row">
                    {#each ALL_SOURCES.filter(s => !selectedProfile!.sources.includes(s)) as s}
                      <button type="button" class="chip add" on:click={() => { selectedProfile = { ...selectedProfile!, sources: [...selectedProfile!.sources, s] }; }}>{s} +</button>
                    {/each}
                  </div>
                </div>
              </div>

              <!-- Codecs ordered -->
              <div class="field">
                <div class="field-label">Codecs <span class="field-hint">rank by priority</span></div>
                <div class="ordered-list">
                  {#each selectedProfile.codecs as c, i}
                    <div class="ordered-row">
                      <span class="rank">{i+1}</span><span class="ordered-value">{c}</span>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, codecs: profileMoveUp(selectedProfile!.codecs, i) }; }} disabled={i===0}><ChevronUp size={13}/></button>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, codecs: profileMoveDown(selectedProfile!.codecs, i) }; }} disabled={i===selectedProfile.codecs.length-1}><ChevronDown size={13}/></button>
                      <button type="button" class="rank-btn remove" on:click={() => { selectedProfile = { ...selectedProfile!, codecs: selectedProfile!.codecs.filter(v=>v!==c) }; }}>✕</button>
                    </div>
                  {/each}
                  <div class="chip-row">
                    {#each ALL_CODECS.filter(c => !selectedProfile!.codecs.includes(c)) as c}
                      <button type="button" class="chip add" on:click={() => { selectedProfile = { ...selectedProfile!, codecs: [...selectedProfile!.codecs, c] }; }}>{c} +</button>
                    {/each}
                  </div>
                </div>
              </div>

              <div class="divider"></div>

              <!-- Audio ordered -->
              <div class="field">
                <div class="field-label">Audio Formats <span class="field-hint">rank by priority</span></div>
                <div class="ordered-list">
                  {#each selectedProfile.audioFormats as a, i}
                    <div class="ordered-row">
                      <span class="rank">{i+1}</span><span class="ordered-value">{a}</span>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, audioFormats: profileMoveUp(selectedProfile!.audioFormats, i) }; }} disabled={i===0}><ChevronUp size={13}/></button>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, audioFormats: profileMoveDown(selectedProfile!.audioFormats, i) }; }} disabled={i===selectedProfile.audioFormats.length-1}><ChevronDown size={13}/></button>
                      <button type="button" class="rank-btn remove" on:click={() => { selectedProfile = { ...selectedProfile!, audioFormats: selectedProfile!.audioFormats.filter(v=>v!==a) }; }}>✕</button>
                    </div>
                  {/each}
                  <div class="chip-row">
                    {#each ALL_AUDIO.filter(a => !selectedProfile!.audioFormats.includes(a)) as a}
                      <button type="button" class="chip add" on:click={() => { selectedProfile = { ...selectedProfile!, audioFormats: [...selectedProfile!.audioFormats, a] }; }}>{a} +</button>
                    {/each}
                  </div>
                </div>
              </div>

              <!-- HDR ordered -->
              <div class="field">
                <div class="field-label">HDR Formats <span class="field-hint">rank by priority</span></div>
                <div class="ordered-list">
                  {#each selectedProfile.hdrFormats as h, i}
                    <div class="ordered-row">
                      <span class="rank">{i+1}</span><span class="ordered-value">{h}</span>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, hdrFormats: profileMoveUp(selectedProfile!.hdrFormats, i) }; }} disabled={i===0}><ChevronUp size={13}/></button>
                      <button type="button" class="rank-btn" on:click={() => { selectedProfile = { ...selectedProfile!, hdrFormats: profileMoveDown(selectedProfile!.hdrFormats, i) }; }} disabled={i===selectedProfile.hdrFormats.length-1}><ChevronDown size={13}/></button>
                      <button type="button" class="rank-btn remove" on:click={() => { selectedProfile = { ...selectedProfile!, hdrFormats: selectedProfile!.hdrFormats.filter(v=>v!==h) }; }}>✕</button>
                    </div>
                  {/each}
                  <div class="chip-row">
                    {#each ALL_HDR.filter(h => !selectedProfile!.hdrFormats.includes(h)) as h}
                      <button type="button" class="chip add" on:click={() => { selectedProfile = { ...selectedProfile!, hdrFormats: [...selectedProfile!.hdrFormats, h] }; }}>{h} +</button>
                    {/each}
                  </div>
                </div>
              </div>

              <div class="divider"></div>

              <!-- Languages chips -->
              <div class="field">
                <div class="field-label">Languages</div>
                <div class="chip-row">
                  {#each ALL_LANGUAGES as lang}
                    <button type="button" class="chip" class:on={selectedProfile.languages.includes(lang)} on:click={() => { selectedProfile = { ...selectedProfile!, languages: profileToggle(selectedProfile!.languages, lang) }; }}>{lang}</button>
                  {/each}
                </div>
              </div>

              <div class="divider"></div>

              <!-- Flags -->
              <div class="field">
                <div class="field-label">Release Flags</div>
                <div class="flags-grid">
                  <label class="flag-row">
                    <input type="checkbox" bind:checked={selectedProfile.preferProper} />
                    <div><strong>Prefer Proper</strong><span>Boost score when release is marked PROPER</span></div>
                  </label>
                  <label class="flag-row">
                    <input type="checkbox" bind:checked={selectedProfile.preferRepack} />
                    <div><strong>Prefer Repack</strong><span>Boost score when release is marked REPACK</span></div>
                  </label>
                  <label class="flag-row">
                    <input type="checkbox" bind:checked={selectedProfile.rejectCam} />
                    <div><strong>Reject CAM / TS / Telecine</strong><span>Hard-reject low-quality cam captures and telesyncs</span></div>
                  </label>
                  <label class="flag-row">
                    <input type="checkbox" bind:checked={selectedProfile.allowUpgrade} />
                    <div><strong>Allow Quality Upgrade</strong><span>Periodically re-search available items for a higher-quality release</span></div>
                  </label>
                </div>
              </div>

              <div class="divider"></div>

              <!-- Cutoff + minimum age -->
              <div class="field">
                <div class="field-label">Upgrade Cutoff</div>
                <div class="size-row">
                  <label>
                    <span>Cutoff Resolution</span>
                    <select bind:value={selectedProfile.cutoffResolution} class="size-input">
                      <option value="">No cutoff</option>
                      {#each ALL_RESOLUTIONS as r}
                        <option value={r}>{r}</option>
                      {/each}
                    </select>
                  </label>
                  <label>
                    <span>Minimum Age (hours)</span>
                    <input type="number" min="0" bind:value={selectedProfile.minimumAgeHours} class="size-input" placeholder="0 = no delay" />
                  </label>
                </div>
                <p class="field-hint" style="margin-top:4px">Stop upgrading once resolution reaches cutoff. Minimum age rejects releases posted within N hours.</p>
              </div>

              <div class="divider"></div>

              <div class="field">
                <div class="field-label">Upgrade Threshold</div>
                <div class="size-row">
                  <label>
                    <span>Minimum CF Upgrade</span>
                    <input type="number" min="0" bind:value={selectedProfile.minimumUpgradeCustomFormatScore} class="size-input" placeholder="0 = no minimum" />
                  </label>
                </div>
                <p class="field-hint" style="margin-top:4px">When upgrades are enabled, the candidate must improve the custom-format subtotal by at least this amount over the current release.</p>
              </div>

              <div class="divider"></div>

              <!-- Size limits -->
              <div class="field">
                <div class="field-label">Size Limits</div>
                <div class="size-row">
                  <label><span>Min (MB/min)</span><input type="number" min="0" bind:value={selectedProfile.minMbPerMinute} class="size-input" placeholder="0 = no limit" /></label>
                  <label><span>Max (MB/min)</span><input type="number" min="0" bind:value={selectedProfile.maxMbPerMinute} class="size-input" placeholder="0 = no limit" /></label>
                </div>
                <p class="field-hint" style="margin-top:4px">Applied per runtime minute. If runtime metadata is missing, size limits are skipped instead of hard-rejecting the release.</p>
              </div>

              <div class="divider"></div>

              <!-- Exclude patterns -->
              <div class="field">
                <div class="field-label">Exclude Patterns <span class="field-hint">(regex, one per line — titles matching any pattern are rejected)</span></div>
                <textarea
                  class="exclude-patterns-input"
                  rows="4"
                  placeholder="e.g. \.FRENCH\.\n\.GERMAN\.\nHardcoded"
                  value={(selectedProfile.excludePatterns ?? []).join('\n')}
                  on:input={(e) => { selectedProfile = { ...selectedProfile!, excludePatterns: (e.currentTarget as HTMLTextAreaElement).value.split('\n').map(s => s.trim()).filter(Boolean) }; }}
                ></textarea>
              </div>

              <div class="divider"></div>

              <div class="editor-actions">
                {#if selectedProfile.id && !selectedProfile.isDefault}
                  <Button kind="danger" on:click={() => selectedProfile && deleteSelectedProfile(selectedProfile)} disabled={profileSaving}>
                    <Trash2 size={15} /> Delete
                  </Button>
                {/if}
                <Button kind="primary" on:click={saveSelectedProfile} disabled={profileSaving}>
                  <Save size={15} /> {profileSaving ? 'Saving…' : 'Save Profile'}
                </Button>
              </div>
            </Panel>
          </div>
        {:else}
          <div class="qp-no-selection">Select a profile to edit, or create a new one.</div>
        {/if}
      </div>
      {/if}

    <!-- CUSTOM FORMATS -->
    {:else if activeTab === 'formats'}
      <Panel title="Custom Formats" subtitle="User-defined scoring rules applied to release titles. Positive scores boost, negative scores penalise.">
        {#if cfImportOpen}
          <div class="cf-import-box">
            <div class="cf-import-header">
              <strong>Import Custom Formats</strong>
              <span class="field-hint">Paste a JSON array of custom format objects. Fields: name, pattern, score, enabled.</span>
            </div>
            <textarea class="cf-import-textarea" bind:value={cfImportJson} rows={8} placeholder={`[{"name":"BluRay","pattern":"(?i)bluray","score":50,"enabled":true}]`}></textarea>
            <div class="editor-actions" style="margin-top:10px">
              <Button kind="ghost" on:click={() => { cfImportOpen = false; cfImportJson = ''; }}>Cancel</Button>
              <Button kind="primary" on:click={importCustomFormats} disabled={cfImporting || !cfImportJson.trim()}>
                {cfImporting ? 'Importing…' : 'Import'}
              </Button>
            </div>
          </div>
        {/if}
        <div class="cf-layout">
          <div class="cf-list">
            <div class="cf-list-header">
              <span>Formats</span>
              <div style="display:flex;gap:6px">
                <Button kind="ghost" on:click={() => { cfImportOpen = !cfImportOpen; cfImportJson = ''; }}>
                  Import
                </Button>
                <Button kind="ghost" on:click={() => { editingFormat = blankFormat(); }}>
                  <Plus size={14} /> New
                </Button>
              </div>
            </div>
            {#if customFormats.length === 0}
              <div class="cf-empty">No custom formats yet.</div>
            {/if}
            {#each customFormats as f (f.id)}
              <button class="cf-item" class:cf-active={editingFormat?.id === f.id} on:click={() => { editingFormat = { ...f }; }}>
                <span class="cf-item-name">{f.name}</span>
                <div style="display:flex;align-items:center;gap:4px;flex-shrink:0">
                  {#if f.source && f.source !== 'custom'}<span class="rf-badge rf-badge-src">{f.source}</span>{/if}
                  <span class="cf-item-score" class:cf-pos={f.score > 0} class:cf-neg={f.score < 0}>{f.score > 0 ? '+' : ''}{f.score}</span>
                  {#if !f.enabled}<span class="cf-disabled-badge">off</span>{/if}
                </div>
              </button>
            {/each}
          </div>
          <div class="cf-editor">
            {#if editingFormat}
              <div class="field">
                <label class="field-label" for="cf-name">Name</label>
                <input id="cf-name" type="text" bind:value={editingFormat.name} placeholder="e.g. BluRay Boost" />
              </div>
              <div class="field">
                <label class="field-label" for="cf-pattern">Pattern <span class="field-hint">(regex matched against release title)</span></label>
                <input id="cf-pattern" type="text" bind:value={editingFormat.pattern} placeholder="(?i)bluray" />
              </div>
              <div class="field">
                <label class="field-label" for="cf-score">Score</label>
                <input id="cf-score" type="number" bind:value={editingFormat.score} placeholder="e.g. 50 or -100" />
              </div>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingFormat.enabled} />
                <div><strong>Enabled</strong><span>Apply this format when scoring releases</span></div>
              </label>
              <div class="editor-actions" style="margin-top:16px">
                {#if editingFormat.id}
                  <Button kind="danger" on:click={() => editingFormat?.id && deleteFormat(editingFormat.id)}>
                    <Trash2 size={15} /> Delete
                  </Button>
                {/if}
                <Button kind="ghost" on:click={() => { editingFormat = null; }}>Cancel</Button>
                <Button kind="primary" on:click={saveFormat} disabled={cfSaving}>
                  <Save size={15} /> {cfSaving ? 'Saving…' : 'Save'}
                </Button>
              </div>
            {:else}
              <div class="cf-empty">Select a format to edit, or create a new one.</div>
            {/if}
          </div>
        </div>
      </Panel>

    <!-- RELEASE FILTERING -->
    {:else if activeTab === 'filtering'}
      <Panel title="Release Filtering" subtitle="Block or penalise known low-quality releases by group, title pattern, or regex. Default rules are from TRaSH Guides LQ lists.">
        <div class="rf-layout">
          <!-- Rule list -->
          <div class="rf-list">
            <div class="rf-list-header">
              <span>Rules ({blockRules.filter(r => r.enabled).length}/{blockRules.length} enabled)</span>
              <Button kind="ghost" on:click={() => { editingRule = blankRule(); }}>
                <Plus size={14} /> Add
              </Button>
            </div>

            {#each [['release_group','Release Groups'], ['title_pattern','Title Patterns'], ['regex','Regex'], ['missing_release_group','Missing Group']] as [typeKey, typeLabel] (typeKey)}
              {@const group = blockRules.filter(r => r.type === typeKey)}
              {#if group.length > 0}
                <div class="rf-type-header">{typeLabel} <span class="rf-count">{group.filter(r => r.enabled).length}/{group.length}</span></div>
                {#each group as rule (rule.id)}
                  <button class="rf-item" class:rf-active={editingRule?.id === rule.id} class:rf-disabled={!rule.enabled}
                    on:click={() => { editingRule = { ...rule }; testResult = null; }}>
                    <span class="rf-pattern">{rule.pattern || '(any)'}</span>
                    <span class="rf-badges">
                      {#if rule.mediaType !== 'both'}<span class="rf-badge rf-badge-mt">{rule.mediaType}</span>{/if}
                      <span class="rf-badge" class:rf-badge-block={rule.action === 'block'} class:rf-badge-penalty={rule.action === 'penalty'}>
                        {rule.action === 'block' ? 'block' : `-${rule.scorePenalty}`}
                      </span>
                      {#if rule.source !== 'custom'}<span class="rf-badge rf-badge-src">{rule.source}</span>{/if}
                      {#if !rule.enabled}<span class="rf-badge rf-badge-off">off</span>{/if}
                    </span>
                  </button>
                {/each}
              {/if}
            {/each}

            {#if blockRules.length === 0}
              <div class="cf-empty">No rules yet.</div>
            {/if}
          </div>

          <!-- Editor + test tool -->
          <div class="rf-editor">
            {#if editingRule}
              <div class="field">
                <label class="field-label" for="rf-type">Type</label>
                <select id="rf-type" bind:value={editingRule.type} disabled={editingRule.id !== undefined && editingRule.source !== 'custom'}>
                  <option value="release_group">Release Group</option>
                  <option value="title_pattern">Title Pattern</option>
                  <option value="regex">Regex</option>
                  <option value="missing_release_group">Missing Release Group</option>
                </select>
              </div>
              {#if editingRule.type !== 'missing_release_group'}
                <div class="field">
                  <label class="field-label" for="rf-pattern">Pattern
                    {#if editingRule.type === 'regex'}<span class="field-hint">(regex, case-insensitive)</span>{/if}
                    {#if editingRule.type === 'title_pattern'}<span class="field-hint">(substring match, dots normalised)</span>{/if}
                    {#if editingRule.type === 'release_group'}<span class="field-hint">(parsed group after last "-")</span>{/if}
                  </label>
                  <input id="rf-pattern" type="text" bind:value={editingRule.pattern}
                    placeholder={editingRule.type === 'release_group' ? 'e.g. GalaxyRG' : editingRule.type === 'title_pattern' ? 'e.g. AI Upscale' : '(?i)upscal(e|ed)'}
                    disabled={editingRule.id !== undefined && editingRule.source !== 'custom'} />
                </div>
              {/if}
              <div class="field">
                <label class="field-label" for="rf-mediatype">Media type</label>
                <select id="rf-mediatype" bind:value={editingRule.mediaType} disabled={editingRule.id !== undefined && editingRule.source !== 'custom'}>
                  <option value="both">Both</option>
                  <option value="movie">Movie only</option>
                  <option value="tv">TV only</option>
                </select>
              </div>
              <div class="field">
                <label class="field-label" for="rf-action">Action</label>
                <select id="rf-action" bind:value={editingRule.action} disabled={editingRule.id !== undefined && editingRule.source !== 'custom'}>
                  <option value="block">Block (reject release)</option>
                  <option value="penalty">Penalty (reduce score)</option>
                </select>
              </div>
              {#if editingRule.action === 'penalty'}
                <div class="field">
                  <label class="field-label" for="rf-penalty">Score penalty <span class="field-hint">(positive = points subtracted)</span></label>
                  <input id="rf-penalty" type="number" bind:value={editingRule.scorePenalty} min="0"
                    disabled={editingRule.id !== undefined && editingRule.source !== 'custom'} />
                </div>
              {/if}
              <div class="field">
                <label class="field-label" for="rf-note">Note <span class="field-hint">(optional)</span></label>
                <input id="rf-note" type="text" bind:value={editingRule.note} placeholder="Why this rule exists" />
              </div>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingRule.enabled} />
                <div><strong>Enabled</strong><span>Apply this rule when scoring releases</span></div>
              </label>
              <div class="editor-actions" style="margin-top:16px">
                {#if editingRule.id && editingRule.source === 'custom'}
                  <Button kind="danger" on:click={() => editingRule?.id && deleteBlockRule(editingRule.id)}>
                    <Trash2 size={15} /> Delete
                  </Button>
                {/if}
                <Button kind="ghost" on:click={() => { editingRule = null; testResult = null; }}>Cancel</Button>
                <Button kind="primary" on:click={saveBlockRule} disabled={bfSaving}>
                  <Save size={15} /> {bfSaving ? 'Saving…' : 'Save'}
                </Button>
              </div>
              {#if editingRule.source !== 'custom'}
                <p class="rf-readonly-note">Default and TRaSH rules: only <strong>enabled</strong> and <strong>note</strong> can be changed. To customise, add a new custom rule.</p>
              {/if}
            {:else}
              <div class="cf-empty">Select a rule to edit, or add a new custom rule.</div>
            {/if}

            <!-- Test tool -->
            <div class="rf-test-panel">
              <div class="rf-test-header">Test a release title</div>
              <div class="rf-test-row">
                <input type="text" bind:value={testTitle} placeholder="Movie.Title.2025.1080p.WEB-DL-GalaxyRG"
                  style="flex:1" on:keydown={(e) => e.key === 'Enter' && runBlockTest()} />
                <select bind:value={testMediaType} style="width:100px">
                  <option value="both">Both</option>
                  <option value="movie">Movie</option>
                  <option value="tv">TV</option>
                </select>
                <Button kind="secondary" on:click={runBlockTest} disabled={testRunning || !testTitle.trim()}>
                  {testRunning ? '…' : 'Test'}
                </Button>
              </div>
              {#if testResult}
                <div class="rf-test-result" class:rf-test-blocked={testResult.blocked} class:rf-test-allowed={testResult.allowed && testResult.scorePenalty === 0}>
                  <strong>{testResult.blocked ? '🚫 Blocked' : testResult.scorePenalty > 0 ? `⚠ Penalty −${testResult.scorePenalty}` : '✓ Allowed'}</strong>
                  {#if testResult.matchedRules.length > 0}
                    <ul class="rf-test-matches">
                      {#each testResult.matchedRules as m}
                        <li><span class="rf-badge rf-badge-src">{m.type}</span> {m.reason}</li>
                      {/each}
                    </ul>
                  {/if}
                </div>
              {/if}
            </div>
          </div>
        </div>
      </Panel>

    <!-- SUBTITLE PROFILES -->
    {:else if activeTab === 'subtitle-profiles'}
      <Panel title="Subtitle Profiles" subtitle="Named language preference sets for subtitle acquisition. Assign a profile per library item to override the global language settings.">
        <div class="cf-layout">
          <div class="cf-list">
            <div class="cf-list-header">
              <span>Profiles</span>
              <Button kind="ghost" on:click={() => { editingSubtitleProfile = { name: '', languages: [], preferHearingImpaired: false, requireExactLanguage: false, isDefault: false }; }}>
                <Plus size={14} /> New
              </Button>
            </div>
            {#if subtitleProfiles.length === 0}
              <div class="cf-empty">No subtitle profiles yet.</div>
            {/if}
            {#each subtitleProfiles as p (p.id)}
              <button class="cf-item" class:cf-active={editingSubtitleProfile?.id === p.id} on:click={() => { editingSubtitleProfile = { ...p }; }}>
                <span class="cf-item-name">{p.name}</span>
                <div style="display:flex;align-items:center;gap:4px;flex-shrink:0">
                  {#if p.isDefault}<span class="rf-badge rf-badge-src">default</span>{/if}
                  {#if p.languages.length > 0}<span class="cf-disabled-badge">{p.languages.slice(0,2).join(', ')}{p.languages.length > 2 ? '…' : ''}</span>{/if}
                </div>
              </button>
            {/each}
          </div>
          <div class="cf-editor">
            {#if editingSubtitleProfile}
              <div class="field">
                <label class="field-label" for="sp-name">Profile Name</label>
                <input id="sp-name" type="text" bind:value={editingSubtitleProfile.name} placeholder="e.g. Dutch Preferred" />
              </div>
              <div class="field">
                <label class="field-label" for="sp-languages">Languages <span class="field-hint">(comma-separated ISO codes, e.g. nl, en)</span></label>
                <input id="sp-languages" type="text"
                  value={editingSubtitleProfile.languages.join(', ')}
                  on:input={(e) => { editingSubtitleProfile = { ...editingSubtitleProfile!, languages: (e.currentTarget as HTMLInputElement).value.split(',').map(l => l.trim()).filter(Boolean) }; }}
                  placeholder="nl, en" />
              </div>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingSubtitleProfile.preferHearingImpaired} />
                <div><strong>Prefer Hearing Impaired</strong><span>Boost scores for SDH/HI subtitles</span></div>
              </label>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingSubtitleProfile.requireExactLanguage} />
                <div><strong>Require Exact Language</strong><span>Skip subtitles in a different language</span></div>
              </label>
              <label class="flag-row">
                <input type="checkbox" bind:checked={editingSubtitleProfile.isDefault} />
                <div><strong>Set as Default</strong><span>Use this profile when no per-item profile is assigned</span></div>
              </label>
              <div class="editor-actions" style="margin-top:16px">
                {#if editingSubtitleProfile.id}
                  <Button kind="danger" on:click={() => editingSubtitleProfile?.id && deleteSubtitleProfile(editingSubtitleProfile.id)}>
                    <Trash2 size={15} /> Delete
                  </Button>
                {/if}
                <Button kind="ghost" on:click={() => { editingSubtitleProfile = null; }}>Cancel</Button>
                <Button kind="primary" on:click={saveSubtitleProfile} disabled={spSaving}>
                  <Save size={15} /> {spSaving ? 'Saving…' : 'Save'}
                </Button>
              </div>
            {:else}
              <div class="cf-empty">Select a profile to edit, or create a new one.</div>
            {/if}
          </div>
        </div>
      </Panel>

    <!-- NOTIFICATIONS -->
    {:else if activeTab === 'notifications'}
      {#if draft}
      <Panel title="Notifications" subtitle="Send event notifications to Discord or a generic webhook.">
        <div class="field">
          <label class="field-label" for="notif-discord">Discord Webhook URL</label>
          <input id="notif-discord" type="url" bind:value={draft.notifications.discordWebhookUrl} placeholder="https://discord.com/api/webhooks/…" />
          <p class="field-hint">Paste a Discord channel webhook URL. Drakkar will send an embed when triggered events fire.</p>
        </div>
        <div class="divider"></div>
        <div class="field">
          <label class="field-label" for="notif-webhook">Generic Webhook URL</label>
          <input id="notif-webhook" type="url" bind:value={draft.notifications.genericWebhookUrl} placeholder="https://your-server.com/hook" />
          <p class="field-hint">Receives a JSON POST body with <code>eventType</code>, <code>title</code>, <code>resolution</code>, and other fields.</p>
        </div>
        <div class="divider"></div>
        <div class="field">
          <div class="field-label">Trigger Events</div>
          <div class="flags-grid">
            <label class="flag-row">
              <input type="checkbox" bind:checked={draft.notifications.onGrab} />
              <div><strong>On Grab</strong><span>Fire when a release is selected for download</span></div>
            </label>
            <label class="flag-row">
              <input type="checkbox" bind:checked={draft.notifications.onAvailable} />
              <div><strong>On Available</strong><span>Fire when an item finishes importing</span></div>
            </label>
            <label class="flag-row">
              <input type="checkbox" bind:checked={draft.notifications.onFailed} />
              <div><strong>On Failed</strong><span>Fire when an item permanently fails</span></div>
            </label>
          </div>
        </div>
        <div class="divider"></div>
        <div class="editor-actions">
          <Button kind="primary" on:click={saveSettings} disabled={working}>
            <Save size={15} /> {working ? 'Saving…' : 'Save Notifications'}
          </Button>
        </div>
      </Panel>
      {/if}

    <!-- LOGS -->
    {:else if activeTab === 'logs'}
      <div class="log-toolbar">
        <div class="log-search-wrap">
          <Search size={14} />
          <input bind:value={logTerm} placeholder="Search logs, service names, request IDs…" class="log-search-input" />
        </div>
        <select bind:value={logLevelFilter} on:change={() => void loadLogs()} class="log-level-select">
          <option value="all">All levels</option>
          <option value="info">Info</option>
          <option value="warn">Warn</option>
          <option value="error">Error</option>
          <option value="debug">Debug</option>
        </select>
        <Button kind="secondary" on:click={loadLogs} disabled={logLoading}>
          <RefreshCw size={14} /> Refresh
        </Button>
        <a class="log-download-link" href="/api/logs?limit=2000" target="_blank" rel="noreferrer" download>
          <Button kind="secondary">Download</Button>
        </a>
      </div>
      {#if logError}<div class="log-error">Error: {logError}</div>{/if}
      <div class="log-table-wrap">
        <table>
          <thead>
            <tr>
              <th class="log-col-time">Time</th>
              <th class="log-col-level">Level</th>
              <th class="log-col-service">Service</th>
              <th class="log-col-message">Message</th>
            </tr>
          </thead>
          <tbody>
            {#if logLoading && logEntries.length === 0}
              <tr><td colspan="4" class="log-empty">Loading…</td></tr>
            {:else if filteredLogs.length === 0}
              <tr><td colspan="4" class="log-empty">No log entries match the current filter.</td></tr>
            {:else}
              {#each filteredLogs as entry, i (i)}
                <tr class="log-row-{entry.level === 'error' ? 'error' : entry.level === 'warn' ? 'warn' : 'default'}">
                  <td class="log-col-time mono muted">{fmtLogDate(entry.time)}</td>
                  <td class="log-col-level">
                    <span class="log-badge log-badge-{entry.level || 'default'}">{(entry.level || '?').toUpperCase()}</span>
                  </td>
                  <td class="log-col-service mono muted">{entry.service || '—'}</td>
                  <td class="log-col-message">{entry.message}</td>
                </tr>
              {/each}
            {/if}
          </tbody>
        </table>
      </div>

    <!-- TASKS -->
    {:else if activeTab === 'tasks'}
      <div class="task-summary-grid">
        <div class="task-summary-card"><div class="task-summary-value">{taskDefs.filter(t => t.group === 'Indexing').length}</div><div class="task-summary-label">Indexing tasks</div></div>
        <div class="task-summary-card"><div class="task-summary-value">{taskDefs.filter(t => t.group === 'Publishing').length}</div><div class="task-summary-label">Publishing tasks</div></div>
        <div class="task-summary-card"><div class="task-summary-value">{taskDefs.filter(t => t.group === 'Maintenance').length}</div><div class="task-summary-label">Maintenance tasks</div></div>
        <div class="task-summary-card"><div class="task-summary-value">{taskRunningCount}</div><div class="task-summary-label">Running now</div></div>
      </div>
      <Panel title="Scheduled Tasks" subtitle="Scheduled-job control plane for indexing, publishing, and maintenance work.">
        <div class="task-table-wrap">
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
              {#each taskGroups as group}
                <tr class="task-group-row"><td colspan="5">{group}</td></tr>
                {#each taskDefs.filter(t => t.group === group) as task}
                  {@const busy = taskRunning[task.id]}
                  {@const result = taskResults[task.id]}
                  {@const schedule = taskScheduleFor(task)}
                  <tr>
                    <td>
                      <div class="task-row-title">{task.label}</div>
                      <div class="task-row-sub">{task.description}</div>
                      {#if result}
                        <div class="task-result {result.ok ? 'ok' : 'fail'}">
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
                        <span class="time-cell"><Clock3 size={12} /> {fmtTaskTime(result.ranAt)}</span>
                      {:else if schedule?.lastRunAt}
                        <span class="time-cell"><Clock3 size={12} /> {fmtTaskTime(schedule.lastRunAt)}</span>
                      {:else if taskSchedulesLoading}
                        <span class="time-cell dim">—</span>
                      {:else}
                        <span class="time-cell dim">Never</span>
                      {/if}
                    </td>
                    <td>
                      <Button kind="secondary" on:click={() => runTask(task)} disabled={busy || !task.manual}>
                        {#if busy}<RefreshCw size={14} class="spin" /> Running…{:else}<Play size={14} /> Run{/if}
                      </Button>
                    </td>
                  </tr>
                {/each}
              {/each}
            </tbody>
          </table>
        </div>
      </Panel>

    <!-- PLEX -->
    {:else if activeTab === 'media-players'}
      {#if draft}
        <Panel title="Plex Media Server" subtitle="Drakkar triggers a library scan automatically after publishing new media.">
          <div class="form-grid">
            <label class="form-field">
              <span>Server URL</span>
              <input type="url" bind:value={draft.plex.url} placeholder="http://your-plex-server:32400" />
            </label>
            <label class="form-field">
              <span>X-Plex-Token</span>
              <div class="plex-token-row">
                <input type="password" bind:value={draft.plex.token} placeholder="••••••••" autocomplete="off" />
                {#if !plexPin}
                  <Button kind="secondary" on:click={startPlexOAuth} disabled={working}>
                    <ExternalLink size={14} /> Get token with Plex
                  </Button>
                {:else}
                  <div class="plex-oauth-status">
                    <a href={plexPin.authUrl} target="_blank" rel="noopener noreferrer" class="plex-open-link">
                      Open PIN {plexPin.code}
                    </a>
                    <span class="plex-oauth-hint">Waiting for approval…</span>
                    <button type="button" class="plex-cancel-btn" on:click={cancelPlexOAuth}>Cancel</button>
                  </div>
                {/if}
              </div>
            </label>
            <label class="form-field">
              <span>Section Key <small class="field-hint-inline">(leave empty to refresh all libraries)</small></span>
              <input type="text" bind:value={draft.plex.sectionKey} placeholder="1" />
            </label>
          </div>
          <div class="actions-row" style="margin-top:16px; gap:10px; justify-content:space-between">
            <div style="display:flex;gap:10px">
              <Button kind="secondary" on:click={async () => {
                working = true;
                try {
                  const r = await api.plexTest();
                  if (r.ok) toastSuccess(`Plex connected: ${r.serverName} (${r.libraries?.length ?? 0} libraries)`);
                  else toastError(r.error ?? 'Plex connection failed');
                } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
                finally { working = false; }
              }} disabled={working}>
                <Wrench size={16} /> Test Connection
              </Button>
              <Button kind="secondary" on:click={async () => {
                working = true;
                try { await api.plexRefresh(); toastSuccess('Plex library scan triggered'); }
                catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
                finally { working = false; }
              }} disabled={working}>
                <RefreshCw size={16} /> Refresh Libraries
              </Button>
            </div>
            <Button kind="primary" on:click={saveSettings} disabled={working}>
              <Save size={16} /> Save Plex Settings
            </Button>
          </div>
        </Panel>

        <Panel title="Jellyfin" subtitle="Drakkar triggers a library scan automatically after publishing new media.">
          <div class="form-grid">
            <label class="form-field">
              <span>Server URL</span>
              <input type="url" bind:value={draft.jellyfin.url} placeholder="http://your-jellyfin-server:8096" />
            </label>
            <label class="form-field">
              <span>API Key</span>
              <input type="password" bind:value={draft.jellyfin.apiKey} placeholder="••••••••" autocomplete="off" />
            </label>
          </div>
          <div class="actions-row" style="margin-top:16px; gap:10px; justify-content:space-between">
            <div style="display:flex;gap:10px">
              <Button kind="secondary" on:click={async () => {
                working = true;
                try {
                  const r = await api.jellyfinTest();
                  if (r.ok) toastSuccess(`Jellyfin connected: ${r.serverName} v${r.version}`);
                  else toastError(r.error ?? 'Jellyfin connection failed');
                } catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
                finally { working = false; }
              }} disabled={working}>
                <Wrench size={16} /> Test Connection
              </Button>
              <Button kind="secondary" on:click={async () => {
                working = true;
                try { await api.jellyfinRefresh(); toastSuccess('Jellyfin library scan triggered'); }
                catch (e) { toastError(e instanceof Error ? e.message : String(e)); }
                finally { working = false; }
              }} disabled={working}>
                <RefreshCw size={16} /> Refresh Libraries
              </Button>
            </div>
            <Button kind="primary" on:click={saveSettings} disabled={working}>
              <Save size={16} /> Save Jellyfin Settings
            </Button>
          </div>
        </Panel>
      {:else}
        <div class="empty">Loading settings…</div>
      {/if}

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

        <Panel title="Integration Status" subtitle="Config-derived readiness for external services.">
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
    gap: 2px;
    position: sticky;
    top: 88px;
  }

  .tab-btn {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 9px 12px;
    border-radius: 8px;
    border: none;
    background: transparent;
    color: hsl(var(--muted-foreground));
    cursor: pointer;
    text-align: left;
    font-size: 13px;
    font-weight: 500;
    transition: background 0.12s, color 0.12s;
  }
  .tab-btn:hover {
    background: hsl(0 0% 100% / 0.06);
    color: hsl(var(--foreground));
  }
  .tab-btn.active {
    background: hsl(var(--primary) / 0.12);
    color: hsl(var(--primary));
  }

  /* content area */
  .tab-content { display: grid; gap: 16px; }
  .grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }

  /* form fields */
  .form-grid {
    display: grid;
    grid-template-columns: minmax(0, 1fr);
    gap: 14px;
  }
  .form-grid--3col { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .form-grid--2col { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .form-grid--compact { gap: 10px; margin-top: 10px; }

  .form-field {
    display: grid;
    gap: 5px;
  }
  .form-field span {
    font-size: 12px;
    font-weight: 500;
    color: hsl(var(--muted-foreground));
  }
  .form-field input[type="text"],
  .form-field input[type="url"],
  .form-field input[type="password"],
  .form-field input[type="number"] {
    height: 40px;
    padding: 0 12px;
    border-radius: 10px;
    border: 1px solid hsl(0 0% 100% / 0.12);
    background: hsl(0 0% 100% / 0.05);
    color: hsl(var(--foreground));
    font-size: 13px;
    transition: border-color 0.15s, background 0.15s;
    width: 100%;
  }
  .form-field input:focus,
  .form-field select:focus {
    outline: none;
    border-color: hsl(var(--primary) / 0.5);
    background: hsl(0 0% 100% / 0.08);
  }
  .form-field input::placeholder { color: hsl(var(--muted-foreground)); }
  .form-field select {
    height: 40px;
    padding: 0 12px;
    border-radius: 10px;
    border: 1px solid hsl(0 0% 100% / 0.12);
    background: hsl(0 0% 100% / 0.05);
    color: hsl(var(--foreground));
    font-size: 13px;
    cursor: pointer;
    appearance: auto;
    transition: border-color 0.15s, background 0.15s;
    width: 100%;
  }
  .form-field select option { background: hsl(215 36% 10%); }

  .form-field--toggle {
    flex-direction: row;
    align-items: center;
    gap: 10px;
  }

  .field-hint {
    display: block;
    font-size: 11px;
    color: hsl(var(--muted-foreground));
    font-weight: 400;
    text-transform: none;
    letter-spacing: 0;
    margin-top: 2px;
  }
  .field-hint-inline {
    font-size: 11px;
    color: hsl(var(--muted-foreground));
    font-weight: 400;
    text-transform: none;
    letter-spacing: 0;
  }

  /* subtitle provider */
  .sub-provider {
    margin-top: 14px;
    padding: 12px 14px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.02);
  }
  .sub-provider-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 4px;
  }
  .sub-provider-head strong { font-size: 13px; }

  /* toggle label */
  .toggle-label {
    display: flex;
    align-items: center;
    gap: 8px;
    cursor: pointer;
    font-size: 13px;
    color: hsl(var(--muted-foreground));
  }
  .toggle-label input[type="checkbox"] { accent-color: hsl(var(--primary)); width: 15px; height: 15px; cursor: pointer; }

  /* provider edit cards */
  .provider-forms { display: grid; gap: 16px; }
  .provider-edit-card {
    padding: 16px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 14px;
    background: hsl(0 0% 100% / 0.03);
  }
  .provider-edit-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 14px;
  }
  .pec-title { display: flex; align-items: center; gap: 10px; }
  .pec-title strong { font-size: 14px; }
  .provider-edit-footer {
    display: flex;
    gap: 20px;
    margin-top: 12px;
    padding-top: 12px;
    border-top: 1px solid hsl(0 0% 100% / 0.06);
  }

  /* icon buttons */
  .icon-btn {
    display: inline-grid;
    place-items: center;
    width: 30px;
    height: 30px;
    border-radius: 8px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: transparent;
    color: hsl(var(--muted-foreground));
    cursor: pointer;
    transition: background 0.1s, color 0.1s;
  }
  .icon-btn.danger:hover { background: hsl(0 72% 51% / 0.15); color: hsl(0 96% 82%); border-color: hsl(0 72% 51% / 0.3); }

  /* add button */
  .add-btn {
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 12px;
    padding: 10px 16px;
    border-radius: 10px;
    border: 1px dashed hsl(0 0% 100% / 0.15);
    background: transparent;
    color: hsl(var(--muted-foreground));
    font-size: 13px;
    cursor: pointer;
    width: 100%;
    justify-content: center;
    transition: border-color 0.15s, color 0.15s;
  }
  .add-btn:hover { border-color: hsl(var(--primary) / 0.5); color: hsl(var(--primary)); }

  /* shared */
  .kv-list { display: grid; gap: 12px; }
  .kv-list div { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; padding: 10px 0; border-bottom: 1px solid hsl(0 0% 100% / 0.04); }
  .kv-list div:last-child { border-bottom: none; }
  .kv-list span { color: hsl(var(--muted-foreground)); font-size: 13px; }
  .kv-list strong { font-size: 13px; }

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

  /* blocklist */
  .bl-editor {
    display: grid;
    gap: 12px;
    margin-bottom: 18px;
    padding: 14px;
    border-radius: 16px;
    border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03);
  }

  .bl-editor-head {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    align-items: flex-start;
  }

  .bl-editor-head p {
    margin: 4px 0 0;
    color: hsl(var(--muted-foreground));
    font-size: 12px;
  }

  .bl-editor-actions {
    display: flex;
    justify-content: flex-end;
  }

  .bl-stats-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-bottom: 14px;
    align-items: center;
  }

  .bl-stat-chip {
    display: flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px;
    border-radius: 10px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.04);
    font-size: 12px;
  }

  .bl-stat-chip.warn .bl-stat-num { color: hsl(47 100% 77%); }
  .bl-stat-num { font-weight: 700; }
  .bl-stat-lbl { color: hsl(var(--muted-foreground)); }

  .bl-reason-chip {
    display: flex;
    align-items: center;
    gap: 6px;
    border-radius: 12px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.04);
    padding: 4px 8px;
    cursor: pointer;
    font-size: 12px;
    transition: border-color 0.15s, background 0.15s;
  }

  .bl-reason-chip:hover { background: hsl(0 0% 100% / 0.08); }

  .bl-reason-chip.active {
    border-color: hsl(var(--primary) / 0.5);
    background: hsl(var(--primary) / 0.08);
  }

  .bl-reason-count {
    font-weight: 700;
    color: hsl(var(--foreground));
  }

  .bl-pagination {
    display: flex;
    align-items: center;
    gap: 6px;
    justify-content: center;
    padding: 14px 0 4px;
  }

  .bl-page-size {
    height: 32px;
    padding: 0 8px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 10px;
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--foreground));
    font-size: 12px;
    cursor: pointer;
  }

  .bl-stats-text { color: hsl(var(--muted-foreground)); font-size: 12px; white-space: nowrap; }

  .bl-show-more {
    padding: 4px 10px;
    border-radius: 12px;
    border: 1px dashed hsl(0 0% 100% / 0.15);
    background: none;
    color: hsl(var(--muted-foreground));
    font-size: 12px;
    cursor: pointer;
    white-space: nowrap;
  }
  .bl-show-more:hover { color: hsl(var(--foreground)); border-color: hsl(0 0% 100% / 0.3); }

  .bl-filter-active {
    display: flex;
    align-items: center;
    gap: 5px;
    height: 32px;
    padding: 0 10px;
    border-radius: 10px;
    border: 1px solid hsl(var(--primary) / 0.4);
    background: hsl(var(--primary) / 0.1);
    color: hsl(var(--primary));
    font-size: 12px;
    font-family: 'JetBrains Mono', monospace;
    cursor: pointer;
  }

  .bl-filter-active:hover { background: hsl(var(--primary) / 0.15); }

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

  .bl-table th.sortable { cursor: pointer; user-select: none; }
  .bl-table th.sortable:hover { color: hsl(var(--foreground)); }

  .bl-table td {
    padding: 10px 14px;
    border-bottom: 1px solid hsl(0 0% 100% / 0.04);
    font-size: 13px;
    vertical-align: middle;
  }

  .bl-table tr:last-child td { border-bottom: none; }

  .bl-key-cell, .bl-context-cell { min-width: 220px; }
  .bl-key-top, .bl-row-actions {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .bl-key-top { justify-content: space-between; margin-bottom: 6px; }
  .bl-key { max-width: 300px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; color: hsl(var(--muted-foreground)); font-size: 11px; }
  .bl-context-title {
    font-size: 13px;
    font-weight: 600;
    margin-bottom: 3px;
  }
  .bl-action { width: 84px; text-align: right; }

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

  .reason-badge.neutral {
    border: 1px solid hsl(0 0% 100% / 0.08);
    background: hsl(0 0% 100% / 0.04);
    color: hsl(var(--muted-foreground));
  }

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

  /* seerr webhook */
  .webhook-setup {
    margin-top: 18px;
    padding: 14px 16px;
    border: 1px solid hsl(0 0% 100% / 0.08);
    border-radius: 12px;
    background: hsl(0 0% 100% / 0.03);
  }
  .webhook-setup__header {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 13px;
    font-weight: 600;
    color: hsl(var(--foreground));
    margin-bottom: 8px;
  }
  .webhook-setup__desc {
    font-size: 12px;
    color: hsl(var(--muted-foreground));
    margin: 0 0 10px;
    line-height: 1.55;
  }
  .webhook-setup__steps {
    font-size: 12px;
    color: hsl(var(--muted-foreground));
    margin: 0 0 12px;
    padding-left: 18px;
    line-height: 1.8;
  }
  .webhook-setup__steps strong { color: hsl(var(--foreground)); }
  .webhook-setup__steps code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    background: hsl(0 0% 100% / 0.06);
    border-radius: 4px;
    padding: 1px 5px;
  }
  .webhook-url-row {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .webhook-url {
    flex: 1;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    padding: 8px 12px;
    border-radius: 8px;
    border: 1px solid hsl(0 0% 100% / 0.12);
    background: hsl(0 0% 100% / 0.05);
    color: hsl(var(--foreground));
    user-select: all;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    display: block;
  }
  .copy-btn {
    flex-shrink: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    width: 32px;
    height: 32px;
    border-radius: 8px;
    border: 1px solid hsl(0 0% 100% / 0.12);
    background: hsl(0 0% 100% / 0.06);
    color: hsl(var(--muted-foreground));
    cursor: pointer;
    transition: background 0.15s, color 0.15s;
  }
  .copy-btn:hover { background: hsl(0 0% 100% / 0.1); color: hsl(var(--foreground)); }

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
  .profile-meta { display: grid; grid-template-columns: repeat(3, 1fr); gap: 6px; font-size: 11px; color: hsl(var(--muted-foreground)); }

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

  /* responsive */
  @media (max-width: 1200px) {
    .settings-shell { grid-template-columns: 1fr; }
    .tab-rail { position: static; grid-template-columns: repeat(3, minmax(0, 1fr)); }
  }

  @media (max-width: 900px) {
    .summary-strip, .grid-2, .form-grid, .form-grid--3col { grid-template-columns: 1fr; }
    .profile-meta, .result-grid { grid-template-columns: 1fr; }
    .qp-shell { grid-template-columns: 1fr; }
    .qp-list { position: static; }
  }

  @media (max-width: 600px) {
    .tab-rail { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .size-row { grid-template-columns: 1fr; }
  }

  /* ── Logs tab ──────────────────────────────────────────────── */
  .log-toolbar {
    display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-bottom: 12px;
  }
  .log-search-wrap {
    display: flex; align-items: center; gap: 8px; flex: 1; min-width: 200px;
    height: 40px; padding: 0 14px;
    border: 1px solid hsl(0 0% 100% / 0.08); border-radius: 14px;
    background: hsl(0 0% 100% / 0.04); color: hsl(var(--muted-foreground));
  }
  .log-search-input {
    flex: 1; background: transparent; border: none; outline: none;
    color: hsl(var(--foreground)); font-size: 13px;
  }
  .log-search-input::placeholder { color: hsl(var(--muted-foreground)); }
  .log-level-select {
    height: 40px; padding: 0 12px;
    border: 1px solid hsl(0 0% 100% / 0.08); border-radius: 14px;
    background: hsl(0 0% 100% / 0.04); color: hsl(var(--foreground)); font-size: 13px; cursor: pointer;
  }
  .log-download-link { display: contents; }
  .log-error { margin-bottom: 10px; padding: 10px 14px; border-radius: 12px; background: hsl(0 72% 51% / 0.15); color: hsl(0 96% 82%); font-size: 13px; }
  .log-table-wrap { overflow-x: auto; border: 1px solid hsl(0 0% 100% / 0.08); border-radius: 18px; background: hsl(var(--background) / 0.6); }
  .log-table-wrap table { width: 100%; min-width: 760px; border-collapse: collapse; }
  .log-table-wrap thead { border-bottom: 1px solid hsl(0 0% 100% / 0.06); }
  .log-table-wrap th { padding: 12px 14px; text-align: left; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.14em; color: hsl(var(--muted-foreground)); white-space: nowrap; }
  .log-table-wrap td { padding: 11px 14px; border-bottom: 1px solid hsl(0 0% 100% / 0.04); vertical-align: top; font-size: 13px; }
  .log-table-wrap tr:last-child td { border-bottom: none; }
  .log-col-time { width: 140px; } .log-col-level { width: 72px; } .log-col-service { width: 160px; } .log-col-message { min-width: 200px; }
  .log-empty { padding: 32px; text-align: center; color: hsl(var(--muted-foreground)); }
  .log-row-error td { background: hsl(0 72% 51% / 0.06); }
  .log-row-warn td  { background: hsl(38 96% 55% / 0.06); }
  .log-badge { display: inline-block; padding: 2px 8px; border-radius: 8px; font-size: 10px; font-weight: 700; font-family: 'JetBrains Mono', monospace; letter-spacing: 0.06em; }
  .log-badge-error   { background: hsl(0 72% 51% / 0.2);   color: hsl(0 96% 82%); }
  .log-badge-warn    { background: hsl(38 96% 55% / 0.2);  color: hsl(38 100% 72%); }
  .log-badge-info    { background: hsl(171 82% 55% / 0.15); color: hsl(171 82% 72%); }
  .log-badge-debug   { background: hsl(var(--muted-foreground) / 0.15); color: hsl(var(--muted-foreground)); }
  .log-badge-default { background: hsl(var(--muted-foreground) / 0.15); color: hsl(var(--muted-foreground)); }

  /* ── Tasks tab ─────────────────────────────────────────────── */
  .task-summary-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
  .task-summary-card { padding: 14px 16px; border: 1px solid hsl(0 0% 100% / 0.06); border-radius: 18px; background: hsl(0 0% 100% / 0.03); }
  .task-summary-value { font-size: 1.8rem; font-weight: 700; line-height: 1; }
  .task-summary-label { margin-top: 6px; color: hsl(var(--muted-foreground)); font-size: 12px; }
  .task-table-wrap { overflow-x: auto; }
  .task-table-wrap table { width: 100%; min-width: 760px; border-collapse: collapse; }
  .task-table-wrap th, .task-table-wrap td { padding: 12px 10px; border-bottom: 1px solid hsl(0 0% 100% / 0.05); text-align: left; vertical-align: top; }
  .task-table-wrap th { font-size: 11px; text-transform: uppercase; letter-spacing: 0.18em; color: hsl(var(--muted-foreground)); }
  .task-group-row td { padding-top: 20px; font-size: 12px; font-weight: 700; letter-spacing: 0.12em; text-transform: uppercase; color: hsl(var(--primary)); }
  .task-row-title { font-weight: 600; }
  .task-row-sub { margin-top: 4px; color: hsl(var(--muted-foreground)); font-size: 12px; }
  .task-result { display: inline-flex; align-items: center; gap: 6px; margin-top: 8px; font-size: 12px; font-family: 'JetBrains Mono', monospace; }
  .task-result.ok { color: hsl(141 80% 68%); }
  .task-result.fail { color: hsl(0 96% 82%); }
  .time-cell { display: inline-flex; align-items: center; gap: 6px; color: hsl(var(--muted-foreground)); font-size: 12px; }
  .time-cell.dim { opacity: 0.4; }
  :global(.spin) { animation: spin 1s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }
  @media (max-width: 900px) { .task-summary-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }

  /* ── Quality Profiles tab ──────────────────────────────────── */
  .qp-shell { display: grid; grid-template-columns: 220px minmax(0, 1fr); gap: 16px; align-items: start; }
  .qp-list { display: grid; gap: 8px; position: sticky; top: 88px; }
  .qp-item {
    display: grid; gap: 3px; padding: 10px 12px;
    border-radius: 14px; border: 1px solid hsl(0 0% 100% / 0.06);
    background: hsl(0 0% 100% / 0.03); text-align: left; cursor: pointer; transition: background 0.12s;
  }
  .qp-item:hover, .qp-item.selected { background: hsl(var(--primary) / 0.12); border-color: hsl(var(--primary) / 0.28); }
  .qp-item-name { display: flex; align-items: center; gap: 6px; font-size: 13px; font-weight: 600; }
  .qp-item-name :global(.qp-star) { color: hsl(var(--primary)); }
  .qp-item-meta { font-size: 11px; color: hsl(var(--muted-foreground)); font-family: 'JetBrains Mono', monospace; }
  .qp-editor { display: grid; }
  .qp-no-selection { padding: 32px; border-radius: 18px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.02); color: hsl(var(--muted-foreground)); text-align: center; }
  .field { margin-bottom: 20px; }
  .field-label { font-size: 13px; font-weight: 600; margin-bottom: 10px; display: flex; align-items: baseline; gap: 8px; }
  .field-hint { font-size: 11px; font-weight: 400; color: hsl(var(--muted-foreground)); }
  .field-input { width: 100%; padding: 10px 12px; border-radius: 12px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.04); color: hsl(var(--foreground)); font-size: 13px; }
  .divider { height: 1px; background: hsl(0 0% 100% / 0.06); margin: 6px 0 20px; }
  .ordered-list { display: grid; gap: 6px; }
  .ordered-row { display: flex; align-items: center; gap: 8px; padding: 8px 10px; border-radius: 10px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03); }
  .rank { min-width: 22px; font-size: 11px; font-weight: 700; font-family: 'JetBrains Mono', monospace; color: hsl(var(--primary)); }
  .ordered-value { flex: 1; font-size: 13px; font-family: 'JetBrains Mono', monospace; }
  .rank-btn { display: grid; place-items: center; width: 26px; height: 26px; border-radius: 7px; border: 1px solid hsl(0 0% 100% / 0.06); background: transparent; color: hsl(var(--muted-foreground)); cursor: pointer; font-size: 12px; }
  .rank-btn:hover { background: hsl(0 0% 100% / 0.08); color: hsl(var(--foreground)); }
  .rank-btn:disabled { opacity: 0.3; cursor: default; }
  .rank-btn.remove:hover { background: hsl(0 72% 51% / 0.15); color: hsl(0 96% 82%); }
  .chip-row { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
  .chip { padding: 5px 12px; border-radius: 10px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.04); color: hsl(var(--muted-foreground)); font-size: 12px; font-family: 'JetBrains Mono', monospace; cursor: pointer; transition: all 0.12s; }
  .chip.on { background: hsl(var(--primary) / 0.18); border-color: hsl(var(--primary) / 0.4); color: hsl(var(--primary)); }
  .chip.add { border-style: dashed; font-size: 11px; }
  .chip.add:hover, .chip:not(.on):hover { background: hsl(0 0% 100% / 0.08); color: hsl(var(--foreground)); }
  .flags-grid { display: grid; gap: 10px; }
  .flag-row { display: flex; align-items: flex-start; gap: 12px; padding: 12px 14px; border-radius: 12px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03); cursor: pointer; }
  .flag-row input[type=checkbox] { width: 16px; height: 16px; flex-shrink: 0; margin-top: 2px; accent-color: hsl(var(--primary)); cursor: pointer; }
  .flag-row strong { display: block; font-size: 13px; margin-bottom: 2px; }
  .flag-row span { display: block; font-size: 12px; color: hsl(var(--muted-foreground)); }
  .size-row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
  .size-row label { display: grid; gap: 6px; }
  .size-row span { font-size: 12px; color: hsl(var(--muted-foreground)); }
  .size-input { width: 100%; padding: 10px 12px; border-radius: 12px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.04); color: hsl(var(--foreground)); font-size: 13px; font-family: 'JetBrains Mono', monospace; }
  .editor-actions { display: flex; justify-content: flex-end; gap: 10px; margin-top: 6px; }
  .exclude-patterns-input { width: 100%; padding: 10px 12px; border-radius: 12px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.04); color: hsl(var(--foreground)); font-size: 12px; font-family: 'JetBrains Mono', monospace; resize: vertical; }

  /* ── Quality sub-tabs ───────────────────────────────────────── */
  .quality-sub-tabs { display: flex; gap: 4px; margin-bottom: 16px; padding: 4px; border-radius: 12px; background: hsl(0 0% 100% / 0.04); border: 1px solid hsl(0 0% 100% / 0.06); width: fit-content; }
  .sub-tab-btn { padding: 6px 18px; border-radius: 9px; border: none; background: transparent; color: hsl(var(--muted-foreground)); font-size: 13px; font-weight: 500; cursor: pointer; transition: all 0.12s; }
  .sub-tab-btn:hover { color: hsl(var(--foreground)); background: hsl(0 0% 100% / 0.06); }
  .sub-tab-btn.active { background: hsl(var(--primary) / 0.18); color: hsl(var(--primary)); }
  .qdef-shell { display: grid; gap: 20px; }
  .qdef-table { width: 100%; border-collapse: collapse; font-size: 13px; }
  .qdef-table thead th { text-align: left; padding: 8px 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.05em; color: hsl(var(--muted-foreground)); border-bottom: 1px solid hsl(0 0% 100% / 0.08); }
  .qdef-table tbody tr:hover { background: hsl(0 0% 100% / 0.02); }
  .qdef-table td { padding: 6px 12px; border-bottom: 1px solid hsl(0 0% 100% / 0.04); }
  .qdef-title { font-size: 13px; min-width: 180px; }
  .qdef-input { width: 90px; padding: 6px 10px; border-radius: 8px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.04); color: hsl(var(--foreground)); font-size: 12px; font-family: 'JetBrains Mono', monospace; }
  .qdef-input:focus { outline: none; border-color: hsl(var(--primary) / 0.5); }
  .qdef-save-btn { padding: 4px 12px; border-radius: 6px; border: 1px solid hsl(var(--primary) / 0.5); background: hsl(var(--primary) / 0.12); color: hsl(var(--primary)); font-size: 11px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; }
  .qdef-save-btn:disabled { opacity: 0.25; cursor: default; }

  /* ── Plex OAuth ─────────────────────────────────────────────── */
  .plex-token-row { display: flex; gap: 10px; align-items: center; }
  .plex-token-row input { flex: 1; }
  .plex-oauth-status { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
  .plex-open-link { display: inline-flex; align-items: center; gap: 6px; height: 36px; padding: 0 14px; border-radius: 12px; border: 1px solid hsl(var(--primary) / 0.4); background: hsl(var(--primary) / 0.12); color: hsl(var(--primary)); font-size: 13px; font-weight: 600; text-decoration: none; }
  .plex-oauth-hint { font-size: 12px; color: hsl(var(--muted-foreground)); }
  .plex-cancel-btn { height: 36px; padding: 0 12px; border-radius: 12px; border: 1px solid hsl(0 0% 100% / 0.08); background: transparent; color: hsl(var(--muted-foreground)); font-size: 12px; cursor: pointer; }
  .plex-cancel-btn:hover { background: hsl(0 0% 100% / 0.08); }

  /* ── Custom Formats ─────────────────────────────────────────── */
  .cf-layout { display: grid; grid-template-columns: 200px 1fr; gap: 16px; }
  .cf-list { display: grid; gap: 4px; align-content: start; }
  .cf-list-header { display: flex; align-items: center; justify-content: space-between; font-size: 12px; font-weight: 600; color: hsl(var(--muted-foreground)); padding: 0 4px 6px; text-transform: uppercase; letter-spacing: 0.06em; }
  .cf-item { display: flex; align-items: center; justify-content: space-between; gap: 6px; padding: 8px 12px; border-radius: 10px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03); cursor: pointer; text-align: left; }
  .cf-item:hover, .cf-item.cf-active { background: hsl(var(--primary) / 0.12); border-color: hsl(var(--primary) / 0.3); }
  .cf-item-name { font-size: 13px; font-weight: 500; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cf-item-score { font-size: 12px; font-family: 'JetBrains Mono', monospace; font-weight: 700; flex-shrink: 0; }
  .cf-item-score.cf-pos { color: hsl(140 60% 50%); }
  .cf-item-score.cf-neg { color: hsl(0 70% 60%); }
  .cf-disabled-badge { font-size: 10px; background: hsl(0 0% 100% / 0.08); border-radius: 4px; padding: 1px 5px; color: hsl(var(--muted-foreground)); }
  .cf-editor { display: grid; gap: 14px; }
  .cf-empty { padding: 24px; color: hsl(var(--muted-foreground)); font-size: 13px; text-align: center; }
  .cf-import-box { display: grid; gap: 10px; padding: 14px 16px; border-radius: 14px; border: 1px solid hsl(var(--primary) / 0.3); background: hsl(var(--primary) / 0.05); margin-bottom: 16px; }
  .cf-import-header { display: grid; gap: 4px; }
  .cf-import-textarea { width: 100%; min-height: 140px; border-radius: 10px; border: 1px solid hsl(0 0% 100% / 0.1); background: hsl(0 0% 100% / 0.04); color: inherit; font-family: 'JetBrains Mono', monospace; font-size: 12px; padding: 10px; resize: vertical; }
  @media (max-width: 600px) {
    .cf-layout { grid-template-columns: 1fr; }
  }

  /* ── Release Filtering ─────────────────────────────────────────────────── */
  .rf-layout { display: grid; grid-template-columns: 240px 1fr; gap: 16px; }
  .rf-list { display: grid; gap: 3px; align-content: start; max-height: 600px; overflow-y: auto; }
  .rf-list-header { display: flex; align-items: center; justify-content: space-between; font-size: 12px; font-weight: 600; color: hsl(var(--muted-foreground)); padding: 0 4px 6px; text-transform: uppercase; letter-spacing: 0.06em; }
  .rf-type-header { font-size: 11px; font-weight: 600; color: hsl(var(--muted-foreground)); padding: 10px 4px 3px; text-transform: uppercase; letter-spacing: 0.06em; display: flex; align-items: center; gap: 6px; }
  .rf-count { font-size: 10px; background: hsl(0 0% 100% / 0.08); border-radius: 4px; padding: 1px 5px; }
  .rf-item { display: flex; align-items: center; justify-content: space-between; gap: 4px; padding: 6px 10px; border-radius: 9px; border: 1px solid hsl(0 0% 100% / 0.06); background: hsl(0 0% 100% / 0.03); cursor: pointer; text-align: left; transition: background 0.1s; }
  .rf-item:hover, .rf-item.rf-active { background: hsl(var(--primary) / 0.12); border-color: hsl(var(--primary) / 0.3); }
  .rf-item.rf-disabled { opacity: 0.45; }
  .rf-pattern { font-size: 12px; font-family: 'JetBrains Mono', monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 1; }
  .rf-badges { display: flex; gap: 3px; flex-shrink: 0; }
  .rf-badge { font-size: 10px; border-radius: 4px; padding: 1px 5px; white-space: nowrap; }
  .rf-badge-block { background: hsl(0 70% 50% / 0.2); color: hsl(0 70% 65%); }
  .rf-badge-penalty { background: hsl(43 90% 50% / 0.2); color: hsl(43 90% 65%); }
  .rf-badge-src { background: hsl(0 0% 100% / 0.08); color: hsl(var(--muted-foreground)); }
  .rf-badge-mt { background: hsl(217 80% 60% / 0.2); color: hsl(217 80% 72%); }
  .rf-badge-off { background: hsl(0 0% 100% / 0.06); color: hsl(var(--muted-foreground)); }
  .rf-editor { display: grid; gap: 14px; align-content: start; }
  .rf-readonly-note { font-size: 12px; color: hsl(var(--muted-foreground)); background: hsl(0 0% 100% / 0.03); border: 1px solid hsl(0 0% 100% / 0.06); border-radius: 10px; padding: 10px 12px; margin: 0; }
  .rf-test-panel { margin-top: 24px; padding: 14px; border: 1px solid hsl(0 0% 100% / 0.06); border-radius: 14px; background: hsl(0 0% 100% / 0.02); display: grid; gap: 10px; }
  .rf-test-header { font-size: 12px; font-weight: 600; color: hsl(var(--muted-foreground)); text-transform: uppercase; letter-spacing: 0.06em; }
  .rf-test-row { display: flex; gap: 8px; align-items: center; }
  .rf-test-result { padding: 10px 12px; border-radius: 10px; border: 1px solid hsl(0 0% 100% / 0.08); background: hsl(0 0% 100% / 0.03); font-size: 13px; }
  .rf-test-result.rf-test-blocked { background: hsl(0 70% 50% / 0.1); border-color: hsl(0 70% 50% / 0.3); color: hsl(0 70% 65%); }
  .rf-test-result.rf-test-allowed { background: hsl(140 60% 40% / 0.1); border-color: hsl(140 60% 40% / 0.3); color: hsl(140 60% 60%); }
  .rf-test-matches { margin: 6px 0 0; padding-left: 18px; display: grid; gap: 4px; list-style: disc; }
  .rf-test-matches li { font-size: 12px; color: inherit; }
  @media (max-width: 700px) {
    .rf-layout { grid-template-columns: 1fr; }
  }
</style>
