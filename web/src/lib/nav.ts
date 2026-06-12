import Gauge from '@lucide/svelte/icons/gauge';
import FileSearch from '@lucide/svelte/icons/file-search';
import LibraryBig from '@lucide/svelte/icons/library-big';
import ListOrdered from '@lucide/svelte/icons/list-ordered';
import HeartPulse from '@lucide/svelte/icons/heart-pulse';
import Server from '@lucide/svelte/icons/server';
import SlidersHorizontal from '@lucide/svelte/icons/sliders-horizontal';
import FolderTree from '@lucide/svelte/icons/folder-tree';
import ScrollText from '@lucide/svelte/icons/scroll-text';
import ListChecks from '@lucide/svelte/icons/list-checks';
import Settings2 from '@lucide/svelte/icons/settings-2';
import CalendarDays from '@lucide/svelte/icons/calendar-days';
import ScanSearch from '@lucide/svelte/icons/scan-search';

// All nav items — shown in sidebar on desktop and in mobile drawer
export const navItems = [
  { href: '/dashboard',       label: 'Dashboard',  icon: Gauge },
  { href: '/library',         label: 'Library',    icon: LibraryBig },
  { href: '/search',          label: 'Discover',   icon: FileSearch },
  { href: '/calendar',        label: 'Calendar',   icon: CalendarDays },
  { href: '/downloads',       label: 'Queue',      icon: ListOrdered },
  { href: '/health',          label: 'Health',     icon: HeartPulse },
  { href: '/services',        label: 'Services',   icon: Server },
  { href: '/vfs',             label: 'Files',      icon: FolderTree },
  { href: '/manual-search',   label: 'NZB Search', icon: ScanSearch },
  { href: '/profiles',        label: 'Profiles',   icon: SlidersHorizontal },
  { href: '/tasks',           label: 'Tasks',      icon: ListChecks },
  { href: '/logs',            label: 'Logs',       icon: ScrollText },
  { href: '/settings',        label: 'Settings',   icon: Settings2 },
] as const;

// First 5 shown in mobile bottom bar; rest in drawer (matches reference AppLayout)
export const mobilePrimaryItems = navItems.slice(0, 5);
