// Package ranking scores and filters Usenet release candidates.
// The logic mirrors Radarr/Sonarr's QualityParser and custom-format approach:
// hard rejections first (bad source, size), then additive scoring for
// resolution, source, codec, audio, HDR, proper/repack, and recency.
package ranking

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// compilePatterns compiles a slice of regex strings, silently skipping invalid ones.
func compilePatterns(patterns []string) []*regexp.Regexp {
	out := make([]*regexp.Regexp, 0, len(patterns))
	for _, p := range patterns {
		if r, err := regexp.Compile(p); err == nil {
			out = append(out, r)
		}
	}
	return out
}

// ── Compiled regexes (Radarr/Sonarr QualityParser.cs patterns) ─────────────

// reStructuredRelease matches release names that have recognizable Usenet
// release markers (year, resolution, season/episode, streaming source).
// Titles that match are treated as readable — the title check applies even
// when TrustSource=true. Titles that don't match (random-looking obfuscated
// NZB subjects like "bGimprckUaaY.mkv") skip the title check as intended.
var reStructuredRelease = regexp.MustCompile(
	`(?i)(?:\b(?:19|20)\d{2}\b` + // year (1900–2099)
		`|\b\d{3,4}[piP I]\b` + // resolution: 720p, 1080i, 2160p
		`|\bS\d{1,2}E\d{1,2}\b` + // SxxExx episode marker
		`|\bS\d{2}\b` + // season pack Sxx
		`|\b(?:BluRay|WEB-DL|WEBRip|HDTV|AMZN|NF|DSNP|HULU|MAX|PCOK|ATVP|SHO)\b)`) // streaming source

var (
	// proper/repack — word-boundary aware, matches -PROPER, PROPER., repack2, rerip
	reProper = regexp.MustCompile(`(?i)\b(proper)\b`)
	reRepack = regexp.MustCompile(`(?i)\b(repack\d?|rerip\d?)\b`)
	reReal   = regexp.MustCompile(`(?i)\b(REAL)\b`)

	// remux — handles BD.Remux, UHD.Remux, Hybrid-Remux (Radarr QualityParser)
	reRemux = regexp.MustCompile(`(?i)(?:(?:BD|UHD)[-_. ]?)?Remux|Hybrid[-_. ]?Remux`)

	// Dolby Vision — word boundary or separator, avoids false positives
	// Radarr custom-format pattern: dovi, dolby.vision, DV followed by separator
	reDV = regexp.MustCompile(`(?i)(?:\b(?:dovi|dolby[-_. ]?vision)\b|(?:^|[-_. \[])dv(?:[-_. \][]|$))`)

	// HDR10+ and HDR10
	reHDR10Plus = regexp.MustCompile(`(?i)\bHDR10\+|HDR10[Pp]\b`)
	reHDR10     = regexp.MustCompile(`(?i)\bHDR10?\b`)
	reHLG       = regexp.MustCompile(`(?i)\bHLG\b`)

	// BR-DISK / unencoded Blu-ray — Radarr rejects these massive raw rips
	// Detects: ISO, BDMV, COMPLETE.BLURAY, FULL.BLURAY, BD25, BD50, BD100
	reBRDisk = regexp.MustCompile(`(?i)\b(?:BDMV|BD(?:25|50|66|100)|COMPLETE[-_. ]?BLU[-_. ]?RAY|FULL[-_. ]?BLU[-_. ]?RAY|BD[-_. ]?ISO|BLU[-_. ]?RAY[-_. ]?ISO)\b`)

	// Streaming-service WEB-DL prefixes (Radarr: AMZN, NF, ATVP, DSNP, HMAX, PCOK, SHO)
	reStreamingService = regexp.MustCompile(`(?i)(?:amzn|amazon|nf|netflix|atvp|apple|dsnp|disney|hmax|hbo|pcok|sho|showtime|pmtp|paramount|crkl|crunchyroll|crav|crave|stan|bcore)[-_. ]web[-_. ]`)

	// Hardcoded/burned-in subtitles (trash source)
	reHardSubs = regexp.MustCompile(`(?i)\b(?:HC|SUBBED|HARDSUB)\b`)

	// Sample detection (more precise than simple Contains)
	reSample = regexp.MustCompile(`(?i)(?:^|[-_. \[(])sample(?:[-_. \])]|$)`)

	// Audio: Atmos (standalone or in TrueHD.Atmos)
	reAtmos  = regexp.MustCompile(`(?i)\bAtmos\b`)
	reTrueHD = regexp.MustCompile(`(?i)\bTrue[-_. ]?HD\b`)
	reDTSHD  = regexp.MustCompile(`(?i)\bDTS[-_. ]?(?:HD[-_. ]?(?:MA|HRA)|MA)\b|\bDTSHD\b|\bDTS[-_. ]?X(?:\b|\d)`)
	reDTS    = regexp.MustCompile(`(?i)\bDTS\b`)
	reDD     = regexp.MustCompile(`(?i)\b(?:EAC[-_. ]?3|DD\+|DDP(?:Atmos)?|E[-_. ]?AC[-_. ]?3)\b`)
	reAC3    = regexp.MustCompile(`(?i)\b(?:AC[-_. ]?3|DD2|Dolby[-_. ]?Digital)\b`)
	reAAC    = regexp.MustCompile(`(?i)\bAAC\b`)
	reFLAC   = regexp.MustCompile(`(?i)\bFLAC\b`)
)

// ── Public types ─────────────────────────────────────────────────────────────

type Candidate struct {
	ID           int64
	Title        string
	SizeBytes    int64
	Resolution   string
	Source       string
	Codec        string
	Language     string
	Indexer      string
	ReleaseGroup string
	UploadedAt   time.Time
	Degraded     bool
	FailureCount int
	Grabs        int
	IndexerScore int
	// IndexerPolicyScore is a static modifier from the indexer_policies table.
	// Loaded by the workflow service at search time and added directly to the score.
	IndexerPolicyScore int
}

type Requirements struct {
	Title         string
	MediaType     string
	Year          int
	SeasonNumber  int
	EpisodeNumber int
	// TrustSource: skip title check for ID-based searches (TMDB/IMDB/TVDB).
	// Obfuscated NZB subjects would otherwise wrongly reject valid results.
	TrustSource bool
	// AlternateTitles: additional titles to accept (e.g. "Avengers Assemble"
	// for UK release of "The Avengers"). Matches any one of these before
	// declaring wrong_title. Mirrors Radarr's AlternativeTitles check.
	AlternateTitles []string
	// RuntimeMinutes: used to convert TierMBPerMinuteLimits into expected total sizes.
	// When 0, per-tier MB/min checks are skipped (graceful degradation).
	RuntimeMinutes int
}

type CustomFormat struct {
	Name    string
	Pattern string
	Score   int
	Enabled bool
	Source  string
}

// BlockRule is a release filter rule loaded from the release_block_rules table.
type BlockRule struct {
	ID           int64
	Type         string // "release_group" | "title_pattern" | "regex" | "missing_release_group"
	Pattern      string
	MediaType    string // "movie" | "tv" | "both"
	Action       string // "block" | "penalty"
	ScorePenalty int    // only used when Action == "penalty"
	Enabled      bool
	Source       string
	Note         string
}

// unifiedRule is the internal normalized form of both a CustomFormat and a BlockRule.
// It is constructed by buildUnifiedRules and evaluated in a single pass inside
// ScoreWithPreferences.
type unifiedRule struct {
	id          int64
	name        string // custom format name; empty for block rules
	patternType string // "regex" | "release_group" | "title_pattern" | "missing_release_group"
	pattern     string
	mediaType   string // "movie" | "tv" | "both"
	action      string // "score" | "penalty" | "block"
	score       int    // used when action=="score" (may be negative)
	penalty     int    // used when action=="penalty"
	enabled     bool
}

// buildUnifiedRules merges custom formats (action=score) and block rules into a
// single ordered slice. Custom formats come first; block rules follow.
func buildUnifiedRules(customFormats []CustomFormat, blockRules []BlockRule) []unifiedRule {
	out := make([]unifiedRule, 0, len(customFormats)+len(blockRules))
	for _, cf := range customFormats {
		out = append(out, unifiedRule{
			name:        cf.Name,
			patternType: "regex",
			pattern:     cf.Pattern,
			mediaType:   "both",
			action:      "score",
			score:       cf.Score,
			enabled:     cf.Enabled,
		})
	}
	for _, br := range blockRules {
		action := br.Action
		if action == "" {
			action = "block"
		}
		out = append(out, unifiedRule{
			id:          br.ID,
			patternType: br.Type,
			pattern:     br.Pattern,
			mediaType:   br.MediaType,
			action:      action,
			penalty:     br.ScorePenalty,
			enabled:     br.Enabled,
		})
	}
	return out
}

type Preferences struct {
	Resolutions     []string
	Sources         []string
	Codecs          []string
	Languages       []string
	AudioFormats    []string
	HdrFormats      []string
	ExcludePatterns []string
	PreferProper    bool
	PreferRepack    bool
	RejectCam       bool
	MinMBPerMinute  int
	MaxMBPerMinute  int
	// TierMBPerMinuteLimits maps a resolution string (e.g. "1080p") to [minMB/min, maxMB/min].
	// Zero values in either slot mean "no limit for that bound".
	// Applied only when Requirements.RuntimeMinutes > 0.
	TierMBPerMinuteLimits map[string][2]int
	// MinimumAgeHours: reject candidates posted fewer than N hours ago.
	MinimumAgeHours int
	// CutoffResolution: once the grabbed release meets this resolution or better,
	// the item is considered "at cutoff" and won't be upgraded further.
	// Used by the upgrade scheduler to skip items already at quality cutoff.
	CutoffResolution string
	// MinimumUpgradeCustomFormatScore requires an upgrade candidate to improve the
	// custom-format subtotal by at least this amount versus the current release.
	MinimumUpgradeCustomFormatScore int
	// CustomFormats: user-defined scoring rules applied on top of base score.
	CustomFormats []CustomFormat
	// BlockRules: release filter rules. Block action rejects; penalty action subtracts score.
	BlockRules []BlockRule
}

type Result struct {
	Score                 int
	CustomFormatScore     int
	Explanations          []string
	CompatibilityWarnings []string
	Rejected              bool
	RejectReason          string
}

// ── Scoring entry points ─────────────────────────────────────────────────────

func Score(candidate Candidate, required Requirements) Result {
	return ScoreWithPreferences(candidate, required, Preferences{})
}

func ScoreWithPreferences(candidate Candidate, required Requirements, prefs Preferences) Result {
	explanations := []string{}
	addExplanation := func(format string, args ...any) {
		explanations = append(explanations, fmt.Sprintf(format, args...))
	}
	titleLower := strings.ToLower(candidate.Title)
	requiredLower := strings.ToLower(required.Title)

	// Apply title check when NOT trusting the source, OR when the release title
	// is structured (has year/season/resolution markers). Structured titles are
	// readable — a wrong show name (e.g. "Reno 911" returned for a "9-1-1"
	// TVDB-ID query) must be rejected even if the indexer said it was correct.
	// Obfuscated NZB subjects (no markers) still bypass the check when
	// TrustSource=true, because their filename conveys nothing about the content.
	if !required.TrustSource || reStructuredRelease.MatchString(candidate.Title) {
		matched := containsNormalized(titleLower, requiredLower)
		for i := 0; i < len(required.AlternateTitles) && !matched; i++ {
			matched = containsNormalized(titleLower, strings.ToLower(required.AlternateTitles[i]))
		}
		if !matched {
			return Result{Rejected: true, RejectReason: "wrong_title", Explanations: []string{"Rejected: title did not match the requested title."}}
		}
	}

	// ── Minimum age ─────────────────────────────────────────────────────────
	if prefs.MinimumAgeHours > 0 && !candidate.UploadedAt.IsZero() {
		if time.Since(candidate.UploadedAt) < time.Duration(prefs.MinimumAgeHours)*time.Hour {
			return Result{Rejected: true, RejectReason: "too_new", Explanations: []string{fmt.Sprintf("Rejected: upload is newer than the minimum age of %dh.", prefs.MinimumAgeHours)}}
		}
	}

	// ── Exclude patterns ────────────────────────────────────────────────────
	for _, re := range compilePatterns(prefs.ExcludePatterns) {
		if re.MatchString(candidate.Title) {
			return Result{Rejected: true, RejectReason: "excluded_pattern", Explanations: []string{"Rejected: matched an excluded profile pattern."}}
		}
	}

	// ── Hard rejections ──────────────────────────────────────────────────────

	// CAM/TS/Screener — Radarr QualityParser extended set
	if hasRejectedSource(titleLower) {
		return Result{Rejected: true, RejectReason: "bad_source", Explanations: []string{"Rejected: low-quality source such as CAM/TS/Screener was detected."}}
	}
	// Unencoded Blu-ray disc (BD-ISO, BDMV, COMPLETE.BLURAY) — always reject
	if reBRDisk.MatchString(candidate.Title) {
		return Result{Rejected: true, RejectReason: "br_disk", Explanations: []string{"Rejected: raw Blu-ray disc content was detected."}}
	}
	// Hardcoded/burned subs
	if reHardSubs.MatchString(candidate.Title) {
		return Result{Rejected: true, RejectReason: "hardsub", Explanations: []string{"Rejected: hardcoded subtitles were detected."}}
	}
	if sizeReject := rejectBySize(candidate, prefs, required.RuntimeMinutes); sizeReject != "" {
		return Result{Rejected: true, RejectReason: sizeReject, Explanations: []string{"Rejected: size was outside configured MB/min limits."}}
	}
	if rejectLanguageMismatch(candidate.Language, prefs) {
		return Result{Rejected: true, RejectReason: "wrong_language", Explanations: []string{"Rejected: detected language did not match the profile language preferences."}}
	}

	// ── Year / episode match ─────────────────────────────────────────────────

	score := 0
	switch required.MediaType {
	case "movie":
		switch matchYear(titleLower, required.Year) {
		case yearMismatch:
			return Result{Rejected: true, RejectReason: "wrong_year", Explanations: []string{"Rejected: release year did not match the requested year."}}
		case yearExact:
			score += 90
			addExplanation("Exact year match (+90)")
		}
	case "episode":
		switch matchEpisode(titleLower, required.SeasonNumber, required.EpisodeNumber) {
		case episodeMismatch:
			return Result{Rejected: true, RejectReason: "wrong_episode", Explanations: []string{"Rejected: season/episode did not match the requested episode."}}
		case episodeExact:
			score += 350
			addExplanation("Exact episode match (+350)")
		case episodeSeasonPack:
			score += 120
			addExplanation("Season pack match (+120)")
		}
		switch matchYear(titleLower, required.Year) {
		case yearExact:
			score += 30
			addExplanation("Year matched show metadata (+30)")
		case yearMismatch:
			score -= 40
			addExplanation("Year mismatched show metadata (-40)")
		}
	}

	// ── Quality scoring ──────────────────────────────────────────────────────

	if points := scoreResolution(candidate.Resolution, prefs); points != 0 {
		score += points
		addExplanation("Resolution %s (%+d)", candidate.Resolution, points)
	}
	if points := scoreSourceField(candidate.Source, titleLower, prefs); points != 0 {
		score += points
		label := candidate.Source
		if strings.TrimSpace(label) == "" {
			label = "detected source"
		}
		addExplanation("Source %s (%+d)", label, points)
	}
	if points := scoreCodec(candidate.Codec, prefs); points != 0 {
		score += points
		addExplanation("Codec %s (%+d)", candidate.Codec, points)
	}
	if points := scoreLanguage(candidate.Language, prefs); points != 0 {
		score += points
		addExplanation("Language %s (%+d)", candidate.Language, points)
	}

	audio := ParseAudioFormat(candidate.Title)
	if points := scoreAudio(audio, prefs); points != 0 {
		score += points
		addExplanation("Audio %s (%+d)", audio, points)
	}

	hdr := ParseHDRFormat(candidate.Title)
	if points := scoreHDR(hdr, prefs); points != 0 {
		score += points
		addExplanation("HDR %s (%+d)", hdr, points)
	}

	// ── Release quality signals ───────────────────────────────────────────────

	// Remux — Radarr pattern: BD.Remux, UHD.Remux, Hybrid-Remux
	if reRemux.MatchString(candidate.Title) {
		score += 40
		addExplanation("Remux detected (+40)")
	}

	// Proper/Repack — Radarr uses \bproper\b, \brepack\d?\b, \brerip\d?\b
	isProper := reProper.MatchString(candidate.Title)
	isRepack := reRepack.MatchString(candidate.Title)
	isReal := reReal.MatchString(candidate.Title)
	if (isProper || isReal) && prefs.PreferProper {
		score += 80
		addExplanation("Preferred proper/real release (+80)")
	} else if isProper || isReal {
		score += 40
		addExplanation("Proper/real release (+40)")
	}
	if isRepack && prefs.PreferRepack {
		score += 60
		addExplanation("Preferred repack release (+60)")
	} else if isRepack {
		score += 20
		addExplanation("Repack release (+20)")
	}

	if candidate.Indexer != "" {
		score += 75
		addExplanation("Named indexer bonus (+75)")
	}
	if candidate.ReleaseGroup != "" {
		score += 50
		addExplanation("Release group detected (+50)")
	}

	// Upload recency — trash-guides: prefer recent uploads
	if candidate.UploadedAt.After(time.Now().Add(-30 * 24 * time.Hour)) {
		score += 25
		addExplanation("Recent upload bonus (+25)")
	}

	// Indexer trust score from NZBHydra2 (1–3). Acts as a tiebreaker between
	// otherwise equal candidates from different indexers.
	if candidate.IndexerScore != 0 {
		points := candidate.IndexerScore * 10
		score += points
		addExplanation("Indexer trust score (%+d)", points)
	}

	// Per-indexer policy modifier configured by the operator in Settings → Indexers.
	if candidate.IndexerPolicyScore != 0 {
		score += candidate.IndexerPolicyScore
		addExplanation("Indexer policy %s (%+d)", candidate.Indexer, candidate.IndexerPolicyScore)
	}

	// Community grab count — proxy for a release actually being complete and
	// working. Capped at 50 points so it doesn't overpower quality signals.
	if candidate.Grabs > 0 {
		grabBonus := candidate.Grabs / 10
		if grabBonus > 50 {
			grabBonus = 50
		}
		score += grabBonus
		addExplanation("Community grab count bonus (%+d)", grabBonus)
	}

	// Sample penalty — word-boundary aware
	if reSample.MatchString(candidate.Title) {
		score -= 150
		addExplanation("Sample content penalty (-150)")
	}

	// Failure penalties
	if candidate.Degraded {
		score -= 300
		addExplanation("Degraded candidate penalty (-300)")
	} else if candidate.FailureCount >= 5 {
		score -= 50000 // effectively excluded
		addExplanation("Heavy prior failure penalty (-50000)")
	} else if candidate.FailureCount > 0 {
		penalty := 300 * candidate.FailureCount
		score -= penalty
		addExplanation("Prior failure penalty (-%d)", penalty)
	}

	// ── Custom formats + release block rules (unified evaluation pass) ──────────
	customFormatScore := 0
	if len(prefs.CustomFormats)+len(prefs.BlockRules) > 0 {
		effectiveMediaType := required.MediaType
		if effectiveMediaType == "episode" {
			effectiveMediaType = "tv"
		}
		candidateTitleLower := strings.ToLower(candidate.Title)
		releaseGroup := candidate.ReleaseGroup
		if releaseGroup == "" {
			releaseGroup = ParseReleaseGroup(candidate.Title)
		}
		releaseGroupLower := strings.ToLower(releaseGroup)

		for _, rule := range buildUnifiedRules(prefs.CustomFormats, prefs.BlockRules) {
			if !rule.enabled {
				continue
			}
			// Media-type filter applies to block rules; scoring rules always apply.
			if rule.mediaType != "both" && rule.mediaType != effectiveMediaType {
				continue
			}
			switch rule.action {
			case "score":
				if rule.pattern == "" {
					continue
				}
				re, err := regexp.Compile(rule.pattern)
				if err != nil {
					continue
				}
				if re.MatchString(candidate.Title) {
					score += rule.score
					customFormatScore += rule.score
					addExplanation("Custom format %s (%+d)", rule.name, rule.score)
				}
			case "penalty":
				br := BlockRule{ID: rule.id, Type: rule.patternType, Pattern: rule.pattern,
					MediaType: rule.mediaType, Action: rule.action, ScorePenalty: rule.penalty, Enabled: rule.enabled}
				if matchBlockRule(br, candidateTitleLower, releaseGroupLower) {
					score -= rule.penalty
					addExplanation("Release rule penalty %s:%s (-%d)", rule.patternType, rule.pattern, rule.penalty)
				}
			case "block":
				br := BlockRule{ID: rule.id, Type: rule.patternType, Pattern: rule.pattern,
					MediaType: rule.mediaType, Action: rule.action, Enabled: rule.enabled}
				if matchBlockRule(br, candidateTitleLower, releaseGroupLower) {
					explanations = append(explanations, fmt.Sprintf("Rejected by release rule %s:%s", rule.patternType, rule.pattern))
					return Result{Rejected: true, RejectReason: "blocklist:" + rule.patternType + ":" + rule.pattern, Explanations: explanations}
				}
			}
		}
	}

	if len(explanations) == 0 {
		explanations = append(explanations, "No special ranking adjustments were applied beyond the baseline candidate checks.")
	}
	return Result{
		Score:                 score,
		CustomFormatScore:     customFormatScore,
		Explanations:          explanations,
		CompatibilityWarnings: compatibilityWarnings(candidate),
	}
}

// compatibilityWarnings returns a list of informational strings for formats
// that may require transcoding or specific hardware in Plex/Jellyfin.
// These do not affect the score; they are surfaced as UI badges.
func compatibilityWarnings(candidate Candidate) []string {
	var out []string
	hdr := ParseHDRFormat(candidate.Title)
	if hdr == "DV" {
		out = append(out, "Dolby Vision — requires DV-capable client or will transcode")
	}
	audio := ParseAudioFormat(candidate.Title)
	if audio == "Atmos" || audio == "TrueHD" {
		out = append(out, "TrueHD/Atmos — requires passthrough or will transcode to lossy")
	}
	codec := strings.ToLower(candidate.Codec)
	if strings.Contains(codec, "av1") {
		out = append(out, "AV1 — limited hardware decode support on older devices")
	}
	return out
}

// ParseReleaseGroup extracts the release group from a release title using scene convention:
// the group is the text after the last "-" separator, excluding file extensions.
// Returns an empty string when no group can be parsed.
func ParseReleaseGroup(title string) string {
	name := title
	// Strip common media/archive extensions.
	for _, ext := range []string{".nzb", ".mkv", ".mp4", ".avi", ".ts", ".m2ts"} {
		if strings.HasSuffix(strings.ToLower(name), ext) {
			name = name[:len(name)-len(ext)]
			break
		}
	}
	idx := strings.LastIndex(name, "-")
	if idx < 0 || idx == len(name)-1 {
		return ""
	}
	group := name[idx+1:]
	// Groups never contain spaces or dots in standard scene naming.
	if strings.ContainsAny(group, " \t\n.") {
		return ""
	}
	return group
}

// matchBlockRule returns true when the given rule matches the (already lower-cased) title/group.
func matchBlockRule(rule BlockRule, titleLower, releaseGroupLower string) bool {
	switch rule.Type {
	case "release_group":
		patternLower := strings.ToLower(rule.Pattern)
		if releaseGroupLower != "" {
			return releaseGroupLower == patternLower
		}
		// Fallback: search whole title when group cannot be parsed.
		return strings.Contains(titleLower, patternLower)
	case "title_pattern":
		patternLower := strings.ToLower(rule.Pattern)
		// Also check with dots normalised to spaces to match scene notation
		// (e.g. pattern "AI Upscale" matches title "...ai.upscale-GROUP").
		normalised := strings.ReplaceAll(titleLower, ".", " ")
		return strings.Contains(titleLower, patternLower) || strings.Contains(normalised, patternLower)
	case "regex":
		if re, err := regexp.Compile("(?i)" + rule.Pattern); err == nil {
			return re.MatchString(titleLower)
		}
	case "missing_release_group":
		return releaseGroupLower == ""
	}
	return false
}

// BlockTestMatch is one matched rule in a TestBlockRules result.
type BlockTestMatch struct {
	RuleID  int64  `json:"ruleId"`
	Type    string `json:"type"`
	Pattern string `json:"pattern"`
	Action  string `json:"action"`
	Reason  string `json:"reason"`
}

// BlockTestResult is returned by TestBlockRules for debugging/API use.
type BlockTestResult struct {
	Allowed      bool             `json:"allowed"`
	Blocked      bool             `json:"blocked"`
	ScorePenalty int              `json:"scorePenalty"`
	MatchedRules []BlockTestMatch `json:"matchedRules"`
}

// TestBlockRules evaluates a release title against a set of rules and returns
// a detailed filter result. Used by the /api/release-block-rules/test endpoint.
func TestBlockRules(rules []BlockRule, title, mediaType string) BlockTestResult {
	titleLower := strings.ToLower(title)
	releaseGroup := ParseReleaseGroup(title)
	releaseGroupLower := strings.ToLower(releaseGroup)

	effectiveMediaType := mediaType
	if effectiveMediaType == "episode" {
		effectiveMediaType = "tv"
	}

	var result BlockTestResult
	for _, rule := range rules {
		if !rule.Enabled {
			continue
		}
		if rule.MediaType != "both" && rule.MediaType != effectiveMediaType {
			continue
		}
		if !matchBlockRule(rule, titleLower, releaseGroupLower) {
			continue
		}
		match := BlockTestMatch{
			RuleID:  rule.ID,
			Type:    rule.Type,
			Pattern: rule.Pattern,
			Action:  rule.Action,
			Reason:  fmt.Sprintf("%s matched %q", rule.Type, rule.Pattern),
		}
		result.MatchedRules = append(result.MatchedRules, match)
		if rule.Action == "block" {
			result.Blocked = true
		} else {
			result.ScorePenalty += rule.ScorePenalty
		}
	}
	result.Allowed = !result.Blocked
	return result
}

// ── Source scoring (field + title parsing) ───────────────────────────────────

// scoreSourceField checks the candidate.Source field first (indexer-populated),
// then falls back to parsing the release title for streaming-service prefixes
// (AMZN.WEB-DL, NF.WEB-DL etc.) — a gap Radarr covers via custom formats.
func scoreSourceField(source, titleLower string, prefs Preferences) int {
	effective := strings.ToLower(strings.TrimSpace(source))

	// If source is blank or generic, try to detect streaming WEB-DL from title
	if effective == "" || effective == "unknown" {
		if reStreamingService.MatchString(titleLower) {
			effective = "web-dl" // treat AMZN/NF/etc. as WEB-DL
		}
	}

	if score, ok := scoreByPreference(effective, prefs.Sources, 300, 40); ok {
		return score
	}
	switch effective {
	case "web-dl", "webrip":
		return 250
	case "bluray":
		return 225
	case "remux":
		return 210
	case "hdtv":
		return 80
	default:
		return 0
	}
}

// ── Size / resolution / codec / language ────────────────────────────────────

func rejectBySize(candidate Candidate, prefs Preferences, runtimeMinutes int) string {
	if candidate.SizeBytes <= 0 {
		return ""
	}
	sizeMB := int(candidate.SizeBytes / (1024 * 1024))
	// Profile-level and tier limits are MB/min; skip if runtime unknown.
	if runtimeMinutes > 0 {
		if prefs.MinMBPerMinute > 0 && sizeMB < prefs.MinMBPerMinute*runtimeMinutes {
			return "too_small"
		}
		if prefs.MaxMBPerMinute > 0 && sizeMB > prefs.MaxMBPerMinute*runtimeMinutes {
			return "too_large"
		}
		if len(prefs.TierMBPerMinuteLimits) > 0 && candidate.Resolution != "" {
			if lim, ok := prefs.TierMBPerMinuteLimits[candidate.Resolution]; ok {
				if lim[0] > 0 && sizeMB < lim[0]*runtimeMinutes {
					return "too_small"
				}
				if lim[1] > 0 && sizeMB > lim[1]*runtimeMinutes {
					return "too_large"
				}
			}
		}
	}
	return ""
}

func scoreResolution(resolution string, prefs Preferences) int {
	if score, ok := scoreByPreference(resolution, prefs.Resolutions, 500, 75); ok {
		return score
	}
	switch resolution {
	case "2160p":
		return 450
	case "1080p":
		return 400
	case "720p":
		return 250
	default:
		return 0
	}
}

func scoreCodec(codec string, prefs Preferences) int {
	if score, ok := scoreByPreference(codec, prefs.Codecs, 180, 30); ok {
		return score
	}
	switch strings.ToLower(codec) {
	case "h265", "x265":
		return 150
	case "h264", "x264":
		return 120
	default:
		return 0
	}
}

func scoreLanguage(language string, prefs Preferences) int {
	if score, ok := scoreByPreference(language, prefs.Languages, 120, 20); ok {
		return score
	}
	switch strings.ToLower(language) {
	case "nl":
		return 100
	case "en":
		return 90
	case "multi":
		return 40
	case "unknown":
		return 10
	default:
		return -80
	}
}

func rejectLanguageMismatch(language string, prefs Preferences) bool {
	if len(prefs.Languages) == 0 {
		return false
	}
	normalized := strings.ToLower(strings.TrimSpace(language))
	switch normalized {
	case "", "unknown", "multi":
		return false
	}
	for _, preferred := range prefs.Languages {
		if strings.EqualFold(strings.TrimSpace(preferred), normalized) {
			return false
		}
	}
	return true
}

func scoreByPreference(value string, ordered []string, base int, step int) (int, bool) {
	if len(ordered) == 0 {
		return 0, false
	}
	normalizedValue := strings.ToLower(strings.TrimSpace(value))
	for idx, item := range ordered {
		if strings.EqualFold(strings.TrimSpace(item), normalizedValue) {
			score := base - (idx * step)
			if score < step {
				score = step
			}
			return score, true
		}
	}
	return -120, true
}

// ── CAM/TS/Screener rejection ────────────────────────────────────────────────

// hasRejectedSource matches all CAM/TS/Screener variants from Radarr's
// QualityParser, including separator-required tokens like "TS." and "HD-TS".
func hasRejectedSource(title string) bool {
	// Normalise-and-space approach for tokens that appear as whole words
	normalized := " " + normalizeText(title) + " "
	wordTokens := []string{
		" cam ", " camrip ", " hdcam ", " hqcam ", " newcam ",
		" telesync ", " ts ", " tscam ",
		" telecine ", " tc ", " hdtc ",
		" workprint ", " wp ",
		" screener ", " scr ", " dvdscr ", " dvdscreener ",
		" pdvd ",
	}
	for _, tok := range wordTokens {
		if strings.Contains(normalized, tok) {
			return true
		}
	}
	// Separator-required raw-title check (not normalised, to catch "HD-TS", "TS.1080p")
	rawLower := strings.ToLower(title)
	separatorTokens := []string{
		"hd-ts", "hdts", "hd-cam", "hdcamrip",
		"hd-tc",
		"tsrip", "hdtsrip",
	}
	for _, tok := range separatorTokens {
		if strings.Contains(rawLower, tok) {
			return true
		}
	}
	return false
}

// ── Year / episode matching ──────────────────────────────────────────────────

type yearMatch int

const (
	yearUnknown yearMatch = iota
	yearExact
	yearMismatch
)

type episodeMatch int

const (
	episodeUnknown episodeMatch = iota
	episodeExact
	episodeSeasonPack
	episodeMismatch
)

// containsNormalized checks whether the required title words appear at the
// start of the candidate title words (within 1-word franchise-prefix tolerance).
// Word-prefix matching prevents substring false positives like "Lost" matching
// "Secrets.of.The.Lost.Liners", while still allowing "DCs.Legends.of.Tomorrow"
// to match "DC's Legends of Tomorrow" (one franchise-prefix word difference).
// Leading articles ("the", "a", "an") are stripped and retried from both sides.
func containsNormalized(title, required string) bool {
	cWords := strings.Fields(normalizeText(title))
	rWords := strings.Fields(normalizeText(required))
	if titlesWordMatch(cWords, rWords) {
		return true
	}
	// Strip leading article from candidate and retry
	if len(cWords) > 0 && isLeadingArticle(cWords[0]) {
		if titlesWordMatch(cWords[1:], rWords) {
			return true
		}
	}
	// Strip leading article from required title and retry (both with and without candidate article)
	if len(rWords) > 0 && isLeadingArticle(rWords[0]) {
		stripped := rWords[1:]
		if titlesWordMatch(cWords, stripped) {
			return true
		}
		if len(cWords) > 0 && isLeadingArticle(cWords[0]) {
			return titlesWordMatch(cWords[1:], stripped)
		}
	}
	return false
}

// titlesWordMatch returns true if rWords appear at the beginning of cWords,
// allowing up to 1 extra prefix word in cWords (e.g. "Marvels" before "Agents").
func titlesWordMatch(cWords, rWords []string) bool {
	if len(rWords) == 0 {
		return true
	}
	for offset := 0; offset < 2; offset++ {
		if offset+len(rWords) > len(cWords) {
			break
		}
		ok := true
		for i, rw := range rWords {
			if cWords[offset+i] != rw {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func isLeadingArticle(word string) bool {
	return word == "the" || word == "a" || word == "an"
}

func normalizeText(value string) string {
	value = strings.ReplaceAll(value, "’", "") // right single quote
	value = strings.ReplaceAll(value, "‘", "") // left single quote
	value = strings.ReplaceAll(value, "'", "")
	value = strings.ReplaceAll(value, "!", "")
	value = strings.ReplaceAll(value, "?", "")
	value = strings.ReplaceAll(value, " & ", " and ")
	value = strings.ReplaceAll(value, "&", " and ")
	replacer := strings.NewReplacer(
		".", " ", "_", " ", "-", " ",
		"[", " ", "]", " ",
		"(", " ", ")", " ",
		":", " ", ";", " ", ",", " ",
	)
	return strings.Join(strings.Fields(strings.ToLower(replacer.Replace(value))), " ")
}

func matchYear(title string, requiredYear int) yearMatch {
	if requiredYear <= 0 {
		return yearUnknown
	}
	// Scan ALL 4-digit year tokens before declaring a mismatch. Returning
	// mismatch on the first token was wrong for titles that start with a
	// year (e.g. "2001: A Space Odyssey" or "1917"), where the title year
	// would be checked before the release year further in the string.
	//
	// If the only non-matching year token is the first token, it is most
	// likely the movie title itself (e.g. "1917", "2001", "2012") rather than
	// a release year stamp — return unknown rather than hard-reject.
	tokens := strings.Fields(normalizeText(title))
	var nonMatchPositions []int
	for i, token := range tokens {
		if len(token) != 4 {
			continue
		}
		year, err := strconv.Atoi(token)
		if err != nil || year < 1900 || year > 2100 {
			continue
		}
		if year == requiredYear {
			return yearExact
		}
		nonMatchPositions = append(nonMatchPositions, i)
	}
	if len(nonMatchPositions) == 0 {
		return yearUnknown
	}
	if len(nonMatchPositions) == 1 && nonMatchPositions[0] == 0 {
		return yearUnknown
	}
	return yearMismatch
}

func matchEpisode(title string, seasonNumber, episodeNumber int) episodeMatch {
	if seasonNumber <= 0 || episodeNumber <= 0 {
		return episodeUnknown
	}
	title = strings.ToLower(title)
	exactTokens := []string{
		fmt.Sprintf("s%02de%02d", seasonNumber, episodeNumber),
		fmt.Sprintf("%dx%02d", seasonNumber, episodeNumber),
		fmt.Sprintf("%d x %02d", seasonNumber, episodeNumber),
	}
	for _, token := range exactTokens {
		if strings.Contains(title, token) {
			return episodeExact
		}
	}
	seasonTokens := []string{
		fmt.Sprintf("season %d", seasonNumber),
		fmt.Sprintf("s%02d", seasonNumber),
	}
	for _, token := range seasonTokens {
		if strings.Contains(title, token) {
			// Bare season token with no SxxExx following it → season pack.
			if !containsEpisodeToken(title) {
				return episodeSeasonPack
			}
			if strings.Contains(title, "complete") || strings.Contains(title, "pack") {
				return episodeSeasonPack
			}
		}
	}
	if containsEpisodeToken(title) {
		return episodeMismatch
	}
	return episodeUnknown
}

func containsEpisodeToken(title string) bool {
	title = strings.ToLower(title)
	for season := 1; season <= 40; season++ {
		for episode := 1; episode <= 99; episode++ {
			if strings.Contains(title, fmt.Sprintf("s%02de%02d", season, episode)) {
				return true
			}
			if strings.Contains(title, fmt.Sprintf("%dx%02d", season, episode)) {
				return true
			}
		}
	}
	return false
}

// ── Audio / HDR parsing (regex-based, Radarr custom-format style) ───────────

// ParseAudioFormat extracts the best audio format using compiled regexes
// matching Radarr's custom-format MediaInfoFormatter patterns.
func ParseAudioFormat(title string) string {
	switch {
	case reTrueHD.MatchString(title):
		if reAtmos.MatchString(title) {
			return "Atmos"
		}
		return "TrueHD"
	case reDTSHD.MatchString(title):
		return "DTS-HD"
	case reDTS.MatchString(title):
		return "DTS"
	case reDD.MatchString(title):
		if reAtmos.MatchString(title) {
			return "Atmos"
		}
		return "DD+"
	case reAC3.MatchString(title):
		return "AC3"
	case reAAC.MatchString(title):
		return "AAC"
	case reFLAC.MatchString(title):
		return "FLAC"
	default:
		return ""
	}
}

// ParseHDRFormat extracts the HDR tier using regex patterns matching
// Radarr/Sonarr custom-format DV/HDR10+/HDR10/HLG specs.
// Priority: DV > HDR10+ > HDR10 > HLG > SDR (trash-guides hierarchy).
func ParseHDRFormat(title string) string {
	switch {
	case reDV.MatchString(title):
		return "DV"
	case reHDR10Plus.MatchString(title):
		return "HDR10+"
	case reHDR10.MatchString(title):
		return "HDR10"
	case reHLG.MatchString(title):
		return "HLG"
	default:
		return "SDR"
	}
}

func scoreAudio(audio string, prefs Preferences) int {
	if audio == "" {
		return 0
	}
	if score, ok := scoreByPreference(audio, prefs.AudioFormats, 200, 25); ok {
		return score
	}
	// Built-in tier (trash-guides audio priority)
	switch audio {
	case "Atmos":
		return 180
	case "TrueHD":
		return 160
	case "DTS-HD":
		return 140
	case "DTS":
		return 100
	case "DD+":
		return 80
	case "AC3":
		return 60
	case "FLAC":
		return 55
	case "AAC":
		return 40
	default:
		return 0
	}
}

func scoreHDR(hdr string, prefs Preferences) int {
	if hdr == "" || hdr == "SDR" {
		if len(prefs.HdrFormats) > 0 {
			if score, ok := scoreByPreference("SDR", prefs.HdrFormats, 160, 20); ok {
				return score
			}
		}
		return 0
	}
	if score, ok := scoreByPreference(hdr, prefs.HdrFormats, 160, 20); ok {
		return score
	}
	// Built-in tier (trash-guides: DV > HDR10+ > HDR10 > HLG)
	switch hdr {
	case "DV":
		return 140
	case "HDR10+":
		return 120
	case "HDR10":
		return 100
	case "HLG":
		return 60
	default:
		return 0
	}
}
