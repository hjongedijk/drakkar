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

// ── Compiled regexes (Radarr/Sonarr QualityParser.cs patterns) ─────────────

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
}

type Preferences struct {
	Resolutions  []string
	Sources      []string
	Codecs       []string
	Languages    []string
	AudioFormats []string
	HdrFormats   []string
	PreferProper bool
	PreferRepack bool
	RejectCam    bool
	MinSizeMB    int
	MaxSizeMB    int
}

type Result struct {
	Score        int
	Rejected     bool
	RejectReason string
}

// ── Scoring entry points ─────────────────────────────────────────────────────

func Score(candidate Candidate, required Requirements) Result {
	return ScoreWithPreferences(candidate, required, Preferences{})
}

func ScoreWithPreferences(candidate Candidate, required Requirements, prefs Preferences) Result {
	titleLower := strings.ToLower(candidate.Title)
	requiredLower := strings.ToLower(required.Title)

	if !required.TrustSource {
		matched := containsNormalized(titleLower, requiredLower)
		for i := 0; i < len(required.AlternateTitles) && !matched; i++ {
			matched = containsNormalized(titleLower, strings.ToLower(required.AlternateTitles[i]))
		}
		if !matched {
			return Result{Rejected: true, RejectReason: "wrong_title"}
		}
	}

	// ── Hard rejections ──────────────────────────────────────────────────────

	// CAM/TS/Screener — Radarr QualityParser extended set
	if hasRejectedSource(titleLower) {
		return Result{Rejected: true, RejectReason: "bad_source"}
	}
	// Unencoded Blu-ray disc (BD-ISO, BDMV, COMPLETE.BLURAY) — always reject
	if reBRDisk.MatchString(candidate.Title) {
		return Result{Rejected: true, RejectReason: "br_disk"}
	}
	// Hardcoded/burned subs
	if reHardSubs.MatchString(candidate.Title) {
		return Result{Rejected: true, RejectReason: "hardsub"}
	}
	if sizeReject := rejectBySize(candidate, prefs); sizeReject != "" {
		return Result{Rejected: true, RejectReason: sizeReject}
	}

	// ── Year / episode match ─────────────────────────────────────────────────

	score := 0
	switch required.MediaType {
	case "movie":
		switch matchYear(titleLower, required.Year) {
		case yearMismatch:
			return Result{Rejected: true, RejectReason: "wrong_year"}
		case yearExact:
			score += 90
		}
	case "episode":
		switch matchEpisode(titleLower, required.SeasonNumber, required.EpisodeNumber) {
		case episodeMismatch:
			return Result{Rejected: true, RejectReason: "wrong_episode"}
		case episodeExact:
			score += 350
		case episodeSeasonPack:
			score += 120
		}
		switch matchYear(titleLower, required.Year) {
		case yearExact:
			score += 30
		case yearMismatch:
			score -= 40
		}
	}

	// ── Quality scoring ──────────────────────────────────────────────────────

	score += scoreResolution(candidate.Resolution, prefs)
	score += scoreSourceField(candidate.Source, titleLower, prefs)
	score += scoreCodec(candidate.Codec, prefs)
	score += scoreLanguage(candidate.Language, prefs)

	audio := ParseAudioFormat(candidate.Title)
	score += scoreAudio(audio, prefs)

	hdr := ParseHDRFormat(candidate.Title)
	score += scoreHDR(hdr, prefs)

	// ── Release quality signals ───────────────────────────────────────────────

	// Remux — Radarr pattern: BD.Remux, UHD.Remux, Hybrid-Remux
	if reRemux.MatchString(candidate.Title) {
		score += 40
	}

	// Proper/Repack — Radarr uses \bproper\b, \brepack\d?\b, \brerip\d?\b
	isProper := reProper.MatchString(candidate.Title)
	isRepack := reRepack.MatchString(candidate.Title)
	isReal   := reReal.MatchString(candidate.Title)
	if (isProper || isReal) && prefs.PreferProper {
		score += 80
	} else if isProper || isReal {
		score += 40
	}
	if isRepack && prefs.PreferRepack {
		score += 60
	} else if isRepack {
		score += 20
	}

	if candidate.Indexer != "" {
		score += 75
	}
	if candidate.ReleaseGroup != "" {
		score += 50
	}

	// Upload recency — trash-guides: prefer recent uploads
	if candidate.UploadedAt.After(time.Now().Add(-30 * 24 * time.Hour)) {
		score += 25
	}

	// Sample penalty — word-boundary aware
	if reSample.MatchString(candidate.Title) {
		score -= 150
	}

	// Failure penalties
	if candidate.Degraded {
		score -= 300
	} else if candidate.FailureCount >= 5 {
		score -= 50000 // effectively excluded
	} else if candidate.FailureCount > 0 {
		score -= 300 * candidate.FailureCount
	}

	return Result{Score: score}
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

func rejectBySize(candidate Candidate, prefs Preferences) string {
	if candidate.SizeBytes <= 0 {
		return ""
	}
	sizeMB := int(candidate.SizeBytes / (1024 * 1024))
	if prefs.MinSizeMB > 0 && sizeMB < prefs.MinSizeMB {
		return "too_small"
	}
	if prefs.MaxSizeMB > 0 && sizeMB > prefs.MaxSizeMB {
		return "too_large"
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

func containsNormalized(title, required string) bool {
	n := normalizeText(title)
	r := normalizeText(required)
	if strings.Contains(n, r) {
		return true
	}
	// Radarr strips leading articles ("the", "a", "an") from both sides.
	// Many releases drop the article that appears in the metadata title (or
	// vice versa), e.g. "Batman 2022" vs required "The Batman". Re-try with
	// the leading article removed so these aren't wrongly rejected.
	return strings.Contains(stripLeadingArticle(n), stripLeadingArticle(r))
}

func stripLeadingArticle(text string) string {
	for _, art := range []string{"the ", "a ", "an "} {
		if strings.HasPrefix(text, art) {
			return text[len(art):]
		}
	}
	return text
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
	tokens := strings.Fields(normalizeText(title))
	foundNonMatch := false
	for _, token := range tokens {
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
		foundNonMatch = true
	}
	if foundNonMatch {
		return yearMismatch
	}
	return yearUnknown
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
		if strings.Contains(title, token) && (strings.Contains(title, "complete") || strings.Contains(title, "pack")) {
			return episodeSeasonPack
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
		if strings.Contains(title, fmt.Sprintf("s%02d", season)) {
			return true
		}
		for episode := 1; episode <= 99; episode++ {
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
