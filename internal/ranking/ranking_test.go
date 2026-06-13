package ranking

import (
	"testing"
	"time"
)

func TestScoreRejectsWrongTitle(t *testing.T) {
	result := Score(Candidate{Title: "Other.Movie.2021.1080p"}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021})
	if !result.Rejected || result.RejectReason != "wrong_title" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestScorePreferredCandidate(t *testing.T) {
	result := Score(Candidate{
		Title:        "Dune.2021.1080p.WEB-DL",
		Resolution:   "1080p",
		Source:       "WEB-DL",
		Codec:        "x265",
		Language:     "en",
		Indexer:      "hydra",
		ReleaseGroup: "GROUP",
		UploadedAt:   time.Now(),
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021})
	if result.Rejected || result.Score <= 0 {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestScoreRejectsWrongMovieYear(t *testing.T) {
	result := Score(Candidate{
		Title:      "Dune.1984.1080p.BluRay",
		Resolution: "1080p",
		Source:     "bluray",
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021})
	if !result.Rejected || result.RejectReason != "wrong_year" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestScoreRejectsBadSource(t *testing.T) {
	result := Score(Candidate{
		Title:      "Dune.2021.1080p.CAM",
		Resolution: "1080p",
		Source:     "cam",
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021})
	if !result.Rejected || result.RejectReason != "bad_source" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestScorePrefersExactEpisodeOverSeasonPack(t *testing.T) {
	exact := Score(Candidate{
		Title:      "Loki.S01E02.1080p.WEB-DL",
		Resolution: "1080p",
		Source:     "web-dl",
		Language:   "en",
		UploadedAt: time.Now(),
	}, Requirements{Title: "Loki", MediaType: "episode", Year: 2021, SeasonNumber: 1, EpisodeNumber: 2})
	pack := Score(Candidate{
		Title:      "Loki.Season.1.Complete.1080p.WEB-DL",
		Resolution: "1080p",
		Source:     "web-dl",
		Language:   "en",
		UploadedAt: time.Now(),
	}, Requirements{Title: "Loki", MediaType: "episode", Year: 2021, SeasonNumber: 1, EpisodeNumber: 2})
	if exact.Rejected || pack.Rejected {
		t.Fatalf("unexpected reject exact=%+v pack=%+v", exact, pack)
	}
	if exact.Score <= pack.Score {
		t.Fatalf("expected exact episode score > season pack score, got exact=%d pack=%d", exact.Score, pack.Score)
	}
}

func TestScoreRejectsWrongEpisode(t *testing.T) {
	result := Score(Candidate{
		Title:      "Loki.S01E03.1080p.WEB-DL",
		Resolution: "1080p",
		Source:     "web-dl",
	}, Requirements{Title: "Loki", MediaType: "episode", Year: 2021, SeasonNumber: 1, EpisodeNumber: 2})
	if !result.Rejected || result.RejectReason != "wrong_episode" {
		t.Fatalf("unexpected result %+v", result)
	}
}

func TestScoreWithPreferencesUsesOrderedResolution(t *testing.T) {
	prefs := Preferences{Resolutions: []string{"720p", "1080p"}}
	low := ScoreWithPreferences(Candidate{
		Title:      "Dune.2021.1080p.WEB-DL",
		Resolution: "1080p",
		Source:     "web-dl",
		Language:   "en",
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021}, prefs)
	high := ScoreWithPreferences(Candidate{
		Title:      "Dune.2021.720p.WEB-DL",
		Resolution: "720p",
		Source:     "web-dl",
		Language:   "en",
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021}, prefs)
	if high.Score <= low.Score {
		t.Fatalf("expected preferred resolution to win, got 720p=%d 1080p=%d", high.Score, low.Score)
	}
}

func TestScoreWithPreferencesRejectsTooLarge(t *testing.T) {
	result := ScoreWithPreferences(Candidate{
		Title:      "Dune.2021.1080p.WEB-DL",
		SizeBytes:  8 * 1024 * 1024 * 1024,
		Resolution: "1080p",
		Source:     "web-dl",
		Language:   "en",
	}, Requirements{Title: "Dune", MediaType: "movie", Year: 2021}, Preferences{MaxSizeMB: 4000})
	if !result.Rejected || result.RejectReason != "too_large" {
		t.Fatalf("unexpected result %+v", result)
	}
}

// TestTitleMatchRegressions guards against past false-positive and false-negative
// title matches that caused wrong content to be downloaded.
func TestTitleMatchRegressions(t *testing.T) {
	cases := []struct {
		name      string
		candidate string
		required  string
		wantMatch bool
	}{
		// Reno.911 must NOT match 9-1-1 (TVDB ID-based search bypassed check)
		{"reno911-vs-911", "Reno.911.2003.S01E01.720p.x265.10bit-vrc", "9-1-1", false},
		// 9-1-1 correct releases must still match
		{"911-correct", "9-1-1.S01E01.720p.WEB-DL", "9-1-1", true},
		// Secrets.of.The.Lost.Liners must NOT match Lost
		{"lost-liners-vs-lost", "Secrets.of.The.Lost.Liners.Series.1.5of6.1080p.WEB.x264.AAC-MVGroup", "Lost", false},
		// Lost correct releases must match
		{"lost-correct", "Lost.S01E01.1080p.BluRay", "Lost", true},
		{"lost-with-the", "The.Lost.S01E01.720p", "Lost", true},
		// DCs prefix allowed (1-word franchise tolerance)
		{"dcs-legends", "DCs.Legends.of.Tomorrow.2016.S06.1080p", "DC's Legends of Tomorrow", true},
		// Marvels prefix allowed
		{"marvels-agents", "Marvels.Agents.of.S.H.I.E.L.D.S01E01.1080p", "Agents of S.H.I.E.L.D.", true},
		// Leading "The" stripped from candidate
		{"the-batman", "The.Batman.2022.1080p.BluRay", "Batman", true},
		// Leading "The" stripped from required
		{"batman-no-the", "Batman.2022.1080p.BluRay", "The Batman", true},
		// Anime release should not match 9-1-1
		{"anime-vs-911", "[SubsPlease] Kamiina Botan, Yoeru Sugata wa Yuri no Hana - 10 (1080p) [7A9116B7]", "9-1-1", false},
		// 9-1-1: Lone Star correct release
		{"lone-star-correct", "9-1-1.Lone.Star.S04E18.1080p.WEB-DL", "9-1-1: Lone Star", true},
		// TrustSource=true with structured release still applies title check
		{"trust-source-reno911", "Reno.911.2003.S01E01.720p", "9-1-1", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Test via Score so the full pipeline (including TrustSource) is exercised
			// For cases testing TrustSource, use TrustSource=true to simulate ID-based search
			req := Requirements{Title: tc.required, MediaType: "episode", TrustSource: true}
			result := Score(Candidate{Title: tc.candidate}, req)
			isMatch := result.RejectReason != "wrong_title"
			if isMatch != tc.wantMatch {
				t.Fatalf("title=%q required=%q: wantMatch=%v got rejected=%v reason=%q score=%d",
					tc.candidate, tc.required, tc.wantMatch, result.Rejected, result.RejectReason, result.Score)
			}
		})
	}
}
