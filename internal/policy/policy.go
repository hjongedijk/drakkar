// Package policy maps queue failure reasons to recovery actions, mirroring the
// reference implementation's policyService.ts decision matrix.
package policy

import "strings"

// FailureKey is a canonical category for a queue failure.
type FailureKey string

const (
	KeyMissingArticles        FailureKey = "missing_articles"
	KeyUnsupportedArchive     FailureKey = "unsupported_archive"
	KeyEncryptedArchive       FailureKey = "encrypted_archive"
	KeyNZBParseFailed         FailureKey = "nzb_parse_failed"
	KeyNZBFetchFailed         FailureKey = "nzb_fetch_failed"
	KeyNZBFetch4xx            FailureKey = "nzb_fetch_4xx"    // 401/404/410/451 = permanent auth/not-found, blocklist
	KeyNZBFetch403            FailureKey = "nzb_fetch_403"    // 403 = quota/rate-limit (e.g. NZBFinder), retry later
	KeyNNTPThrottled          FailureKey = "nntp_throttled"   // NNTP 430 = connection/transfer limit, retry later
	KeyPreflightFailed        FailureKey = "preflight_failed"
	KeyPublishFailed          FailureKey = "publish_failed"
	KeySymlinkFailed          FailureKey = "symlink_failed"
	KeyNoReleaseFound         FailureKey = "no_release_found"
	KeyAllCandidatesRejected  FailureKey = "all_candidates_rejected"
	KeyBadSource              FailureKey = "bad_source"
	KeyWrongTitle             FailureKey = "wrong_title"
	KeyInterruptedByRestart   FailureKey = "interrupted_by_restart"
	KeyUnknown                FailureKey = "unknown"
)

// Action is the recovery action to take after a failure.
type Action string

const (
	// ActionSearchAgain discards the current selected release and triggers a
	// fresh Hydra search for a replacement candidate.
	ActionSearchAgain Action = "search_again"

	// ActionBlocklistAndSearch blocklists the current release URL so it will
	// never be selected again, then searches for a replacement.
	ActionBlocklistAndSearch Action = "blocklist_and_search"

	// ActionRetryLater re-queues the item for retry after a delay (e.g. for
	// transient network errors where the same release might work later).
	ActionRetryLater Action = "retry_later"

	// ActionDoNothing leaves the item as failed. The monitoring pass will
	// retry it on the next cycle (for hard-unknown failures).
	ActionDoNothing Action = "do_nothing"
)

// defaultMatrix maps each failure key to its recovery action.
var defaultMatrix = map[FailureKey]Action{
	KeyMissingArticles:       ActionBlocklistAndSearch, // article expired → block release, find another
	KeyUnsupportedArchive:    ActionBlocklistAndSearch, // solid/encrypted RAR → block, search again
	KeyEncryptedArchive:      ActionBlocklistAndSearch,
	KeyNZBParseFailed:        ActionBlocklistAndSearch, // bad NZB XML → block URL, search again
	KeyNZBFetchFailed:        ActionBlocklistAndSearch, // fetch failed → blocklist this URL, find another
	KeyNZBFetch4xx:           ActionBlocklistAndSearch, // 401/404/410/451 = permanent error, blocklist immediately
	KeyNZBFetch403:           ActionRetryLater,         // 403 = quota exhausted (NZBFinder etc.), retry when quota resets
	KeyNNTPThrottled:         ActionRetryLater,         // NNTP 430 = transient connection limit, article still valid
	KeyPreflightFailed:       ActionSearchAgain,        // transient preflight → try another release
	KeyPublishFailed:         ActionRetryLater,         // FUSE publish issue → retry, don't abandon
	KeySymlinkFailed:         ActionRetryLater,
	KeyNoReleaseFound:        ActionDoNothing,          // nothing on indexers → wait for next cycle
	KeyAllCandidatesRejected: ActionSearchAgain,
	KeyBadSource:             ActionBlocklistAndSearch,
	KeyWrongTitle:            ActionSearchAgain, // bad title match → retry search, don't blocklist valid NZBs
	KeyInterruptedByRestart:  ActionSearchAgain, // was mid-fetch → retry same flow
	KeyUnknown:               ActionDoNothing,
}

// Classify maps a raw failure reason string to a FailureKey.
// The reason string comes from queue_items.failure_reason in the database.
func Classify(reason string) FailureKey {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch {
	// NNTP 430 = "Transfer Error" / connection limit exceeded — transient throttle,
	// the article still exists. Must be checked before the nntp_article_unavailable
	// catch-all so a throttled download is retried rather than blocklisted.
	case strings.Contains(r, "status 430") ||
		strings.Contains(r, "body status 430"):
		return KeyNNTPThrottled

	case strings.Contains(r, "article not found") ||
		strings.Contains(r, "nntp_article_unavailable") ||
		strings.Contains(r, "missing article"):
		return KeyMissingArticles

	case strings.Contains(r, "unsupported_archive") ||
		strings.Contains(r, "archive_compression") ||
		strings.Contains(r, "archive_solid") ||
		strings.Contains(r, "solid"):
		return KeyUnsupportedArchive

	case strings.Contains(r, "archive_encrypted") ||
		strings.Contains(r, "encrypted"):
		return KeyEncryptedArchive

	case strings.Contains(r, "nzb_parse") ||
		strings.Contains(r, "parse nzb") ||
		strings.Contains(r, "indexer error"):
		return KeyNZBParseFailed

	// 403 = quota exhausted (common with NZBFinder) — retry when quota resets.
	case strings.Contains(r, "status 403"):
		return KeyNZBFetch403

	// Other 4xx HTTP errors are permanent — blocklist immediately.
	case strings.Contains(r, "status 404") ||
		strings.Contains(r, "status 410") ||
		strings.Contains(r, "status 401") ||
		strings.Contains(r, "status 451"):
		return KeyNZBFetch4xx

	case strings.Contains(r, "nzb_fetch") ||
		strings.Contains(r, "nzb fetch") ||
		strings.Contains(r, "fetch status"):
		return KeyNZBFetchFailed

	case strings.Contains(r, "preflight"):
		return KeyPreflightFailed

	case strings.Contains(r, "publish_failed") ||
		strings.Contains(r, "no publishable"):
		return KeyPublishFailed

	case strings.Contains(r, "symlink"):
		return KeySymlinkFailed

	case strings.Contains(r, "no_release_found") ||
		strings.Contains(r, "no releases") ||
		strings.Contains(r, "no_releases"):
		return KeyNoReleaseFound

	// "all_candidates_bad_source" must be checked before "all_candidates" and
	// "bad_source" — it contains both as substrings and needs its own routing.
	// "all_candidates" must be checked before "wrong_title" for the same reason.
	case strings.Contains(r, "all_candidates_bad_source"):
		return KeyBadSource

	case strings.Contains(r, "all_candidates"):
		return KeyAllCandidatesRejected

	case strings.Contains(r, "wrong_title") ||
		strings.Contains(r, "wrong title"):
		return KeyWrongTitle

	case strings.Contains(r, "bad_source"):
		return KeyBadSource

	case strings.Contains(r, "interrupted_by_restart") ||
		strings.Contains(r, "stale_worker") ||
		strings.Contains(r, "too_many_failures"):
		return KeyInterruptedByRestart

	default:
		return KeyUnknown
	}
}

// Decide returns the recovery Action for a given FailureKey.
func Decide(key FailureKey) Action {
	if action, ok := defaultMatrix[key]; ok {
		return action
	}
	return ActionDoNothing
}

// DecideFromReason is a convenience wrapper: Decide(Classify(reason)).
func DecideFromReason(reason string) Action {
	return Decide(Classify(reason))
}
