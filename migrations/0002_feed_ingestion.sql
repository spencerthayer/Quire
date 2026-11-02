-- Quire Phase 2: RSS/Atom Ingestion, Article Extraction, and Scheduling
-- Preserves all Phase 1 tables and rows while adding feed scheduling metadata,
-- execution history, cross-feed article associations, and persistent SQLite
-- extraction queue state.

-- 1. Extend `feeds` with conditional request headers, scheduling intervals,
--    per-feed limits, and execution diagnostics.
ALTER TABLE feeds ADD COLUMN etag TEXT;
ALTER TABLE feeds ADD COLUMN last_modified TEXT;
ALTER TABLE feeds ADD COLUMN last_fetch_status TEXT NOT NULL DEFAULT 'idle' CHECK (
    last_fetch_status IN ('idle', 'queued', 'running', 'success', 'unchanged', 'partial', 'failed', 'cancelled')
);
ALTER TABLE feeds ADD COLUMN last_http_status INTEGER;
ALTER TABLE feeds ADD COLUMN last_fetch_duration_ms INTEGER;
ALTER TABLE feeds ADD COLUMN last_discovered_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE feeds ADD COLUMN last_inserted_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE feeds ADD COLUMN last_extraction_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE feeds ADD COLUMN consecutive_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE feeds ADD COLUMN next_scheduled_fetch_at TEXT;
ALTER TABLE feeds ADD COLUMN fetch_interval_minutes INTEGER CHECK (
    fetch_interval_minutes IS NULL OR (fetch_interval_minutes >= 5 AND fetch_interval_minutes <= 10080)
);
ALTER TABLE feeds ADD COLUMN max_entries_override INTEGER CHECK (
    max_entries_override IS NULL OR (max_entries_override >= 1 AND max_entries_override <= 5000)
);
ALTER TABLE feeds ADD COLUMN fetch_in_progress INTEGER NOT NULL DEFAULT 0 CHECK (fetch_in_progress IN (0, 1));
ALTER TABLE feeds ADD COLUMN feed_language TEXT;
ALTER TABLE feeds ADD COLUMN feed_updated_at TEXT;

CREATE INDEX IF NOT EXISTS idx_feeds_next_scheduled
    ON feeds(enabled, fetch_in_progress, next_scheduled_fetch_at);

-- 2. Add extraction metadata columns to `articles` additively so existing
--    `articles` rows and `edition_articles` foreign keys are never dropped.
ALTER TABLE articles ADD COLUMN extraction_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE articles ADD COLUMN last_extraction_attempt_at TEXT;
ALTER TABLE articles ADD COLUMN last_successful_extraction_at TEXT;
ALTER TABLE articles ADD COLUMN extraction_error TEXT;
ALTER TABLE articles ADD COLUMN next_extraction_retry_at TEXT;
ALTER TABLE articles ADD COLUMN extracted_content_type TEXT NOT NULL DEFAULT 'html' CHECK (
    extracted_content_type IN ('html', 'summary_fallback', 'none')
);
ALTER TABLE articles ADD COLUMN original_source_url TEXT;
ALTER TABLE articles ADD COLUMN entry_updated_at TEXT;

CREATE INDEX IF NOT EXISTS idx_articles_extraction_retry
    ON articles(extraction_status, next_extraction_retry_at);

-- 3. Cross-feed article sources table so multiple feeds referencing the same
--    canonical article are recorded without duplicating article content, and so
--    deleting one feed can reassign primary ownership if another feed still
--    references the article.
CREATE TABLE IF NOT EXISTS article_feed_sources (
    article_id INTEGER NOT NULL,
    feed_id INTEGER NOT NULL,
    external_guid TEXT,
    discovered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (article_id, feed_id),
    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE,
    FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_article_feed_sources_feed_id
    ON article_feed_sources(feed_id, discovered_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_article_feed_sources_feed_guid
    ON article_feed_sources(feed_id, external_guid)
    WHERE external_guid IS NOT NULL AND external_guid != '';

-- Backfill existing Phase 1 articles into article_feed_sources
INSERT OR IGNORE INTO article_feed_sources (article_id, feed_id, external_guid, discovered_at, last_seen_at)
SELECT id, feed_id, external_guid, discovered_at, updated_at
FROM articles;

-- 4. Feed execution history (`feed_fetch_runs`)
CREATE TABLE IF NOT EXISTS feed_fetch_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_id INTEGER NOT NULL,
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('manual', 'scheduled')),
    status TEXT NOT NULL CHECK (
        status IN ('running', 'success', 'unchanged', 'partial', 'failed', 'cancelled')
    ),
    started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    completed_at TEXT,
    duration_ms INTEGER,
    http_status INTEGER,
    entries_seen INTEGER NOT NULL DEFAULT 0,
    entries_skipped INTEGER NOT NULL DEFAULT 0,
    entries_truncated INTEGER NOT NULL DEFAULT 0,
    articles_inserted INTEGER NOT NULL DEFAULT 0,
    articles_updated INTEGER NOT NULL DEFAULT 0,
    articles_unchanged INTEGER NOT NULL DEFAULT 0,
    extraction_queued INTEGER NOT NULL DEFAULT 0,
    error_category TEXT,
    error_message TEXT,
    FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_feed_fetch_runs_feed_started
    ON feed_fetch_runs(feed_id, started_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_feed_fetch_runs_status
    ON feed_fetch_runs(status, started_at DESC);

-- 5. Persistent SQLite extraction queue (`article_extraction_jobs`)
--    Enforces at most one job row per article (`UNIQUE(article_id)`) so an article
--    cannot accumulate duplicate active extraction jobs.
CREATE TABLE IF NOT EXISTS article_extraction_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id INTEGER NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'queued' CHECK (
        status IN ('queued', 'processing', 'completed', 'failed', 'cancelled')
    ),
    trigger_type TEXT NOT NULL DEFAULT 'auto' CHECK (
        trigger_type IN ('auto', 'manual', 'retry')
    ),
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    next_eligible_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    claimed_at TEXT,
    completed_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_extraction_jobs_eligible
    ON article_extraction_jobs(status, next_eligible_at ASC, id ASC);
