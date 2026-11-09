-- Quire Phase 2 Remediation Migration 0004:
-- Rebuild `articles` following SQLite's 12-step procedure so that:
--   1. `articles.feed_id` is nullable with `ON DELETE SET NULL` (QUA-007: articles
--      referenced by editions survive when their source feed is deleted).
--   2. `extracted_content_type` defaults to `'none'` instead of `'html'` (QUA-023).
--   3. `idx_articles_feed_external_guid` is dropped so `article_feed_sources` is
--      the sole GUID authority and feed deletion cannot fail on GUID collisions
--      (QUA-006 / TRP-012).
--
-- sqlx 0.8 SQLite runs every migration inside its own transaction and ignores
-- `-- no-transaction`; `PRAGMA foreign_keys` is a no-op inside a transaction.
-- `db::run_migrations` therefore applies migrations on a dedicated connection
-- opened with `foreign_keys = OFF`, so `DROP TABLE articles` cannot cascade into
-- `edition_articles`, `article_feed_sources`, or `article_extraction_jobs`.
-- Never apply this file on a connection with foreign keys ON.

CREATE TABLE articles_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_id INTEGER,
    external_guid TEXT,
    canonical_url TEXT NOT NULL,
    title TEXT NOT NULL,
    author TEXT,
    description TEXT,
    published_at TEXT,
    discovered_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    extracted_content TEXT,
    extraction_status TEXT NOT NULL DEFAULT 'pending' CHECK (
        extraction_status IN ('pending', 'extracted', 'failed', 'skipped')
    ),
    content_hash TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    extraction_attempts INTEGER NOT NULL DEFAULT 0,
    last_extraction_attempt_at TEXT,
    last_successful_extraction_at TEXT,
    extraction_error TEXT,
    next_extraction_retry_at TEXT,
    extracted_content_type TEXT NOT NULL DEFAULT 'none' CHECK (
        extracted_content_type IN ('html', 'summary_fallback', 'none')
    ),
    original_source_url TEXT,
    entry_updated_at TEXT,
    FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE SET NULL
);

INSERT INTO articles_new (
    id,
    feed_id,
    external_guid,
    canonical_url,
    title,
    author,
    description,
    published_at,
    discovered_at,
    extracted_content,
    extraction_status,
    content_hash,
    created_at,
    updated_at,
    extraction_attempts,
    last_extraction_attempt_at,
    last_successful_extraction_at,
    extraction_error,
    next_extraction_retry_at,
    extracted_content_type,
    original_source_url,
    entry_updated_at
)
SELECT
    id,
    feed_id,
    external_guid,
    canonical_url,
    title,
    author,
    description,
    published_at,
    discovered_at,
    extracted_content,
    extraction_status,
    content_hash,
    created_at,
    updated_at,
    extraction_attempts,
    last_extraction_attempt_at,
    last_successful_extraction_at,
    extraction_error,
    next_extraction_retry_at,
    CASE
        WHEN extracted_content IS NULL OR trim(extracted_content) = '' THEN 'none'
        ELSE COALESCE(extracted_content_type, 'html')
    END,
    original_source_url,
    entry_updated_at
FROM articles;

DROP TABLE articles;

ALTER TABLE articles_new RENAME TO articles;

CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_canonical_url
    ON articles(canonical_url);

CREATE INDEX IF NOT EXISTS idx_articles_content_hash
    ON articles(content_hash)
    WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_articles_feed_id ON articles(feed_id);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_discovered_at ON articles(discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_extraction_status ON articles(extraction_status);
CREATE INDEX IF NOT EXISTS idx_articles_extraction_retry
    ON articles(extraction_status, next_extraction_retry_at);

UPDATE articles
SET feed_id = NULL
WHERE feed_id IS NOT NULL
  AND feed_id NOT IN (SELECT id FROM feeds);
