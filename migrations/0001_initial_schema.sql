-- Quire Phase 1 Initial Schema
-- Requires SQLite with foreign_keys = ON and journal_mode = WAL (enforced by connection pool).
-- All timestamps are stored as ISO-8601 UTC strings (YYYY-MM-DDTHH:MM:SSZ).

CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    feed_url TEXT NOT NULL UNIQUE,
    website_url TEXT,
    description TEXT,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    last_successful_fetch_at TEXT,
    last_attempted_fetch_at TEXT,
    last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_feeds_enabled ON feeds(enabled);
CREATE INDEX IF NOT EXISTS idx_feeds_created_at ON feeds(created_at);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    feed_id INTEGER NOT NULL,
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
    FOREIGN KEY (feed_id) REFERENCES feeds(id) ON DELETE CASCADE
);

-- Deduplication indexes without assuming every publisher provides reliable GUIDs:
-- 1. When a publisher provides a non-null GUID, prevent duplicates within the same feed.
CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_feed_external_guid
    ON articles(feed_id, external_guid)
    WHERE external_guid IS NOT NULL AND external_guid != '';

-- 2. Prevent duplicate canonical URLs across all feeds.
CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_canonical_url
    ON articles(canonical_url);

-- 3. Index content_hash for cross-feed body/title deduplication lookups.
CREATE INDEX IF NOT EXISTS idx_articles_content_hash
    ON articles(content_hash)
    WHERE content_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_articles_feed_id ON articles(feed_id);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_discovered_at ON articles(discovered_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_extraction_status ON articles(extraction_status);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now'))
);

CREATE TABLE IF NOT EXISTS editions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    publication_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft' CHECK (
        status IN ('draft', 'generating', 'completed', 'failed', 'delivered')
    ),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    output_file_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_editions_publication_date ON editions(publication_date DESC);
CREATE INDEX IF NOT EXISTS idx_editions_status ON editions(status);

CREATE TABLE IF NOT EXISTS edition_articles (
    edition_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    position INTEGER NOT NULL CHECK (position >= 0),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ', 'now')),
    PRIMARY KEY (edition_id, article_id),
    UNIQUE (edition_id, position),
    FOREIGN KEY (edition_id) REFERENCES editions(id) ON DELETE CASCADE,
    FOREIGN KEY (article_id) REFERENCES articles(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_edition_articles_article_id ON edition_articles(article_id);
