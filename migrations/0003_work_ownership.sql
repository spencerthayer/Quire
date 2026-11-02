-- Quire Phase 2 Remediation Migration 0003:
-- 1. Add durable manual fetch request timestamp (`manual_requested_at`) and active
--    run ownership (`current_run_id`) to `feeds`.
-- 2. Add unique claim ownership token (`claim_token`) to `article_extraction_jobs`.
-- 3. Fix QUA-023 on existing rows where `extracted_content` is NULL or empty so
--    they do not falsely claim `extracted_content_type = 'html'`.
-- 4. Backfill missing `article_extraction_jobs` for any legacy pending articles
--    (QUA-022).

ALTER TABLE feeds ADD COLUMN manual_requested_at TEXT;
ALTER TABLE feeds ADD COLUMN current_run_id INTEGER;

ALTER TABLE article_extraction_jobs ADD COLUMN claim_token TEXT;

-- QUA-023: Clear false `'html'` content-type label on articles with no extracted body
UPDATE articles
SET extracted_content_type = 'none'
WHERE extracted_content IS NULL OR trim(extracted_content) = '';

-- Normalize legacy Phase 1 `extracted` articles that actually have HTML content
UPDATE articles
SET extracted_content_type = 'html'
WHERE extraction_status = 'extracted'
  AND extracted_content IS NOT NULL
  AND trim(extracted_content) != '';

-- QUA-022: Ensure every pending article with no content has a queued extraction job
INSERT OR IGNORE INTO article_extraction_jobs (
    article_id,
    status,
    trigger_type,
    attempts,
    max_attempts,
    next_eligible_at
)
SELECT
    a.id,
    'queued',
    'auto',
    0,
    3,
    strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
FROM articles a
WHERE a.extraction_status = 'pending'
  AND (a.extracted_content IS NULL OR trim(a.extracted_content) = '')
  AND NOT EXISTS (
      SELECT 1 FROM article_extraction_jobs j WHERE j.article_id = a.id
  );
