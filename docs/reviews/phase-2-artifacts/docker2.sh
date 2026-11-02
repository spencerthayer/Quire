#!/bin/bash
set -euo pipefail
SCR=${SCR:-$(mktemp -d)}
cd "$(dirname "$0")/../../.."

T=$SCR/dbx; rm -rf "$T"; mkdir -p "$T"
T0=$(date +%s)
./stop --profile review --keep-container
echo "stop took $(( $(date +%s)-T0 ))s"

docker cp quire-review:/data/. "$T/"
ls "$T"
sqlite3 "$T/quire.db" "PRAGMA wal_checkpoint(TRUNCATE);
INSERT INTO articles (feed_id, canonical_url, title, description, extraction_status, extracted_content_type) VALUES (1,'https://quire-review.invalid/a1','Seeded with summary','<p>Seed summary</p>','pending','none');
INSERT INTO articles (feed_id, canonical_url, title, extraction_status, extracted_content_type) VALUES (1,'https://quire-review.invalid/a2','Seeded no summary','pending','none');
INSERT INTO article_feed_sources (article_id, feed_id) SELECT id,1 FROM articles;
INSERT INTO article_extraction_jobs (article_id,status,attempts,claim_token,claimed_at) SELECT id,'processing',1,'seed-tok',strftime('%Y-%m-%dT%H:%M:%SZ','now') FROM articles;
UPDATE feeds SET fetch_in_progress=1,current_run_id=1,last_fetch_status='running';
INSERT INTO feed_fetch_runs (id,feed_id,trigger_type,status) VALUES (1,1,'scheduled','running');
PRAGMA journal_mode=DELETE;"
ls "$T"
sqlite3 "$T/quire.db" "select id,status,attempts from article_extraction_jobs"
(cd "$T" && tar --uid 10001 --gid 10001 -cf - quire.db) | docker cp -a - quire-review:/data/

echo "--- recreate container (volume kept) via ./start --profile review --recreate --no-build"
./start --profile review --recreate --no-build
for i in $(seq 1 30); do curl -sf localhost:18080/health >/dev/null && break; sleep 1; done
sleep 6
docker logs quire-review 2>&1 | grep -E "Recovered|article_extraction|ERROR" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-220 | head -8
docker exec quire-review ls -la /data
curl -s localhost:18080/feeds/1 | grep -oiE "cancelled|Interrupted by application restart[^<]*" | sort | uniq -c
curl -s "localhost:18080/articles" | grep -oE "Seeded[^<]*|Summary[^<]*|Pending|Failed" | head -8
curl -s localhost:18080/articles/2 | grep -oiE "(Retry|Next Retry|attempts|error)[^<]{0,80}" | head -6
