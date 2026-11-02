#!/bin/bash
ART="$(cd "$(dirname "$0")" && pwd)"; SCR=${SCR:-$(mktemp -d)}
cd "$(dirname "$0")/../../.."
DC="docker compose -p quirereview -f compose.yaml -f $ART/compose.review.yaml"
T=$SCR/dbx; rm -rf $T; mkdir -p $T
T0=$(date +%s); $DC stop 2>&1 | tail -1; echo "stop took $(( $(date +%s)-T0 ))s"
docker cp quire-review:/data/. $T/ ; ls $T
sqlite3 $T/quire.db "PRAGMA wal_checkpoint(TRUNCATE);
INSERT INTO articles (feed_id, canonical_url, title, description, extraction_status) VALUES (1,'https://quire-review.invalid/a1','Seeded with summary','<p>Seed summary</p>','pending');
INSERT INTO articles (feed_id, canonical_url, title, extraction_status) VALUES (1,'https://quire-review.invalid/a2','Seeded no summary','pending');
INSERT INTO article_feed_sources (article_id, feed_id) SELECT id,1 FROM articles;
INSERT INTO article_extraction_jobs (article_id,status,attempts,claimed_at) SELECT id,'processing',1,strftime('%Y-%m-%dT%H:%M:%SZ','now') FROM articles;
UPDATE feeds SET fetch_in_progress=1,last_fetch_status='running';
INSERT INTO feed_fetch_runs (feed_id,trigger_type,status) VALUES (1,'scheduled','running');
PRAGMA journal_mode=DELETE;" ; ls $T
sqlite3 $T/quire.db "select id,status,attempts from article_extraction_jobs"
(cd $T && tar --uid 10001 --gid 10001 -cf - quire.db) | docker cp -a - quire-review:/data/
echo "--- recreate container (volume kept)"
$DC up -d --force-recreate 2>&1 | tail -1
for i in $(seq 1 30); do curl -sf localhost:18080/health >/dev/null && break; sleep 1; done
sleep 8
docker logs quire-review 2>&1 | grep -E "Recovered|article_extraction|ERROR" | sed 's/\x1b\[[0-9;]*m//g' | cut -c1-220 | head -8
docker exec quire-review ls -la /data
curl -s localhost:18080/feeds/1 | grep -oiE "cancelled|Interrupted by application restart[^<]*" | sort | uniq -c
curl -s "localhost:18080/articles" | grep -oE "Seeded[^<]*|status-[a-z]+|badge[^>]*>[a-z]+" | head -8
curl -s localhost:18080/articles/2 | grep -oiE "(Retry|Next retry|Attempts|error)[^<]{0,80}" | head -6
