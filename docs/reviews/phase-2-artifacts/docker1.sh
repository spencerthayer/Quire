#!/bin/bash
ART="$(cd "$(dirname "$0")" && pwd)"; SCR=${SCR:-$(mktemp -d)}
cd "$(dirname "$0")/../../.."
DC="docker compose -p quirereview -f compose.yaml -f $ART/compose.review.yaml"
$DC up -d 2>&1 | tail -2
for i in $(seq 1 40); do s=$(docker inspect -f '{{.State.Health.Status}}' quire-review); [ "$s" = healthy ] && break; sleep 3; done; echo "health=$s after ~$((i*3))s"
docker exec quire-review id; docker exec quire-review ls -la /data
curl -s localhost:18080/health; echo
J=$SCR/cj.txt; rm -f $J; curl -s -c $J -b $J localhost:18080/feeds -o /dev/null; TOK=$(awk '/quire_csrf/{print $7}' $J); echo "tok=${TOK:0:8}..."
curl -s -o /dev/null -w "create feed: %{http_code} -> %{redirect_url}\n" -c $J -b $J -X POST localhost:18080/feeds --data "csrf_token=$TOK&name=Review+Feed&feed_url=https://quire-review.invalid/feed.xml&enabled=on"
curl -s -o /dev/null -w "create feed (bad csrf): %{http_code}\n" -c $J -b $J -X POST localhost:18080/feeds --data "csrf_token=$(printf 'a%.0s' {1..64})&name=X&feed_url=https://x.invalid/f"
curl -s -o /dev/null -w "create feed (no csrf cookie): %{http_code}\n" -X POST localhost:18080/feeds --data "csrf_token=$TOK&name=X&feed_url=https://x.invalid/f"
curl -s -o /dev/null -w "create feed (missing token field): %{http_code}\n" -c $J -b $J -X POST localhost:18080/feeds --data "name=X&feed_url=https://x.invalid/f"
curl -s -o /dev/null -w "ssrf feed 169.254: %{http_code}\n" -c $J -b $J -X POST localhost:18080/feeds --data "csrf_token=$TOK&name=M&feed_url=http://169.254.169.254/latest"
curl -s -c $J -b $J -X POST localhost:18080/settings --data "csrf_token=$TOK&fetch_enabled=true" | grep -io "locked by an environment variable[^<]*" | head -1
curl -s -c $J -b $J -X POST localhost:18080/settings --data "csrf_token=$TOK&fetch_interval_minutes=120&extraction_concurrency=0" | grep -io "concurrency must[^<]*" | head -1
curl -s -o /dev/null -w "settings save valid: %{http_code} -> %{redirect_url}\n" -c $J -b $J -X POST localhost:18080/settings --data "csrf_token=$TOK&fetch_interval_minutes=120"
curl -s -o /dev/null -w "GET /articles/999999: %{http_code}\n" localhost:18080/articles/999999
curl -s -o /dev/null -w "GET /articles?page=0&page_size=100000: %{http_code}\n" "localhost:18080/articles?page=0&page_size=100000"
curl -s -o /dev/null -w "GET /articles?feed_id=abc&status=<script>: %{http_code}\n" "localhost:18080/articles?feed_id=abc&status=%3Cscript%3E"
curl -s -o /dev/null -w "POST fetch nonexistent feed: %{http_code}\n" -c $J -b $J -X POST localhost:18080/feeds/9999/fetch --data "csrf_token=$TOK"
curl -s -o /dev/null -w "GET /feeds/1/fetch: %{http_code}\n" localhost:18080/feeds/1/fetch
echo "$TOK" > $SCR/tok.txt
echo "--- restart"
$DC restart 2>&1 | tail -1
for i in $(seq 1 20); do curl -sf localhost:18080/health >/dev/null && break; sleep 1; done
curl -s localhost:18080/feeds | grep -o "Review Feed" | head -1
curl -s localhost:18080/settings | grep -o 'name="fetch_interval_minutes"[^>]*' | head -1
