import http.server, socketserver, sys, time, os
PORT = int(sys.argv[1]); DELAY_FILE = sys.argv[2]; N = int(sys.argv[3])
class H(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def do_GET(self):
        if self.path.startswith('/feed.xml'):
            items = ''.join(f'<item><guid>g{i}</guid><title>Article {i}</title><link>http://articles.example.com:{PORT}/a/{i}</link><description>Summary {i}</description></item>' for i in range(N))
            body = f'<?xml version="1.0"?><rss version="2.0"><channel><title>Crash</title>{items}</channel></rss>'.encode()
            self.send_response(200); self.send_header('Content-Type','application/rss+xml'); self.end_headers(); self.wfile.write(body); return
        try: d = float(open(DELAY_FILE).read().strip())
        except Exception: d = 0
        time.sleep(d)
        i = self.path.rsplit('/',1)[-1]
        body = f'<html><head><title>A{i}</title></head><body><article><h1>Article {i}</h1>' + ''.join(f'<p>Paragraph {k} of article {i}, with enough prose, commas, and sentences to satisfy a readability scorer. Lorem ipsum dolor sit amet, consectetur adipiscing elit.</p>' for k in range(8)) + '</article></body></html>'
        self.send_response(200); self.send_header('Content-Type','text/html; charset=utf-8'); self.end_headers(); self.wfile.write(body.encode())
class S(socketserver.ThreadingMixIn, http.server.HTTPServer): daemon_threads = True
S(('127.0.0.1', PORT), H).serve_forever()
