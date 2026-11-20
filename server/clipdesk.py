"""GameInsight clip desk server: a small HTTP API so n8n (or cron) can drive the scripts on a VPS.

All endpoints except /health need the header  Authorization: Bearer $CLIPDESK_TOKEN

  GET  /health
  POST /pipeline         {"query":"gta 6","hours":24,"clips":3,"layout":"blur","max_minutes":90}
                         trends -> best unprocessed video -> moments -> cut top clips with hooks -> rule check
  POST /jobs/<name>      {"args":[...]}  run one whitelisted script (trends, streams, questions, live)
  GET  /runs             the last 20 runs
  GET  /runs/<id>        status, log tail and finished clips (with download URLs)
  POST /mcp              MCP (Streamable HTTP) for Claude: Keycloak token (audience clipdesk-api, role clipdesk-admin)
                         or the static token; metadata at /.well-known/oauth-protected-resource
  GET  /files/<path>     download a file below the data folder (clips, markdown reports)

Only one heavy run (pipeline or job) works at a time; one live recording may run alongside.
Nothing is posted anywhere: the clips wait for a human to approve them.
"""
import hmac, json, os, re, shutil, subprocess, sys, threading, time, traceback, urllib.request, uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DATA = Path(os.environ.get("CLIP_DESK_HOME", "/data")).resolve()
APP = Path(os.environ.get("CLIPDESK_APP", "/app")).resolve()
SCRIPTS = APP / "scripts"
TOKEN = os.environ.get("CLIPDESK_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")
RETAIN_DAYS = float(os.environ.get("RETAIN_DAYS", "3"))
HOOK_MODEL = os.environ.get("HOOK_MODEL", "claude-haiku-5-5")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HASHTAGS = os.environ.get("HASHTAGS", "#GTA6 #GTAVI #GTA6Clips #GameInsight")

JOBS = {"trends": 900, "streams": 600, "questions": 900, "live": 4 * 3600}  # script -> timeout (s)
# whisper "small" on 3 CPUs needs roughly 0.3-0.6x the video length, so a 90-minute video can exceed 1 h
MOMENTS_TIMEOUT = int(os.environ.get("MOMENTS_TIMEOUT", str(2 * 3600)))
RUNS_DIR = DATA / "runs"
STATE = DATA / "state.json"
heavy = threading.Lock()
live_lock = threading.Lock()
runs = {}


# ---------- runs ----------

def new_run(kind, params):
    rid = time.strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:6]
    r = {"id": rid, "kind": kind, "params": params, "status": "queued", "started": None, "finished": None,
         "log": [], "clips": [], "error": None}
    runs[rid] = r
    save_run(r)
    return r


def save_run(r):
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    (RUNS_DIR / f"{r['id']}.json").write_text(json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")


def load_run(rid):
    if rid in runs:
        return runs[rid]
    p = RUNS_DIR / f"{rid}.json"
    return json.loads(p.read_text(encoding="utf-8")) if re.fullmatch(r"[\w-]+", rid) and p.exists() else None


def log(r, msg):
    line = f"{time.strftime('%H:%M:%S')} {msg}"
    print(f"[{r['id']}] {msg}", flush=True)
    r["log"] = (r["log"] + [line])[-200:]
    save_run(r)


def script(r, name, args, timeout):
    cmd = [sys.executable, str(SCRIPTS / f"{name}.py"), *map(str, args)]
    log(r, "$ " + " ".join(cmd[1:])[:300])
    p = subprocess.run(cmd, cwd=DATA, text=True, encoding="utf-8", errors="replace",
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    for line in p.stdout.strip().splitlines()[-15:]:
        log(r, "  " + line[:300])
    return p.returncode, p.stdout


def start(r, fn, lock):
    def body():
        with lock:
            r["status"], r["started"] = "running", time.time()
            save_run(r)
            try:
                fn(r)
                r["status"] = "done"
            except Exception as e:  # keep the server alive, report the failure
                r["status"], r["error"] = "failed", f"{type(e).__name__}: {e}"
                log(r, traceback.format_exc()[-800:])
            r["finished"] = time.time()
            save_run(r)
    threading.Thread(target=body, daemon=True).start()


# ---------- pipeline ----------

def file_url(path):
    rel = Path(path).resolve().relative_to(DATA).as_posix()
    return f"{PUBLIC_URL}/files/{rel}" if PUBLIC_URL else f"/files/{rel}"


def load_state():
    try:
        return json.loads(STATE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {"done": [], "hooks": []}


def make_hook(text, title, channel):
    """Three hook options following skills/hook-writer, return the pick. Empty string if no API key."""
    if not ANTHROPIC_KEY:
        return ""
    rules = ""
    sk = APP / "skills" / "hook-writer" / "SKILL.md"
    if sk.exists():
        rules = sk.read_text(encoding="utf-8").split("---", 2)[-1]
    recent = ", ".join(load_state().get("hooks", [])[-30:])
    prompt = (f"{rules}\n\nSource video: \"{title}\" by {channel}.\nTranscript of the clip:\n\"\"\"{text[:1500]}\"\"\"\n"
              f"Hooks used this week (do not reuse): {recent or 'none'}\n\n"
              'Answer only with JSON: {"options": ["...", "...", "..."], "pick": "..."}')
    req = urllib.request.Request("https://api.anthropic.com/v1/messages", method="POST",
                                 data=json.dumps({"model": HOOK_MODEL, "max_tokens": 300,
                                                  "messages": [{"role": "user", "content": prompt}]}).encode(),
                                 headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01",
                                          "content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            out = json.loads(resp.read())["content"][0]["text"]
        m = re.search(r"\{.*\}", out, re.S)
        pick = json.loads(m.group(0)).get("pick", "") if m else ""
        return re.sub(r"\s+", " ", pick).strip()[:80]
    except Exception as e:
        print(f"hook generation failed: {e}", flush=True)
        return ""


def pipeline(r):
    p = r["params"]
    query, hours = str(p.get("query", "gta 6")), float(p.get("hours", 24))
    n_clips, layout = int(p.get("clips", 3)), str(p.get("layout", "blur"))
    max_min = int(p.get("max_minutes", 90))
    if layout not in ("fill", "blur", "facecam", "react"):
        raise ValueError("layout must be fill, blur, facecam or react")

    rc, _ = script(r, "trends", ["--query", query, "--hours", hours, "--json", "trends.json"], JOBS["trends"])
    if rc:
        raise RuntimeError("trends failed (YouTube reachable? see log)")
    rows = json.loads((DATA / "trends.json").read_text(encoding="utf-8"))
    state = load_state()
    pick = next((x for x in rows if x["url"] not in state["done"] and 60 <= (x["duration"] or 0) <= max_min * 60), None)
    if not pick:
        log(r, "no new video in the trend list – nothing to do")
        return
    log(r, f"video: {pick['title'][:80]} ({pick['channel']}, {pick['views_per_hour']:.0f} views/h)")
    r["source"] = {k: pick[k] for k in ("title", "channel", "url", "views_per_hour")}

    rc, out = script(r, "moments", [pick["url"], "--max-minutes", max_min], MOMENTS_TIMEOUT)
    m = re.search(r"^job: (.+)$", out, re.M)
    if rc or not m:
        raise RuntimeError("moments failed")
    job = Path(m.group(1).strip())
    job = job if job.is_absolute() else DATA / job
    cands = json.loads((job / "candidates.json").read_text(encoding="utf-8"))["candidates"]
    for c in sorted(cands, key=lambda c: -c["score"])[:n_clips]:
        hook = make_hook(c["text"], pick["title"], pick["channel"])
        args = [job, "--pick", c["n"], "--layout", layout] + (["--hook", hook] if hook else [])
        rc, out = script(r, "cut", args, 1800)
        mp4 = next((l.strip() for l in reversed(out.splitlines()) if l.strip().endswith(".mp4")), "")
        if rc or not mp4:
            log(r, f"cut of candidate {c['n']} failed – skipped")
            continue
        mp4 = Path(mp4) if Path(mp4).is_absolute() else DATA / mp4
        caption = (f"{hook}\n\n" if hook else "") + f"🎥 Credit: {pick['channel']} – {pick['url']}\n{HASHTAGS}"
        rc, out = script(r, "check", [mp4, "--caption", caption], 300)
        r["clips"].append({"file": mp4.relative_to(DATA).as_posix(), "url": file_url(mp4), "hook": hook,
                           "caption": caption, "check": "PASS" if rc == 0 else "FAIL",
                           "check_report": [l for l in out.splitlines() if l.startswith(("PASS", "FAIL"))],
                           "start": c["start"], "end": c["end"], "why": c["why"], "source": pick["url"]})
        if hook:
            state.setdefault("hooks", []).append(hook)
        save_run(r)

    state["done"] = (state["done"] + [pick["url"]])[-500:]
    state["hooks"] = state.get("hooks", [])[-60:]
    STATE.write_text(json.dumps(state, ensure_ascii=False, indent=1), encoding="utf-8")
    cleanup(r)


def cleanup(r):
    """Delete job folders (source video, edits) older than RETAIN_DAYS."""
    jobs = DATA / "jobs"
    if not jobs.exists():
        return
    limit = time.time() - RETAIN_DAYS * 86400
    for d in jobs.iterdir():
        if d.is_dir() and d.stat().st_mtime < limit:
            shutil.rmtree(d, ignore_errors=True)
            log(r, f"cleanup: removed {d.name}")


def job_runner(name, args):
    def fn(r):
        rc, _ = script(r, name, args, JOBS[name])
        if rc:
            raise RuntimeError(f"{name} exited with {rc}")
        for f in sorted(DATA.glob("*.md")) + sorted((DATA / "jobs").glob("*/edits/*.mp4")):
            if f.stat().st_mtime >= r["started"]:
                r["clips" if f.suffix == ".mp4" else "files"] = r.get("clips" if f.suffix == ".mp4" else "files", []) + [
                    {"file": f.relative_to(DATA).as_posix(), "url": file_url(f)}]
    return fn


# ---------- actions (shared by REST and MCP) ----------

def action_pipeline(data):
    """Start the pipeline. Returns (http_code, body)."""
    if heavy.locked():
        return 409, {"error": "busy – another run is working"}
    r = new_run("pipeline", data)
    start(r, pipeline, heavy)
    return 202, {"run": r["id"]}


def action_job(name, args):
    if name not in JOBS or not isinstance(args, list) or not all(isinstance(a, (str, int, float)) for a in args):
        return 400, {"error": f"job must be one of {sorted(JOBS)} with args as a list"}
    lock = live_lock if name == "live" else heavy
    if lock.locked():
        return 409, {"error": "busy"}
    r = new_run(name, {"args": args})
    start(r, job_runner(name, args), lock)
    return 202, {"run": r["id"]}


def list_runs(limit=20):
    files = sorted(RUNS_DIR.glob("*.json"), reverse=True)[:limit] if RUNS_DIR.exists() else []
    return [{k: v for k, v in json.loads(f.read_text(encoding="utf-8")).items() if k != "log"} for f in files]


def status():
    return {"status": "ok", "busy": heavy.locked(), "live": live_lock.locked()}


# ---------- MCP (Streamable HTTP, stateless JSON) ----------
# Claude connects as a custom connector; OAuth runs against Keycloak (client clipdesk-mcp in coach-raffael/identity).
# The access token must carry the audience MCP_AUDIENCE and the realm role MCP_ROLE.

OIDC_ISSUER = os.environ.get("OIDC_ISSUER", "https://auth.coach-raffael.com/realms/coach-raffael").rstrip("/")
MCP_AUDIENCE = os.environ.get("MCP_AUDIENCE", "clipdesk-api")
MCP_ROLE = os.environ.get("MCP_ROLE", "clipdesk-admin")
MCP_PROTOCOLS = ("2025-06-18", "2025-03-26", "2024-11-05")
_jwks = None

LAYOUTS = ["fill", "blur", "facecam", "react"]
MCP_TOOLS = [
    {"name": "clipdesk_status",
     "description": "Health of the GameInsight clip desk: whether a heavy run (pipeline/job) or a live recording is busy, plus the 5 latest runs.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "start_pipeline",
     "description": "Start the clip pipeline: trending video -> highlight moments -> 1080x1920 clips with hook -> rule check. "
                    "Only one heavy run at a time. Returns a run id; poll it with get_run. Nothing is posted anywhere.",
     "inputSchema": {"type": "object", "properties": {
         "query": {"type": "string", "description": "YouTube search, default 'gta 6'"},
         "hours": {"type": "number", "description": "Only videos from the last N hours, default 24"},
         "clips": {"type": "integer", "minimum": 1, "maximum": 8, "description": "Number of clips, default 3"},
         "layout": {"type": "string", "enum": LAYOUTS, "description": "9:16 layout, default blur"},
         "max_minutes": {"type": "integer", "minimum": 1, "maximum": 240, "description": "Only the first N minutes of the source, default 90"}}}},
    {"name": "start_job",
     "description": "Run one whitelisted script: trends, streams, questions (heavy, one at a time) or live (records a stream; runs alongside). "
                    "args are the script's command-line arguments, e.g. live: ['https://twitch.tv/<login>', '--minutes', '120'].",
     "inputSchema": {"type": "object", "required": ["name"], "properties": {
         "name": {"type": "string", "enum": sorted(JOBS)},
         "args": {"type": "array", "items": {"type": ["string", "number"]}}}}},
    {"name": "list_runs",
     "description": "The latest runs with status, parameters and finished clips (without logs).",
     "inputSchema": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}}}},
    {"name": "get_run",
     "description": "One run: status (queued/running/done/failed), error, log tail and clips with download URL, hook, caption and rule-check result.",
     "inputSchema": {"type": "object", "required": ["run_id"], "properties": {
         "run_id": {"type": "string"},
         "log_lines": {"type": "integer", "minimum": 0, "maximum": 200, "description": "Log lines to include, default 30"}}}},
]


def mcp_call(name, args):
    """Execute one tool. Returns (payload, is_error)."""
    if name == "clipdesk_status":
        return {**status(), "latest_runs": list_runs(5)}, False
    if name == "start_pipeline":
        params = {k: args[k] for k in ("query", "hours", "clips", "layout", "max_minutes") if k in args}
        code, body = action_pipeline(params)
        return body, code >= 400
    if name == "start_job":
        code, body = action_job(str(args.get("name", "")), args.get("args", []))
        return body, code >= 400
    if name == "list_runs":
        return list_runs(int(args.get("limit", 20))), False
    if name == "get_run":
        r = load_run(str(args.get("run_id", "")))
        if not r:
            return {"error": "unknown run"}, True
        n = int(args.get("log_lines", 30))
        return {**r, "log": r.get("log", [])[-n:] if n else []}, False
    return {"error": f"unknown tool {name}"}, True


def mcp_handle(msg):
    """One JSON-RPC message -> response dict, or None for notifications."""
    if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0" or "method" not in msg:
        return {"jsonrpc": "2.0", "id": msg.get("id") if isinstance(msg, dict) else None,
                "error": {"code": -32600, "message": "invalid request"}}
    mid, method, params = msg.get("id"), msg["method"], msg.get("params") or {}
    if mid is None:  # notification (notifications/initialized, cancelled, ...)
        return None
    ok = lambda result: {"jsonrpc": "2.0", "id": mid, "result": result}
    if method == "initialize":
        want = params.get("protocolVersion")
        return ok({"protocolVersion": want if want in MCP_PROTOCOLS else MCP_PROTOCOLS[0],
                   "capabilities": {"tools": {"listChanged": False}},
                   "serverInfo": {"name": "gameinsight-clipdesk", "title": "GameInsight Clip Desk", "version": "1.0.0"},
                   "instructions": "GameInsight clip desk (clips.coach-raffael.com): starts clip runs on the VPS and reports their results. "
                                   "Runs take 10-60 minutes; start one, then poll get_run. Nothing is ever posted - "
                                   "finished clips go to Raffael via Telegram for approval."})
    if method == "ping":
        return ok({})
    if method == "tools/list":
        return ok({"tools": MCP_TOOLS})
    if method == "tools/call":
        try:
            payload, err = mcp_call(params.get("name"), params.get("arguments") or {})
        except Exception as e:
            payload, err = {"error": f"{type(e).__name__}: {e}"}, True
        return ok({"content": [{"type": "text", "text": json.dumps(payload, ensure_ascii=False, indent=1)}],
                   "structuredContent": payload if isinstance(payload, dict) else {"items": payload},
                   "isError": err})
    return {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": f"method not found: {method}"}}


def verify_jwt(token):
    """Validate a Keycloak access token: signature (JWKS), issuer, audience, realm role. Returns the claims or None."""
    global _jwks
    if not OIDC_ISSUER:
        return None
    try:
        import jwt  # PyJWT[crypto]
        if _jwks is None:
            _jwks = jwt.PyJWKClient(f"{OIDC_ISSUER}/protocol/openid-connect/certs", cache_keys=True, lifespan=3600)
        key = _jwks.get_signing_key_from_jwt(token).key
        claims = jwt.decode(token, key, algorithms=["RS256", "ES256", "PS256"], audience=MCP_AUDIENCE,
                            issuer=OIDC_ISSUER, leeway=30)
    except Exception as e:
        print(f"mcp: token rejected ({type(e).__name__}: {e})", flush=True)
        return None
    if MCP_ROLE and MCP_ROLE not in (claims.get("realm_access") or {}).get("roles", []):
        print(f"mcp: token of {claims.get('preferred_username')} lacks role {MCP_ROLE}", flush=True)
        return None
    return claims


# ---------- HTTP ----------

class Handler(BaseHTTPRequestHandler):
    server_version = "clipdesk/1"

    def log_message(self, fmt, *args):
        if not self.path.startswith("/health"):
            print(f"{self.address_string()} {fmt % args}", flush=True)

    def send_json(self, code, obj, headers=None):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def base_url(self):
        return PUBLIC_URL or f"http://{self.headers.get('Host', 'localhost')}"

    def static_token_ok(self):
        got = self.headers.get("Authorization", "")
        return bool(TOKEN) and hmac.compare_digest(got.encode(), f"Bearer {TOKEN}".encode())

    def authed(self):
        if self.static_token_ok():
            return True
        self.send_json(401, {"error": "unauthorized"})
        return False

    def mcp_authed(self):
        """Static clip desk token (n8n, tests) or a Keycloak token with audience + role (Claude connector)."""
        if self.static_token_ok():
            return True
        got = self.headers.get("Authorization", "")
        if got.startswith("Bearer ") and verify_jwt(got[7:].strip()):
            return True
        meta = f"{self.base_url()}/.well-known/oauth-protected-resource"
        self.send_json(401, {"error": "unauthorized"},
                       {"WWW-Authenticate": f'Bearer resource_metadata="{meta}"'})
        return False

    def raw_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(min(n, 1 << 20)) if n else b""

    def body(self):
        raw = self.raw_body()
        if not raw:
            return {}
        data = json.loads(raw or b"{}")
        if not isinstance(data, dict):
            raise ValueError("JSON object expected")
        return data

    def resource_metadata(self):
        return {"resource": f"{self.base_url()}/mcp",
                "authorization_servers": [OIDC_ISSUER] if OIDC_ISSUER else [],
                "bearer_methods_supported": ["header"],
                "scopes_supported": ["openid", "profile", "email", "roles", "offline_access"],
                "resource_name": "GameInsight Clip Desk"}

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/health":
            return self.send_json(200, status())
        if path in ("/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"):
            return self.send_json(200, self.resource_metadata())
        if path == "/mcp":  # no server-initiated SSE stream
            return self.send_json(405, {"error": "use POST"}, {"Allow": "POST"})
        if not self.authed():
            return
        if path == "/runs":
            return self.send_json(200, list_runs(20))
        if path.startswith("/runs/"):
            r = load_run(path[6:])
            return self.send_json(200, r) if r else self.send_json(404, {"error": "unknown run"})
        if path.startswith("/files/"):
            f = (DATA / urllib.request.unquote(path[7:])).resolve()
            if DATA not in f.parents or not f.is_file() or f.suffix not in (".mp4", ".md", ".json", ".srt", ".ass", ".jpg", ".png"):
                return self.send_json(404, {"error": "not found"})
            ctype = {".mp4": "video/mp4", ".jpg": "image/jpeg", ".png": "image/png"}.get(f.suffix, "text/plain; charset=utf-8")
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(f.stat().st_size))
            self.send_header("Content-Disposition", f'inline; filename="{f.name}"')
            self.end_headers()
            with f.open("rb") as fh:
                shutil.copyfileobj(fh, self.wfile, 1 << 20)
            return
        self.send_json(404, {"error": "not found"})

    def do_DELETE(self):
        if self.path.split("?", 1)[0] == "/mcp":  # stateless: no sessions to end
            return self.send_json(405, {"error": "no sessions"}, {"Allow": "POST"})
        self.send_json(404, {"error": "not found"})

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        if path == "/mcp":
            return self.post_mcp()
        if not self.authed():
            return
        try:
            data = self.body()
        except ValueError as e:
            return self.send_json(400, {"error": str(e)})
        if path == "/pipeline":
            return self.send_json(*action_pipeline(data))
        if path.startswith("/jobs/"):
            return self.send_json(*action_job(path[6:], data.get("args", [])))
        self.send_json(404, {"error": "not found"})

    def post_mcp(self):
        if not self.mcp_authed():
            return
        try:
            msg = json.loads(self.raw_body() or b"null")
        except ValueError:
            return self.send_json(400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "parse error"}})
        if isinstance(msg, list):
            out = [r for r in (mcp_handle(m) for m in msg) if r is not None]
        else:
            out = mcp_handle(msg)
        if not out:  # only notifications
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_json(200, out)


def mark_interrupted():
    """Runs left as queued/running by a restart (deploy, OOM) would make n8n poll forever – mark them failed."""
    if not RUNS_DIR.exists():
        return
    for f in RUNS_DIR.glob("*.json"):
        try:
            r = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if r.get("status") in ("queued", "running"):
            r["status"], r["error"], r["finished"] = "failed", "interrupted by a server restart", time.time()
            save_run(r)


def main():
    if not TOKEN:
        sys.exit("CLIPDESK_TOKEN is not set – refusing to start without auth")
    DATA.mkdir(parents=True, exist_ok=True)
    mark_interrupted()
    port = int(os.environ.get("PORT", "8088"))
    print(f"clipdesk on :{port}, data {DATA}, hooks {'on (' + HOOK_MODEL + ')' if ANTHROPIC_KEY else 'off'}, "
          f"mcp /mcp (issuer {OIDC_ISSUER or 'off'}, audience {MCP_AUDIENCE}, role {MCP_ROLE})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
