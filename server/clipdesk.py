"""GameInsight clip desk server: a small HTTP API so n8n (or cron) can drive the scripts on a VPS.

All endpoints except /health need the header  Authorization: Bearer $CLIPDESK_TOKEN

  GET  /health
  POST /pipeline         {"query":"gta 6","hours":24,"clips":3,"layout":"blur","max_minutes":90}
                         trends -> best unprocessed video -> moments -> cut top clips with hooks -> rule check
  POST /jobs/<name>      {"args":[...]}  run one whitelisted script (trends, streams, questions, live)
  GET  /runs             the last 20 runs
  GET  /runs/<id>        status, log tail and finished clips (with download URLs)
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


# ---------- HTTP ----------

class Handler(BaseHTTPRequestHandler):
    server_version = "clipdesk/1"

    def log_message(self, fmt, *args):
        if not self.path.startswith("/health"):
            print(f"{self.address_string()} {fmt % args}", flush=True)

    def send_json(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def authed(self):
        got = self.headers.get("Authorization", "")
        if TOKEN and hmac.compare_digest(got.encode(), f"Bearer {TOKEN}".encode()):
            return True
        self.send_json(401, {"error": "unauthorized"})
        return False

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        if not n:
            return {}
        data = json.loads(self.rfile.read(min(n, 65536)) or b"{}")
        if not isinstance(data, dict):
            raise ValueError("JSON object expected")
        return data

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/health":
            return self.send_json(200, {"status": "ok", "busy": heavy.locked(), "live": live_lock.locked()})
        if not self.authed():
            return
        if path == "/runs":
            files = sorted(RUNS_DIR.glob("*.json"), reverse=True)[:20] if RUNS_DIR.exists() else []
            out = [{k: v for k, v in json.loads(f.read_text(encoding="utf-8")).items() if k != "log"} for f in files]
            return self.send_json(200, out)
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

    def do_POST(self):
        if not self.authed():
            return
        try:
            data = self.body()
        except ValueError as e:
            return self.send_json(400, {"error": str(e)})
        path = self.path.split("?", 1)[0]
        if path == "/pipeline":
            if heavy.locked():
                return self.send_json(409, {"error": "busy – another run is working"})
            r = new_run("pipeline", data)
            start(r, pipeline, heavy)
            return self.send_json(202, {"run": r["id"]})
        if path.startswith("/jobs/"):
            name = path[6:]
            args = data.get("args", [])
            if name not in JOBS or not isinstance(args, list) or not all(isinstance(a, (str, int, float)) for a in args):
                return self.send_json(400, {"error": f"job must be one of {sorted(JOBS)} with args as a list"})
            lock = live_lock if name == "live" else heavy
            if lock.locked():
                return self.send_json(409, {"error": "busy"})
            r = new_run(name, {"args": args})
            start(r, job_runner(name, args), lock)
            return self.send_json(202, {"run": r["id"]})
        self.send_json(404, {"error": "not found"})


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
    print(f"clipdesk on :{port}, data {DATA}, hooks {'on (' + HOOK_MODEL + ')' if ANTHROPIC_KEY else 'off'}", flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), Handler).serve_forever()


if __name__ == "__main__":
    main()
