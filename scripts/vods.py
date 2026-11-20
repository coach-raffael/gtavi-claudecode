"""VODs of a Twitch or Kick channel, audio-only windows and short 1080p sections.

Used by the VPS clip desk for campaign runs: find the newest VOD, download only the audio of the next
unprocessed window (for whisper and the loudness signal), then only the picked moments in 1080p.
A 4-hour stream costs a few hundred MB of audio instead of 5-13 GB of video.

usage: python vods.py list twitch kaicenat [--since 2026-11-18T00:00:00Z]
       python vods.py audio <vod-url> <start-s> <end-s> <out.m4a>
       python vods.py section <vod-url> <start-s> <end-s> <out.mp4>
"""
import argparse, json, sys
from datetime import datetime, timezone
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from common import run, ytdlp_base

LIVE_MARGIN = 180  # seconds kept away from the live edge of a VOD that is still recording


def _ts(s):
    """ISO / Kick 'YYYY-MM-DD HH:MM:SS' (UTC) -> epoch seconds."""
    s = str(s).strip().replace(" ", "T")
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    d = datetime.fromisoformat(s)
    return (d if d.tzinfo else d.replace(tzinfo=timezone.utc)).timestamp()


def twitch_vods(login, limit=10):
    from twitch import gql
    q = """query($login:String!,$n:Int!){ user(login:$login){ displayName stream{ id createdAt }
      videos(first:$n, type:ARCHIVE, sort:TIME){ edges{ node{ id title createdAt lengthSeconds } } } } }"""
    js = gql(q, {"login": login, "n": limit})
    if js.get("errors"):
        raise RuntimeError(f"Twitch GQL: {js['errors'][0].get('message')}")
    u = (js.get("data") or {}).get("user")
    if not u:
        raise RuntimeError(f"Twitch channel {login} not found")
    live_since = _ts(u["stream"]["createdAt"]) if u.get("stream") else None
    out = []
    for e in (u.get("videos") or {}).get("edges", []):
        v = e["node"]
        start = _ts(v["createdAt"])
        recording = bool(live_since and abs(start - live_since) < 900)
        out.append({"platform": "twitch", "channel": login, "id": v["id"], "title": v.get("title") or "",
                    "url": f"https://www.twitch.tv/videos/{v['id']}", "start": start,
                    "length": int(v.get("lengthSeconds") or 0), "recording": recording,
                    "channel_name": u.get("displayName") or login})
    return out


def _kick_get(url):
    try:  # Kick sits behind Cloudflare: a browser TLS fingerprint gets through where plain requests is refused
        from curl_cffi import requests as cr
        r = cr.get(url, impersonate="chrome", timeout=20)
    except ImportError:
        import requests
        r = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    if r.status_code != 200:
        raise RuntimeError(f"Kick API {r.status_code} for {url}")
    return r.json()


def kick_vods(slug, limit=10):
    js = _kick_get(f"https://kick.com/api/v2/channels/{slug}/videos")
    rows = js if isinstance(js, list) else js.get("data", [])
    out = []
    for v in rows[:limit]:
        vid = v.get("video") or {}
        uuid = vid.get("uuid") or v.get("uuid")
        if not uuid:
            continue
        start = _ts(v.get("start_time") or v.get("created_at") or vid.get("created_at"))
        length = int((v.get("duration") or 0) / 1000) if (v.get("duration") or 0) > 100000 else int(v.get("duration") or 0)
        out.append({"platform": "kick", "channel": slug, "id": uuid, "title": v.get("session_title") or "",
                    "url": f"https://kick.com/{slug}/videos/{uuid}", "start": start, "length": length,
                    "recording": bool(v.get("is_live")), "channel_name": slug})
    out.sort(key=lambda x: -x["start"])
    return out


def list_vods(platform, channel, since=None, limit=10):
    if platform == "twitch":
        rows = twitch_vods(channel, limit)
    elif platform == "kick":
        rows = kick_vods(channel, limit)
    else:
        raise RuntimeError(f"{platform} VODs are not supported on the VPS (YouTube blocks datacenter IPs)")
    if since:
        rows = [r for r in rows if r["start"] >= _ts(since)]
    return rows


def usable_end(v):
    """Last second that is safe to download (a recording VOD keeps growing at the live edge)."""
    return max(0, v["length"] - (LIVE_MARGIN if v.get("recording") else 0))


def _sections(url, start, end, out, fmt):
    # --force-keyframes-at-cuts: the file starts exactly at `start`, so transcript/caption times line up
    run(ytdlp_base() + ["-f", fmt, "--download-sections", f"*{start:.2f}-{end:.2f}", "--force-keyframes-at-cuts",
                        "--no-part", "-o", str(out), url])
    return out


def audio_window(url, start, end, out):
    """Audio of [start, end] (Kick has no audio-only format: the smallest video rendition is used)."""
    return _sections(url, start, end, out, "ba/worst")


def video_section(url, start, end, out):
    return _sections(url, start, end, out, "best[height<=1080]/bv*[height<=1080]+ba/best")


def main():
    ap = argparse.ArgumentParser()
    sp = ap.add_subparsers(dest="cmd", required=True)
    p = sp.add_parser("list"); p.add_argument("platform"); p.add_argument("channel"); p.add_argument("--since")
    for name in ("audio", "section"):
        p = sp.add_parser(name); p.add_argument("url"); p.add_argument("start", type=float); p.add_argument("end", type=float)
        p.add_argument("out")
    a = ap.parse_args()
    if a.cmd == "list":
        for v in list_vods(a.platform, a.channel, a.since):
            when = datetime.fromtimestamp(v["start"], timezone.utc).strftime("%Y-%m-%d %H:%M")
            print(f"{when}  {v['length'] / 3600:5.1f} h  {'REC ' if v['recording'] else '    '}{v['url']}  {v['title'][:60]}")
    elif a.cmd == "audio":
        print(audio_window(a.url, a.start, a.end, a.out))
    else:
        print(video_section(a.url, a.start, a.end, a.out))


if __name__ == "__main__":
    main()
