"""Trend radar: recent GTA 6 videos on YouTube ranked by views per hour.

usage: python trends.py [--query "gta 6"] [--hours 24] [--limit 30] [--out trends.md]
"""
import argparse, json, sys, time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from common import ytdlp_json

UPLOAD_DATE_SORT = "&sp=CAI%253D"  # YouTube search sorted by upload date


ERRORS = []


def detail(url):
    try:
        return ytdlp_json(url, ["--skip-download"])
    except SystemExit as e:  # run() exits with yt-dlp's stderr tail
        ERRORS.append(str(e))
        return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--query", default="gta 6"); ap.add_argument("--hours", type=float, default=24)
    ap.add_argument("--limit", type=int, default=30); ap.add_argument("--out", default="trends.md")
    ap.add_argument("--json", help="also write the ranked rows as JSON (for automation)")
    a = ap.parse_args()
    q = a.query.replace(" ", "+")
    flat = ytdlp_json(f"https://www.youtube.com/results?search_query={q}{UPLOAD_DATE_SORT}",
                      ["--flat-playlist", "--playlist-end", str(a.limit)])
    urls = [e["url"] for e in flat.get("entries", []) if e.get("url") and "/shorts/" not in e["url"]]
    with ThreadPoolExecutor(6) as ex:
        infos = [i for i in ex.map(detail, urls) if i]
    if ERRORS:
        last = [l for l in ERRORS[0].splitlines() if l.strip()][-1:] or ["?"]
        print(f"{len(ERRORS)} of {len(urls)} videos could not be read. First error: {last[0][:300]}", file=sys.stderr)
        if "not a bot" in ERRORS[0]:
            print("YouTube blocks this IP (bot check). Run trends from a residential connection (PC).", file=sys.stderr)
    if urls and not infos:
        sys.exit(f"trends failed: none of the {len(urls)} videos could be read")
    now = time.time(); rows = []
    for i in infos:
        t = i.get("timestamp") or i.get("release_timestamp")
        if not t or i.get("is_live"):
            continue
        age = max(0.25, (now - t) / 3600)
        if age > a.hours:
            continue
        v = i.get("view_count") or 0
        rows.append((v / age, v, age, i.get("duration") or 0, i.get("channel") or "", i.get("title") or "", i.get("webpage_url"),
                     bool(i.get("heatmap"))))
    rows.sort(reverse=True)
    md = [f"# Trend radar: \"{a.query}\", last {a.hours:.0f} h ({time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())})", "",
          "| # | views/h | views | age h | min | channel | title | most replayed |", "|---|---|---|---|---|---|---|---|"]
    for k, r in enumerate(rows, 1):
        md.append(f"| {k} | {r[0]:,.0f} | {r[1]:,} | {r[2]:.1f} | {r[3] / 60:.0f} | {r[4]} | [{r[5][:70]}]({r[6]}) | {'yes' if r[7] else ''} |")
    Path(a.out).write_text("\n".join(md) + "\n", encoding="utf-8")
    if a.json:
        keys = ("views_per_hour", "views", "age_hours", "duration", "channel", "title", "url", "most_replayed")
        Path(a.json).write_text(json.dumps([dict(zip(keys, r)) for r in rows], ensure_ascii=False, indent=1), encoding="utf-8")
    print("\n".join(md[:14]))
    print(f"... {len(rows)} videos -> {a.out}")


if __name__ == "__main__":
    main()
