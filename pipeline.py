"""Khatico Reels publish-from-Drive-Queue (Railway cron version).

Fires every 2h on Railway cron schedule. Each invocation:
  1. List Google Drive Queue folder
  2. Pick oldest mp4+json pair
  3. Download mp4
  4. Upload to catbox.moe (with retry)
  5. Publish to IG via Meta Graph API
  6. Move Drive files Queue -> Uploaded with PUBLISHED_<ts>__<shortcode> prefix
  7. Exit (Railway cron service)

No SQLite (ephemeral container). No Remotion render. Drive Queue is the
source of truth; build_queue.py + approve.py still run on Mac to feed it.

All env vars come from Railway environment (set via dashboard or graphql).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).parent
TMP = Path("/tmp/khatico_reels_cron")
TMP.mkdir(exist_ok=True)


def env(key: str, default: str | None = None) -> str:
    v = os.environ.get(key, default)
    if v is None:
        raise RuntimeError(f"Missing env {key}")
    return v


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Google Drive
# ---------------------------------------------------------------------------
def refresh_access_token() -> str:
    body = urllib.parse.urlencode({
        "client_id": env("GOOGLE_CLIENT_ID"),
        "client_secret": env("GOOGLE_CLIENT_SECRET"),
        "refresh_token": env("GOOGLE_REFRESH_TOKEN"),
        "grant_type": "refresh_token",
    }).encode()
    req = urllib.request.Request(
        "https://oauth2.googleapis.com/token", data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["access_token"]


def gdrive(token: str, method: str, path: str, body: bytes | None = None,
           ctype: str | None = None, base: str = "https://www.googleapis.com/drive/v3") -> dict:
    h = {"Authorization": f"Bearer {token}"}
    if ctype:
        h["Content-Type"] = ctype
    req = urllib.request.Request(f"{base}{path}", data=body, headers=h, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            data = r.read()
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"{method} {path} failed: HTTP {e.code} - {e.read().decode()[:300]}")


def list_folder(token: str, parent_id: str) -> list[dict]:
    q = urllib.parse.quote(f"'{parent_id}' in parents and trashed=false")
    res = gdrive(token, "GET",
                 f"/files?q={q}&fields=files(id,name,mimeType,createdTime)&orderBy=createdTime")
    return res.get("files", [])


def download_file(token: str, file_id: str, out_path: Path) -> None:
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        out_path.write_bytes(r.read())


def get_file_text(token: str, file_id: str) -> str:
    url = f"https://www.googleapis.com/drive/v3/files/{file_id}?alt=media"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req) as r:
        return r.read().decode()


def move_file(token: str, file_id: str, new_parent: str, old_parent: str, new_name: str) -> None:
    qs = f"?addParents={new_parent}&removeParents={old_parent}&fields=id,parents"
    body = json.dumps({"name": new_name}).encode()
    gdrive(token, "PATCH", f"/files/{file_id}{qs}", body, "application/json")


# ---------------------------------------------------------------------------
# Hosting + IG publish
# ---------------------------------------------------------------------------
def upload_catbox(file_path: Path, retries: int = 4) -> str:
    """Upload to catbox.moe with retry on transient failures."""
    last_err = None
    for attempt in range(retries):
        try:
            cmd = [
                "curl", "-sS", "--max-time", "120",
                "-F", "reqtype=fileupload",
                "-F", f"fileToUpload=@{file_path}",
                "https://catbox.moe/user/api.php",
            ]
            out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode().strip()
            if out.startswith("https://"):
                return out
            last_err = f"unexpected response: {out[:200]}"
        except subprocess.CalledProcessError as e:
            last_err = f"curl exit {e.returncode}: {e.output.decode()[:200] if e.output else '?'}"
        log(f"  catbox attempt {attempt+1} failed: {last_err}")
        time.sleep(5 * (attempt + 1))
    raise RuntimeError(f"catbox upload failed after {retries}: {last_err}")


def ig_publish(video_url: str, caption: str) -> tuple[str, str, str]:
    """Returns (container_id, media_id, permalink)."""
    api = f"https://graph.facebook.com/{env('GRAPH_API_VERSION')}"
    user = env("IG_BUSINESS_ACCOUNT_ID")
    token = env("USER_ACCESS_TOKEN")

    def post(path, data):
        body = urllib.parse.urlencode(data).encode()
        try:
            with urllib.request.urlopen(urllib.request.Request(f"{api}{path}", data=body, method="POST")) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"POST {path} failed: HTTP {e.code} - {e.read().decode()}")

    def get(path, params):
        qs = urllib.parse.urlencode(params)
        try:
            with urllib.request.urlopen(f"{api}{path}?{qs}") as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"GET {path} failed: HTTP {e.code} - {e.read().decode()}")

    res = post(f"/{user}/media", {
        "media_type": "REELS", "video_url": video_url, "caption": caption,
        "share_to_feed": "true", "access_token": token,
    })
    cid = res["id"]
    log(f"  IG container: {cid}")

    for _ in range(40):
        st = get(f"/{cid}", {"fields": "status_code,status", "access_token": token})
        sc = st.get("status_code", "?")
        if sc == "FINISHED":
            break
        if sc in ("ERROR", "EXPIRED"):
            raise RuntimeError(f"IG container failed: {st}")
        time.sleep(5)
    else:
        raise RuntimeError("IG container timeout")

    pres = post(f"/{user}/media_publish", {"creation_id": cid, "access_token": token})
    media_id = pres["id"]
    pm = get(f"/{media_id}", {"fields": "permalink,shortcode", "access_token": token})
    return cid, media_id, pm.get("permalink", "")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def pick_from_queue(token: str) -> tuple[str, dict, str, str] | None:
    """Returns (base, meta, drive_mp4_id, drive_json_id) or None."""
    queue_id = env("GDRIVE_QUEUE_FOLDER_ID")
    files = list_folder(token, queue_id)
    by_base: dict[str, dict[str, dict]] = {}
    for f in files:
        nm = f["name"]
        if nm.endswith(".mp4") or nm.endswith(".json"):
            base = nm.rsplit(".", 1)[0]
            ext = "mp4" if nm.endswith(".mp4") else "json"
            by_base.setdefault(base, {})[ext] = f
    candidates = [(b, p) for b, p in by_base.items() if "mp4" in p and "json" in p]
    candidates.sort(key=lambda kv: kv[1]["mp4"].get("createdTime", ""))
    if not candidates:
        return None
    base, pair = candidates[0]
    meta = json.loads(get_file_text(token, pair["json"]["id"]))
    return base, meta, pair["mp4"]["id"], pair["json"]["id"]


def main():
    log("=== khatico-reels-cron tick ===")
    token = refresh_access_token()

    picked = pick_from_queue(token)
    if picked is None:
        log("Drive Queue empty - nothing to publish.")
        return

    base, meta, drive_mp4_id, drive_json_id = picked
    style = meta.get("style", "?")
    photo = meta.get("photo", "?")
    log(f"QUEUE: picked {base}.mp4 (style={style}, photo={photo})")

    local_mp4 = TMP / f"{base}.mp4"
    download_file(token, drive_mp4_id, local_mp4)
    log(f"  downloaded {local_mp4.stat().st_size // 1024}KB")

    try:
        url = upload_catbox(local_mp4)
        log(f"  catbox: {url}")

        cid, media_id, permalink = ig_publish(url, meta["caption"])
        log(f"PUBLISHED: {permalink}")

        # Move Drive files Queue -> Uploaded
        token = refresh_access_token()  # refresh in case it expired during IG poll
        queue_id = env("GDRIVE_QUEUE_FOLDER_ID")
        uploaded_id = env("GDRIVE_UPLOADED_FOLDER_ID")
        ts = int(time.time())
        stamp = time.strftime("%Y-%m-%dT%H-%M", time.gmtime(ts))
        shortcode = permalink.rstrip("/").split("/")[-1] if permalink else "no-shortcode"
        new_mp4_name = f"PUBLISHED_{stamp}_{style}__{shortcode}.mp4"
        new_json_name = new_mp4_name.replace(".mp4", ".json")
        move_file(token, drive_mp4_id, uploaded_id, queue_id, new_mp4_name)
        move_file(token, drive_json_id, uploaded_id, queue_id, new_json_name)
        log(f"  Drive: moved Queue->Uploaded as {new_mp4_name}")
    finally:
        local_mp4.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"FATAL: {e}")
        sys.exit(1)
