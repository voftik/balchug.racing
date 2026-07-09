#!/usr/bin/env python3
"""
Каталог записей Balchug Racing — HTTP API (FastAPI), за nginx на /api/.
Просмотр публичный; правка тегов — под Bearer ADMIN_TOKEN.
"""
import os, json, time
from fastapi import FastAPI, HTTPException, Header, Query
from fastapi.responses import JSONResponse, StreamingResponse
import common as C

HLS_CT = {".m3u8": "application/vnd.apple.mpegurl", ".ts": "video/mp2t",
          ".m4s": "video/iso.segment", ".mp4": "video/mp4", ".vtt": "text/vtt"}

ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "")
PRESIGN_TTL = int(os.environ.get("PRESIGN_TTL", "21600"))  # 6 ч
S3_PUBLIC = f"{C.S3_ENDPOINT}/{C.S3_BUCKET}"  # для public-read объектов (HLS VOD)
HLS_QUEUE = "/var/lib/balchug/hls_queue"

app = FastAPI(title="Balchug Racing Catalog", docs_url=None, redoc_url=None)
_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = C.s3()
    return _s3


def presign(key, ttl=PRESIGN_TTL, download_name=None, content_type=None):
    if not key:
        return None
    params = {"Bucket": C.S3_BUCKET, "Key": key}
    if download_name:
        params["ResponseContentDisposition"] = f'attachment; filename="{download_name}"'
    if content_type:
        params["ResponseContentType"] = content_type
    return s3().generate_presigned_url("get_object", Params=params, ExpiresIn=ttl)


def row_to_card(r):
    return {
        "id": r["id"], "source": r["source"], "title": r["title"], "date": r["date"],
        "season": r["season"], "pilot": r["pilot_name"], "pilot_code": r["pilot_code"],
        "track": r["track_name"], "car": r["car"], "type": r["session_type"],
        "duration": r["duration"], "best_lap": r["best_lap"], "laps": r["laps"],
        "size": r["size"], "has_hls": bool(r["hls_key"]),
        "thumb": presign(r["thumb_key"], content_type="image/jpeg") if r["thumb_key"] else None,
    }


@app.get("/catalog")
def catalog(q: str = "", pilot: str = "", track: str = "", season: str = "",
            stype: str = "", source: str = "", sort: str = "date_desc",
            limit: int = Query(48, le=200), offset: int = 0):
    conn = C.db()
    where, args = [], []
    if q:
        where.append("(title LIKE ? OR pilot_name LIKE ? OR track_name LIKE ? OR notes LIKE ? OR car LIKE ?)")
        args += [f"%{q}%"] * 5
    for col, val in (("pilot_name", pilot), ("track_name", track), ("season", season),
                     ("session_type", stype), ("source", source)):
        if val:
            where.append(f"{col} = ?")
            args.append(val)
    wsql = (" WHERE " + " AND ".join(where)) if where else ""
    order = {"date_desc": "date DESC, mtime DESC", "date_asc": "date ASC",
             "best_lap": "best_lap IS NULL, best_lap ASC",
             "duration": "duration DESC", "pilot": "pilot_name ASC"}.get(sort, "date DESC")
    total = conn.execute(f"SELECT COUNT(*) c FROM items{wsql}", args).fetchone()["c"]
    rows = conn.execute(f"SELECT * FROM items{wsql} ORDER BY {order} LIMIT ? OFFSET ?",
                        args + [limit, offset]).fetchall()
    conn.close()
    return {"total": total, "limit": limit, "offset": offset,
            "items": [row_to_card(r) for r in rows]}


@app.get("/filters")
def filters():
    conn = C.db()
    def distinct(col):
        return [r[0] for r in conn.execute(
            f"SELECT DISTINCT {col} FROM items WHERE {col}<>'' ORDER BY {col}").fetchall()]
    out = {"pilots": distinct("pilot_name"), "tracks": distinct("track_name"),
           "seasons": distinct("season"), "types": distinct("session_type"),
           "sources": distinct("source")}
    conn.close()
    return out


@app.get("/item/{item_id}")
def item(item_id: str):
    conn = C.db()
    r = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    if not r:
        raise HTTPException(404, "not found")
    related = json.loads(r["related_json"] or "[]")
    rel = [{"category": x["category"], "name": x["key"].split("/")[-1],
            "size": x.get("size"),
            "url": presign(x["key"], download_name=x["key"].split("/")[-1])} for x in related]
    card = row_to_card(r)
    card.update({
        "notes": r["notes"], "video_key": r["video_key"],
        "mp4": presign(r["video_key"], content_type="video/mp4"),
        "hls": (f"/api/hls/{r['id']}/master.m3u8") if r["hls_key"] else None,
        "annotation": presign(r["annotation_key"]) if r["annotation_key"] else None,
        "related": rel,
    })
    return card


def _check_admin(authorization):
    if not ADMIN_TOKEN or authorization != f"Bearer {ADMIN_TOKEN}":
        raise HTTPException(401, "unauthorized")


@app.post("/admin/item/{item_id}")
async def edit_item(item_id: str, payload: dict, authorization: str = Header(None)):
    _check_admin(authorization)
    allowed = {"pilot_name", "track_name", "car", "session_type", "title", "notes"}
    fields = {k: v for k, v in payload.items() if k in allowed}
    if not fields:
        raise HTTPException(400, "no editable fields")
    conn = C.db()
    sets = ", ".join(f"{k}=?" for k in fields)
    conn.execute(f"UPDATE items SET {sets}, edited=1, updated_at=? WHERE id=?",
                 list(fields.values()) + [C.now_iso(), item_id])
    conn.commit()
    r = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    if not r:
        raise HTTPException(404, "not found")
    # перезапись annotation JSON в S3 (схема 2.1.0) — best-effort
    try:
        ann = {
            "annotation_version": "2.1.0",
            "created_by": "BALCHUG Racing Catalog (manual edit)",
            "created_at": C.now_iso(),
            "session_info": {"pilot_full_name": r["pilot_name"], "pilot_code": r["pilot_code"],
                             "track_full_name": r["track_name"], "car_full_name": r["car"],
                             "session_type": r["session_type"], "session_date": r["date"]},
            "ui_metadata": {"display_title": r["title"]},
            "file_metadata": {"s3_key": r["video_key"]},
            "notes": r["notes"] or "",
        }
        akey = r["annotation_key"] or f"annotations/{r['date']}/{r['pilot_slug']}/{r['id']}_annotation.json"
        s3().put_object(Bucket=C.S3_BUCKET, Key=akey,
                        Body=json.dumps(ann, ensure_ascii=False).encode("utf-8"),
                        ContentType="application/json")
        if not r["annotation_key"]:
            conn2 = C.db(); conn2.execute("UPDATE items SET annotation_key=? WHERE id=?", (akey, item_id)); conn2.commit(); conn2.close()
    except Exception as e:
        return JSONResponse({"ok": True, "warn": f"annotation not written: {e}"})
    return {"ok": True}


@app.post("/enqueue_hls/{item_id}")
def enqueue_hls(item_id: str):
    conn = C.db()
    r = conn.execute("SELECT video_key, hls_key FROM items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    if not r:
        raise HTTPException(404, "not found")
    if r["hls_key"]:
        return {"status": "ready"}
    os.makedirs(HLS_QUEUE, exist_ok=True)
    with open(os.path.join(HLS_QUEUE, f"{item_id}.job"), "w") as f:
        f.write(r["video_key"])
    return {"status": "queued"}


@app.get("/hls/{item_id}/{path:path}")
def hls_proxy(item_id: str, path: str):
    # Прокси приватных HLS-объектов (same-origin, без публичного доступа к бакету).
    if ".." in path or path.startswith("/") or not item_id.isalnum():
        raise HTTPException(400, "bad path")
    key = f"hls_vod/{item_id}/{path}"
    try:
        obj = s3().get_object(Bucket=C.S3_BUCKET, Key=key)
    except Exception:
        raise HTTPException(404, "not found")
    ext = os.path.splitext(path)[1].lower()
    ct = HLS_CT.get(ext, "application/octet-stream")
    body = obj["Body"]
    cache = "no-cache" if ext == ".m3u8" else "public, max-age=86400"
    return StreamingResponse(body.iter_chunks(65536), media_type=ct,
                             headers={"Cache-Control": cache})


@app.delete("/admin/item/{item_id}")
def delete_item(item_id: str, authorization: str = Header(None)):
    _check_admin(authorization)
    conn = C.db()
    r = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
    if not r:
        conn.close()
        raise HTTPException(404, "not found")
    # Удаляем только объекты, принадлежащие записи 1:1: видео, превью, HLS VOD.
    # Телеметрию/отчёты/аннотации НЕ трогаем (общие по сессии, ценные).
    keys = []
    if r["video_key"]:
        keys.append(r["video_key"])
    if r["thumb_key"]:
        keys.append(r["thumb_key"])
    try:
        resp = s3().list_objects_v2(Bucket=C.S3_BUCKET, Prefix=f"hls_vod/{item_id}/")
        keys += [o["Key"] for o in resp.get("Contents", [])]
    except Exception:
        pass
    deleted = 0
    for k in keys:
        try:
            s3().delete_object(Bucket=C.S3_BUCKET, Key=k)
            deleted += 1
        except Exception:
            pass
    conn.execute("DELETE FROM items WHERE id=?", (item_id,))
    conn.commit()
    conn.close()
    return {"ok": True, "deleted_objects": deleted}


@app.get("/boris")
def boris():
    # «Кнопка для Бориса»: при подтверждении галочки выдаём админ-токен клиенту.
    # Это намеренно низкий порог (шуточный гейт) — архив публичный, правки некритичны.
    return {"token": ADMIN_TOKEN}


# ---- счётчик зрителей live (heartbeat, в памяти) ----
VIEWERS = {}          # session_id -> last_seen (epoch)
VIEWER_WINDOW = 25    # сек: сессия считается активной, если пинговала за это время


@app.get("/viewers")
def viewers(id: str = "", watching: int = 0):
    now = time.time()
    if id and watching:
        VIEWERS[id] = now
    for k in [k for k, t in VIEWERS.items() if now - t > VIEWER_WINDOW]:
        VIEWERS.pop(k, None)
    return JSONResponse({"count": len(VIEWERS)}, headers={"Cache-Control": "no-store"})


@app.get("/health")
def health():
    conn = C.db()
    n = conn.execute("SELECT COUNT(*) c FROM items").fetchone()["c"]
    conn.close()
    return {"ok": True, "items": n, "ts": int(time.time())}
