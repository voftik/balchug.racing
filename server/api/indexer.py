#!/usr/bin/env python3
"""
Индексатор каталога: сканирует S3-бакет, парсит имена, связывает видео с превью,
телеметрией, отчётами и аннотациями, обогащает русскими именами и пишет в SQLite.
Запуск: source venv && python indexer.py  (по cron ежечасно + после новых записей).
Ручные правки (edited=1) не перетираются.
"""
import os, sys, json, collections
import common as C

# Канонизация названий трасс (аннотации иногда дают иные написания)
TRACK_NORM = {
    "автодром игора драйв": "Igora Drive",
    "igora drive (rec)": "Igora Drive",
    "test track": "Тестовая трасса",
    "нижегородское кольцо (nring)": "Нижегородское кольцо",
}


def norm_track(name):
    return TRACK_NORM.get((name or "").strip().lower(), name)


def list_all(s3, prefix=""):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=C.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            yield obj["Key"], obj["Size"], obj["LastModified"].isoformat()


def main():
    s3 = C.s3()
    print("Листинг бакета…", flush=True)
    videos = []                 # (key,size,mtime) для onboard_video + stream_records
    thumbs = set()              # ключи thumbnails/*
    by_dateslug = collections.defaultdict(list)   # (date,slug) -> [keys телеметрии/отчётов]
    annotations = []            # (key, parsed json) — мало

    for key, size, mtime in list_all(s3):
        top = key.split("/", 1)[0]
        low = key.lower()
        if top in ("onboard_video", "stream_records") and low.endswith(C.VIDEO_EXT):
            videos.append((key, size, mtime))
        elif top == "thumbnails":
            thumbs.add(key)
        elif top in ("telemetry", "data_log", "report"):
            parts = key.split("/")
            if len(parts) >= 3:
                by_dateslug[(parts[1], parts[2])].append({"key": key, "size": size, "category": top})
        elif top == "annotations" and low.endswith(".json"):
            annotations.append(key)

    print(f"Видео: {len(videos)}, превью: {len(thumbs)}, аннотаций: {len(annotations)}", flush=True)

    # аннотации: грузим (их немного). Первичный ключ — точная привязка к видео
    # через file_metadata.s3_key; fallback — (date, pilot_code) ТОЛЬКО при непустом
    # коде пилота (у стримов код пуст, иначе все записи дня получали одну аннотацию).
    ann_by_s3key = {}
    ann_by_dateslug = {}
    for akey in annotations:
        try:
            body = s3.get_object(Bucket=C.S3_BUCKET, Key=akey)["Body"].read()
            data = json.loads(body)
            si = data.get("session_info", {})
            tm = data.get("technical_metadata", {})
            ui = data.get("ui_metadata", {})
            entry = {
                "key": akey, "best_lap": tm.get("best_lap_time"), "laps": tm.get("total_laps"),
                "duration": tm.get("session_duration_seconds"),
                "title": ui.get("display_title"),
                "summary": (data.get("ai_annotation") or {}).get("session_summary", ""),
                "pilot_name": si.get("pilot_full_name"), "track_name": si.get("track_full_name"),
                "car": si.get("car_full_name"),
                "session_type": si.get("session_type"),
            }
            skey = (data.get("file_metadata") or {}).get("s3_key") or ""
            if skey:
                ann_by_s3key[skey] = entry
            code = (si.get("pilot_code") or "").lower()
            if code:
                ann_by_dateslug[(si.get("session_date", ""), code)] = entry
        except Exception as e:
            print("annotation err", akey, e, file=sys.stderr)

    conn = C.db()
    cur = conn.cursor()
    n_new = n_upd = 0
    for key, size, mtime in videos:
        meta = C.parse_item(key, size, mtime)
        item_id = C.make_id(key)
        source = "live" if meta["category"] == "stream_records" else "onboard"
        if source == "live" and meta["session_type"] == "Сессия":
            meta["session_type"] = "Прямой эфир"

        # превью: thumbnails/<date>/<slug>/<filename>.jpg
        thumb_key = ""
        cand = f"thumbnails/{meta['date']}/{meta['pilot_slug']}/{meta['filename']}.jpg"
        if cand in thumbs:
            thumb_key = cand

        # связанные файлы (та же дата+пилот)
        related = by_dateslug.get((meta["date"], meta["pilot_slug"]), [])

        # обогащение аннотацией: сначала точно по s3_key, затем по date+code
        ann = ann_by_s3key.get(key)
        if ann is None and meta["pilot_code"]:
            ann = ann_by_dateslug.get((meta["date"], meta["pilot_code"].lower()))
        best_lap = laps = duration = None
        annotation_key = ""
        title = C.build_title(meta)
        summary = ""
        if ann:
            best_lap, laps, duration = ann.get("best_lap"), ann.get("laps"), ann.get("duration")
            annotation_key = ann.get("key", "")
            if ann.get("title"):
                title = ann["title"]
            if ann.get("track_name"):
                meta["track_name"] = norm_track(ann["track_name"])
            if ann.get("car"):
                meta["car"] = ann["car"]
            summary = ann.get("summary", "")
            if source == "live":
                # у стримов пилот/тип берём из аннотации (команда/Гонка/Тренировка…)
                if ann.get("pilot_name"):
                    meta["pilot_name"] = ann["pilot_name"]
                if ann.get("session_type"):
                    meta["session_type"] = ann["session_type"]

        # не перетираем ручные правки
        row = cur.execute("SELECT edited FROM items WHERE id=?", (item_id,)).fetchone()
        if row and row["edited"]:
            cur.execute("UPDATE items SET size=?, mtime=?, thumb_key=?, related_json=? WHERE id=?",
                        (size, mtime, thumb_key, json.dumps(related, ensure_ascii=False), item_id))
            n_upd += 1
            continue

        cur.execute("""
          INSERT INTO items (id,source,category,date,season,pilot_slug,pilot_code,pilot_name,
            track_name,car,session_type,title,notes,video_key,thumb_key,annotation_key,hls_key,
            size,duration,best_lap,laps,related_json,mtime,updated_at,edited)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)
          ON CONFLICT(id) DO UPDATE SET
            source=excluded.source, category=excluded.category, date=excluded.date,
            season=excluded.season, pilot_slug=excluded.pilot_slug, pilot_code=excluded.pilot_code,
            pilot_name=excluded.pilot_name, track_name=excluded.track_name, car=excluded.car,
            session_type=excluded.session_type, title=excluded.title, video_key=excluded.video_key,
            thumb_key=excluded.thumb_key, annotation_key=excluded.annotation_key,
            size=excluded.size, duration=excluded.duration, best_lap=excluded.best_lap,
            laps=excluded.laps, related_json=excluded.related_json, mtime=excluded.mtime,
            notes=excluded.notes, updated_at=excluded.updated_at
        """, (item_id, source, meta["category"], meta["date"], meta["season"],
              meta["pilot_slug"], meta["pilot_code"], meta["pilot_name"], meta["track_name"],
              meta["car"], meta["session_type"], title, summary, key, thumb_key, annotation_key,
              "", size, duration, best_lap, laps,
              json.dumps(related, ensure_ascii=False), mtime, C.now_iso()))
        n_new += 1

    conn.commit()
    total = cur.execute("SELECT COUNT(*) c FROM items").fetchone()["c"]
    conn.close()
    print(f"Готово. Обработано {n_new}, обновлено {n_upd}. Всего в каталоге: {total}", flush=True)


if __name__ == "__main__":
    main()
