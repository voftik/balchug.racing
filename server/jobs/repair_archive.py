#!/usr/bin/env python3
"""
Одноразовый ретро-ремонт архива трансляций (запускать на сервере).

До внедрения session_merger каждый кусок эфира (реконнект LiveU) загружался в S3
отдельным видео с аннотацией-заглушкой. Скрипт:
  1) группирует существующие stream_records/* по фактическим интервалам
     (start из id + длительность из ffprobe по presigned URL);
  2) склеивает куски каждой сессии (ffmpeg concat, без перекодирования) и
     заливает объединённый MP4 под id первого куска;
  3) генерирует правильные аннотации (annotator: календарь + LLM);
  4) удаляет объекты лишних кусков (видео/превью/аннотации/hls_vod) и их строки
     в каталоге; сбрасывает устаревший HLS VOD объединённых записей и ставит их
     в очередь транскода;
  5) переиндексирует каталог.

Запуск (env из /etc/balchug/secrets.env):
  dry-run (только план):  repair_archive.py
  выполнить:              repair_archive.py --apply
  только одна дата:       repair_archive.py --apply --date 2026-06-27
  без LLM (быстрее):      repair_archive.py --apply --no-llm
"""
import argparse
import datetime
import os
import re
import subprocess
import sys
import tempfile
import shutil

sys.path.insert(0, "/opt/balchug_racing/server/api")
import common as C          # noqa: E402
import annotator            # noqa: E402

GAP_SEC = int(float(os.environ.get("SESSION_GAP_MIN", "30")) * 60)
HLS_QUEUE = "/var/lib/balchug/hls_queue"
KEY_RE = re.compile(r"^stream_records/(\d{4}-\d{2}-\d{2})/live_(\d{2})(\d{2})(\d{2})/source\.mp4$")


def log(*a):
    print(*a, flush=True)


def presign(s3, key, ttl=6 * 3600):
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": C.S3_BUCKET, "Key": key}, ExpiresIn=ttl)


def ffprobe_duration(src):
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", src], timeout=300)
        return float(out.decode().strip())
    except Exception:
        return None


def list_stream_records(s3, only_date=None):
    """[{key, date, rec_id, start_dt}] отсортированные по дате/времени."""
    items = []
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=C.S3_BUCKET, Prefix="stream_records/"):
        for o in page.get("Contents", []):
            m = KEY_RE.match(o["Key"])
            if not m:
                continue
            date = m.group(1)
            if only_date and date != only_date:
                continue
            start_dt = datetime.datetime.strptime(
                f"{date} {m.group(2)}:{m.group(3)}:{m.group(4)}", "%Y-%m-%d %H:%M:%S")
            items.append({"key": o["Key"], "date": date, "size": o["Size"],
                          "rec_id": f"live_{m.group(2)}{m.group(3)}{m.group(4)}",
                          "start_dt": start_dt})
    items.sort(key=lambda x: x["start_dt"])
    return items


def group_by_gap(items):
    """Группы кусков одной сессии (по датам, зазор < GAP_SEC)."""
    groups, cur, cur_end = [], [], None
    for it in items:
        if cur and (it["date"] != cur[-1]["date"]
                    or (it["start_dt"] - cur_end).total_seconds() > GAP_SEC):
            groups.append(cur)
            cur, cur_end = [], None
        cur.append(it)
        end = it["start_dt"] + datetime.timedelta(seconds=it.get("duration") or 0)
        cur_end = max(cur_end, end) if cur_end else end
    if cur:
        groups.append(cur)
    return groups


def delete_prefix(s3, prefix):
    n = 0
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=C.S3_BUCKET, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=C.S3_BUCKET, Delete={"Objects": keys})
            n += len(keys)
    return n


def db_delete(item_ids):
    conn = C.db()
    for iid in item_ids:
        conn.execute("DELETE FROM items WHERE id=?", (iid,))
    conn.commit()
    conn.close()


def db_clear_hls(item_id):
    conn = C.db()
    conn.execute("UPDATE items SET hls_key='' WHERE id=?", (item_id,))
    conn.commit()
    conn.close()


def enqueue_hls(video_key):
    os.makedirs(HLS_QUEUE, exist_ok=True)
    with open(os.path.join(HLS_QUEUE, f"{C.make_id(video_key)}.job"), "w") as f:
        f.write(video_key)


def process_group(s3, group, apply, use_llm):
    tgt = group[0]
    date, rec_id = tgt["date"], tgt["rec_id"]
    tstr = tgt["start_dt"].strftime("%H:%M:%S")
    total = sum(g.get("duration") or 0 for g in group)
    log(f"\n[{date}] сессия {rec_id} ({tstr}): кусков {len(group)}, суммарно {total / 60:.1f} мин")
    for g in group:
        log(f"    {g['rec_id']}  {(g.get('duration') or 0):7.0f}с  {g['size'] / 1e6:6.1f}МБ  {g['key']}")
    if not apply:
        return

    tmp = tempfile.mkdtemp(prefix="balchug_repair_")
    try:
        video_key = tgt["key"]
        if len(group) == 1:
            dur = tgt.get("duration")
            video_path = presign(s3, video_key)      # кадры для LLM — прямо из S3
        else:
            paths = []
            for i, g in enumerate(group):
                p = os.path.join(tmp, f"part{i:03d}.mp4")
                log(f"    ↓ {g['key']}")
                s3.download_file(C.S3_BUCKET, g["key"], p)
                paths.append(p)
            lst = os.path.join(tmp, "list.txt")
            with open(lst, "w") as f:
                for p in paths:
                    f.write(f"file '{p}'\n")
            merged = os.path.join(tmp, "source.mp4")
            log("    склейка…")
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
                            "-fflags", "+genpts", "-i", lst, "-c", "copy",
                            "-movflags", "+faststart", merged], check=True)
            dur = ffprobe_duration(merged)
            log(f"    ↑ {video_key} ({(dur or 0):.0f}с)")
            s3.upload_file(merged, C.S3_BUCKET, video_key, ExtraArgs={"ContentType": "video/mp4"})
            jpg = os.path.join(tmp, "thumb.jpg")
            try:
                subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                                "-ss", str(max(1, int((dur or 10) / 2))), "-i", merged,
                                "-vframes", "1", "-vf", "scale=640:-1", jpg], check=True)
                s3.upload_file(jpg, C.S3_BUCKET, f"thumbnails/{date}/{rec_id}/source.mp4.jpg",
                               ExtraArgs={"ContentType": "image/jpeg"})
            except Exception:
                pass
            video_path = merged

        # старые аннотации всех кусков — под нож (включая целевой)
        for g in group:
            delete_prefix(s3, f"annotations/{g['date']}/{g['rec_id']}/")

        parts_meta = [{"start": g["start_dt"].strftime("%H:%M:%S"),
                       "duration_seconds": round(g.get("duration") or 0, 1)} for g in group]
        ann = annotator.annotate(date=date, start_hms=tstr, duration_sec=dur,
                                 video_key=video_key, video_path=video_path,
                                 parts=parts_meta if len(group) > 1 else None,
                                 use_llm=use_llm)
        annotator.upload(s3, C.S3_BUCKET, ann, date, rec_id)
        log(f"    аннотация: {ann['ui_metadata']['display_title']}")

        # лишние куски: объекты + строки каталога
        removed_ids = []
        for g in group[1:]:
            s3.delete_object(Bucket=C.S3_BUCKET, Key=g["key"])
            delete_prefix(s3, f"thumbnails/{g['date']}/{g['rec_id']}/")
            delete_prefix(s3, f"hls_vod/{C.make_id(g['key'])}/")
            removed_ids.append(C.make_id(g["key"]))
        if removed_ids:
            db_delete(removed_ids)

        # целевая запись: HLS VOD пересобрать (старый — от куска)
        if len(group) > 1:
            delete_prefix(s3, f"hls_vod/{C.make_id(video_key)}/")
            db_clear_hls(C.make_id(video_key))
            enqueue_hls(video_key)
        log(f"    готово: {rec_id} (удалено кусков: {len(group) - 1})")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="выполнить (без флага — dry-run)")
    ap.add_argument("--date", help="обработать только эту дату (YYYY-MM-DD)")
    ap.add_argument("--no-llm", action="store_true", help="без LLM-слоя аннотации")
    args = ap.parse_args()

    s3 = C.s3()
    log("Листинг stream_records…")
    items = list_stream_records(s3, args.date)
    log(f"Найдено записей: {len(items)}")
    if not items:
        return

    log("Определение длительностей (ffprobe по presigned URL)…")
    for it in items:
        it["duration"] = ffprobe_duration(presign(s3, it["key"]))
        if it["duration"] is None:
            log(f"  WARN: не удалось получить длительность {it['key']} — считаю 0")
            it["duration"] = 0.0

    groups = group_by_gap(items)
    merged_n = sum(1 for g in groups if len(g) > 1)
    log(f"\nПлан: {len(items)} кусков → {len(groups)} записей "
        f"(склеек: {merged_n}, зазор сессии: {GAP_SEC // 60} мин)"
        + ("" if args.apply else "  [DRY-RUN]"))

    for group in groups:
        process_group(s3, group, args.apply, not args.no_llm)

    if args.apply:
        log("\nПереиндексация каталога…")
        import indexer
        indexer.main()
        log("Готово.")


if __name__ == "__main__":
    main()
