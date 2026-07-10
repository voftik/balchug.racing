#!/usr/bin/env python3
"""
Одноразовый ретро-ремонт архива трансляций (запускать на сервере).

До внедрения session_merger каждый кусок эфира (реконнект LiveU) загружался в S3
отдельным видео с аннотацией-заглушкой. Скрипт:
  1) группирует существующие stream_records/* по фактическим интервалам
     (start из id + длительность из ffprobe по presigned URL);
  2) склеивает куски каждой сессии (ffmpeg concat, без перекодирования) в
     непубличный staging и только затем публикует объединённый MP4 под id первого куска;
  3) генерирует правильные аннотации (annotator: календарь + LLM);
  4) удаляет объекты лишних кусков (видео/превью/аннотации/hls_vod) и их строки
     в каталоге; сбрасывает устаревший HLS VOD объединённых записей и ставит их
     в очередь транскода;
  5) переиндексирует каталог.

Для групп из нескольких кусков перед публикацией сохраняется журнал в
repair_state/archive/ и staging в repair_staging/archive/. Если процесс оборвётся,
повторный --apply продолжит публикацию из staging, не склеивая уже заменённый target
с оставшимися кусками второй раз.

Запуск (env из /etc/balchug/secrets.env):
  dry-run (только план):  repair_archive.py
  выполнить:              repair_archive.py --apply
  только одна дата:       repair_archive.py --apply --date 2026-06-27
  без LLM (быстрее):      repair_archive.py --apply --no-llm
"""
import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, "/opt/balchug_racing/server/api")
import common as C          # noqa: E402
import annotator            # noqa: E402

GAP_SEC = int(float(os.environ.get("SESSION_GAP_MIN", "60")) * 60)
HLS_QUEUE = "/var/lib/balchug/hls_queue"
KEY_RE = re.compile(r"^stream_records/(\d{4}-\d{2}-\d{2})/live_(\d{2})(\d{2})(\d{2})/source\.mp4$")
STATE_PREFIX = "repair_state/archive"
STAGE_PREFIX = "repair_staging/archive"
S3_RETRY_COUNT = 3


def log(*a):
    print(*a, flush=True)


def presign(s3, key, ttl=6 * 3600):
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": C.S3_BUCKET, "Key": key}, ExpiresIn=ttl)


def ffprobe_duration(src):
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", src],
            timeout=300, stderr=subprocess.DEVNULL)
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


def s3_retry(action, label):
    """Повторяет транзиентную S3-операцию, но не маскирует окончательную ошибку."""
    error = None
    for attempt in range(S3_RETRY_COUNT):
        try:
            return action()
        except Exception as exc:
            error = exc
            if attempt + 1 < S3_RETRY_COUNT:
                time.sleep(attempt + 1)
    raise RuntimeError(f"S3-операция не выполнена: {label}") from error


def delete_object(s3, key):
    s3_retry(lambda: s3.delete_object(Bucket=C.S3_BUCKET, Key=key), f"delete {key}")


def delete_prefix(s3, prefix):
    """Удаляет все объекты префикса или завершается с ошибкой для безопасного resume."""
    n = 0
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=C.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            delete_object(s3, obj["Key"])
            n += 1
    return n


def state_key(date, rec_id):
    return f"{STATE_PREFIX}/{date}/{rec_id}.json"


def stage_prefix(date, rec_id):
    return f"{STAGE_PREFIX}/{date}/{rec_id}"


def is_not_found(exc):
    code = (getattr(exc, "response", {}) or {}).get("Error", {}).get("Code", "")
    return str(code) in {"404", "NoSuchKey", "NotFound"}


def load_state(s3, date, rec_id):
    key = state_key(date, rec_id)
    try:
        body = s3.get_object(Bucket=C.S3_BUCKET, Key=key)["Body"].read()
    except Exception as exc:
        if is_not_found(exc):
            return None
        raise RuntimeError(f"не удалось прочитать журнал ремонта {date}/{rec_id}") from exc
    try:
        state = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"журнал ремонта {date}/{rec_id} повреждён") from exc
    if state.get("version") != 1 or state.get("target_key") == "":
        raise RuntimeError(f"журнал ремонта {date}/{rec_id} имеет неизвестный формат")
    return state


def save_state(s3, state):
    payload = json.dumps(state, ensure_ascii=False, sort_keys=True).encode("utf-8")
    s3_retry(
        lambda: s3.put_object(Bucket=C.S3_BUCKET, Key=state["state_key"], Body=payload,
                              ContentType="application/json"),
        f"save {state['state_key']}")


def stage_exists(s3, key):
    try:
        s3.head_object(Bucket=C.S3_BUCKET, Key=key)
        return True
    except Exception as exc:
        if is_not_found(exc):
            return False
        raise RuntimeError(f"не удалось проверить staging-объект {key}") from exc


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


def normalize_and_merge(s3, group, tmp):
    """Собирает локальный MP4 из исходных частей, не изменяя S3."""
    paths = []
    for i, item in enumerate(group):
        part = os.path.join(tmp, f"part{i:03d}.mp4")
        log(f"    ↓ {item['key']}")
        s3.download_file(C.S3_BUCKET, item["key"], part)
        try:
            start = float(subprocess.check_output(
                ["ffprobe", "-v", "error", "-show_entries", "format=start_time",
                 "-of", "default=noprint_wrappers=1:nokey=1", part],
                timeout=120, stderr=subprocess.DEVNULL).decode().strip())
        except Exception:
            start = 0.0
        normalized = os.path.join(tmp, f"norm{i:03d}.mp4")
        command = ["ffmpeg", "-y", "-loglevel", "error", "-i", part, "-c", "copy"]
        if start > 0.01:
            command += ["-output_ts_offset", f"-{start:.3f}"]
        command += ["-avoid_negative_ts", "make_zero", normalized]
        subprocess.run(command, check=True)
        paths.append(normalized)

    merged = os.path.join(tmp, "source.mp4")
    if len(paths) == 1:
        shutil.copyfile(paths[0], merged)
    else:
        listing = os.path.join(tmp, "list.txt")
        with open(listing, "w") as f:
            for path in paths:
                f.write(f"file '{path}'\n")
        log("    склейка…")
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
                        "-i", listing, "-c", "copy", "-movflags", "+faststart", merged],
                       check=True)
    return merged


def create_thumbnail(video_path, duration, tmp):
    jpg = os.path.join(tmp, "thumb.jpg")
    try:
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                        "-ss", str(max(1, int((duration or 10) / 2))), "-i", video_path,
                        "-vframes", "1", "-vf", "scale=640:-1", jpg], check=True)
        return jpg
    except Exception:
        return None


def stage_group(s3, group, use_llm):
    """Готовит полную замену в staging. До записи state исходные объекты не меняются."""
    target = group[0]
    date, rec_id = target["date"], target["rec_id"]
    video_key = target["key"]
    prefix = stage_prefix(date, rec_id)
    tmp = tempfile.mkdtemp(prefix="balchug_repair_")
    try:
        merged = normalize_and_merge(s3, group, tmp)
        duration = ffprobe_duration(merged)
        if not duration:
            raise RuntimeError("не удалось определить длительность staged MP4")
        thumb = create_thumbnail(merged, duration, tmp)
        parts = [{"start": item["start_dt"].strftime("%H:%M:%S"),
                  "duration_seconds": round(item.get("duration") or 0, 1)} for item in group]
        ann = annotator.annotate(
            date=date, start_hms=target["start_dt"].strftime("%H:%M:%S"),
            duration_sec=duration, video_key=video_key, video_path=merged,
            parts=parts, use_llm=use_llm)

        staged_video = f"{prefix}/source.mp4"
        staged_thumb = f"{prefix}/thumb.jpg" if thumb else ""
        staged_annotation = f"{prefix}/annotation.json"
        s3_retry(lambda: s3.upload_file(merged, C.S3_BUCKET, staged_video,
                                         ExtraArgs={"ContentType": "video/mp4"}),
                 f"upload {staged_video}")
        if thumb:
            s3_retry(lambda: s3.upload_file(thumb, C.S3_BUCKET, staged_thumb,
                                             ExtraArgs={"ContentType": "image/jpeg"}),
                     f"upload {staged_thumb}")
        s3_retry(
            lambda: s3.put_object(Bucket=C.S3_BUCKET, Key=staged_annotation,
                                  Body=json.dumps(ann, ensure_ascii=False).encode("utf-8"),
                                  ContentType="application/json"),
            f"upload {staged_annotation}")

        state = {
            "version": 1,
            "state_key": state_key(date, rec_id),
            "target_key": video_key,
            "target_thumb_key": f"thumbnails/{date}/{rec_id}/source.mp4.jpg",
            "target_annotation_key": annotator.annotation_key(date, rec_id),
            "staged_video_key": staged_video,
            "staged_thumb_key": staged_thumb,
            "staged_annotation_key": staged_annotation,
            "sources": [{"key": item["key"], "date": item["date"], "rec_id": item["rec_id"]}
                        for item in group],
        }
        save_state(s3, state)
        log(f"    staging готов: {len(group)} частей, {(duration or 0):.0f}с")
        return state
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def copy_staged(s3, source_key, target_key, label):
    if not stage_exists(s3, source_key):
        raise RuntimeError(f"отсутствует staging-объект {source_key}")
    s3_retry(lambda: s3.copy({"Bucket": C.S3_BUCKET, "Key": source_key}, C.S3_BUCKET, target_key),
             label)


def commit_state(s3, state):
    """Публикует уже подготовленную замену; повторный запуск воспроизводит те же шаги."""
    video_key = state["target_key"]
    sources = state["sources"]
    copy_staged(s3, state["staged_video_key"], video_key, f"publish {video_key}")
    if state.get("staged_thumb_key"):
        copy_staged(s3, state["staged_thumb_key"], state["target_thumb_key"],
                    f"publish {state['target_thumb_key']}")
    copy_staged(s3, state["staged_annotation_key"], state["target_annotation_key"],
                f"publish {state['target_annotation_key']}")

    removed_ids = []
    for item in sources[1:]:
        delete_object(s3, item["key"])
        delete_prefix(s3, f"annotations/{item['date']}/{item['rec_id']}/")
        delete_prefix(s3, f"thumbnails/{item['date']}/{item['rec_id']}/")
        delete_prefix(s3, f"hls_vod/{C.make_id(item['key'])}/")
        removed_ids.append(C.make_id(item["key"]))
    if removed_ids:
        db_delete(removed_ids)

    delete_prefix(s3, f"hls_vod/{C.make_id(video_key)}/")
    db_clear_hls(C.make_id(video_key))
    enqueue_hls(video_key)
    # Сначала убираем журнал: если затем не удастся подчистить staging, останутся
    # только безопасные временные объекты, а не state без исходника для resume.
    delete_object(s3, state["state_key"])
    delete_prefix(s3, os.path.dirname(state["staged_video_key"]) + "/")
    log(f"    готово: удалено кусков {len(sources) - 1}")


def reannotate_single(s3, group, use_llm):
    target = group[0]
    ann = annotator.annotate(
        date=target["date"], start_hms=target["start_dt"].strftime("%H:%M:%S"),
        duration_sec=target.get("duration"), video_key=target["key"],
        video_path=presign(s3, target["key"]), parts=None, use_llm=use_llm)
    annotator.upload(s3, C.S3_BUCKET, ann, target["date"], target["rec_id"])
    log(f"    аннотация: {ann['ui_metadata']['display_title']}")


def process_group(s3, group, apply, use_llm):
    target = group[0]
    date, rec_id = target["date"], target["rec_id"]
    total = sum(item.get("duration") or 0 for item in group)
    log(f"\n[{date}] сессия {rec_id}: кусков {len(group)}, суммарно {total / 60:.1f} мин")
    if not apply:
        return

    state = load_state(s3, date, rec_id)
    if state:
        if state["target_key"] != target["key"]:
            raise RuntimeError("журнал ремонта не соответствует целевой записи")
        log("    возобновление из staging…")
        commit_state(s3, state)
    elif len(group) == 1:
        reannotate_single(s3, group, use_llm)
    else:
        state = stage_group(s3, group, use_llm)
        commit_state(s3, state)


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
    duration_errors = 0
    for it in items:
        it["duration"] = ffprobe_duration(presign(s3, it["key"]))
        if it["duration"] is None:
            duration_errors += 1
            log(f"  ERR: не удалось получить длительность {it['key']}")
    if duration_errors:
        raise SystemExit(f"Не удалось прочитать длительность {duration_errors} записей; ремонт остановлен.")

    groups = group_by_gap(items)
    merged_n = sum(1 for g in groups if len(g) > 1)
    log(f"\nПлан: {len(items)} кусков → {len(groups)} записей "
        f"(склеек: {merged_n}, зазор сессии: {GAP_SEC // 60} мин)"
        + ("" if args.apply else "  [DRY-RUN]"))

    failed = 0
    for group in groups:
        try:
            process_group(s3, group, args.apply, not args.no_llm)
        except Exception as e:
            failed += 1
            log(f"    ERR группа {group[0]['rec_id']}: {type(e).__name__}")
    if failed:
        raise SystemExit(f"Групп с ошибками: {failed}; staging сохранён для безопасного resume.")

    if args.apply:
        log("\nПереиндексация каталога…")
        import indexer
        indexer.main()
        log("Готово.")


if __name__ == "__main__":
    main()
