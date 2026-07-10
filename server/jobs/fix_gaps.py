#!/usr/bin/env python3
"""
Починка «дырявого» таймлайна в записях stream_records (запускать на сервере).

Причина: куски записи (FLV от реконнектов LiveU) могли иметь ненулевой
внутренний старт таймстемпов; при склейке `ffmpeg concat -c copy` контент
уезжал вперёд, оставляя дыры в таймлайне — плеер замирает на границе куска,
а длительность контейнера завышена.

Что делает:
  1) скан: полный проход по DTS видеопакетов (демакс по presigned URL) —
     находит регионы непрерывного контента (разрыв > GAP_T секунд = дыра);
  2) --apply: скачивает файл, вырезает каждый регион (`-ss/-to -c copy`,
     `-avoid_negative_ts make_zero` — таймстемпы региона с нуля), склеивает
     регионы подряд и проверяет непрерывность. Исправленный файл сначала
     попадает в непубличный staging, а затем публикуется вместе с метаданными;
     журнал позволяет безопасно завершить прерванный запуск без повторной обрезки.
     Для нескольких регионов нужен MP4Box из пакета `gpac`.

Запуск (env из /etc/balchug/secrets.env):
  скан всех:        fix_gaps.py
  чинить все битые: fix_gaps.py --apply
  один файл:        fix_gaps.py --apply --key stream_records/.../source.mp4
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, "/opt/balchug_racing/server/api")
import common as C          # noqa: E402

# Порог дыры: мелкие пропуски (2–30с) — фризы самого потока LiveU, их не трогаем
# (рез не по keyframe опасен); крупные (>30с) — границы склеенных кусков
# (начинаются с keyframe нового паблиша) — именно они вешают плеер.
GAP_T = float(os.environ.get("FIX_GAP_T", "30"))
START_T = 0.5      # старт первого пакета позже этого — смещённый таймлайн
HLS_QUEUE = "/var/lib/balchug/hls_queue"
STATE_PREFIX = "repair_state/gaps"
STAGE_PREFIX = "repair_staging/gaps"
S3_RETRY_COUNT = 3


class ScanError(RuntimeError):
    """Безопасная для вывода ошибка чтения видео."""


def log(*a):
    print(*a, flush=True)


def presign(s3, key, ttl=6 * 3600):
    return s3.generate_presigned_url("get_object",
                                     Params={"Bucket": C.S3_BUCKET, "Key": key}, ExpiresIn=ttl)


def s3_retry(action, label):
    """Повторяет временные S3-сбои, не раскрывая текст SDK-исключения в логах."""
    error = None
    for attempt in range(S3_RETRY_COUNT):
        try:
            return action()
        except Exception as exc:
            error = exc
            if attempt + 1 < S3_RETRY_COUNT:
                time.sleep(attempt + 1)
    raise RuntimeError(f"S3-операция не выполнена: {label}") from error


def is_not_found(exc):
    code = (getattr(exc, "response", {}) or {}).get("Error", {}).get("Code", "")
    return str(code) in {"404", "NoSuchKey", "NotFound"}


def state_key(video_key):
    return f"{STATE_PREFIX}/{C.make_id(video_key)}.json"


def stage_prefix(video_key):
    return f"{STAGE_PREFIX}/{C.make_id(video_key)}"


def load_state(s3, video_key):
    key = state_key(video_key)
    try:
        body = s3.get_object(Bucket=C.S3_BUCKET, Key=key)["Body"].read()
    except Exception as exc:
        if is_not_found(exc):
            return None
        raise RuntimeError(f"не удалось прочитать журнал ремонта {C.make_id(video_key)}") from exc
    try:
        state = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"журнал ремонта {C.make_id(video_key)} повреждён") from exc
    if state.get("version") != 1 or state.get("target_key") != video_key:
        raise RuntimeError(f"журнал ремонта {C.make_id(video_key)} имеет неизвестный формат")
    return state


def save_state(s3, state):
    s3_retry(
        lambda: s3.put_object(
            Bucket=C.S3_BUCKET, Key=state["state_key"],
            Body=json.dumps(state, ensure_ascii=False, sort_keys=True).encode("utf-8"),
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


def delete_object(s3, key):
    s3_retry(lambda: s3.delete_object(Bucket=C.S3_BUCKET, Key=key), f"delete {key}")


def delete_prefix(s3, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=C.S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            delete_object(s3, obj["Key"])


def copy_staged(s3, source_key, target_key):
    if not stage_exists(s3, source_key):
        raise RuntimeError(f"отсутствует staging-объект {source_key}")
    s3_retry(lambda: s3.copy({"Bucket": C.S3_BUCKET, "Key": source_key}, C.S3_BUCKET, target_key),
             f"publish {target_key}")


def ffprobe_output(args, timeout, context):
    """Запускает ffprobe, не давая подписанному URL попасть в stderr или ошибку."""
    try:
        return subprocess.check_output(args, timeout=timeout, stderr=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        raise ScanError(f"{context}: таймаут ffprobe") from None
    except subprocess.CalledProcessError:
        raise ScanError(f"{context}: ffprobe не смог прочитать видео") from None
    except OSError:
        raise ScanError(f"{context}: ffprobe недоступен") from None


def video_dts_list(src):
    """Все DTS видеопакетов (сек). Демакс без декодирования."""
    try:
        out = ffprobe_output(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "packet=dts_time", "-of", "csv=p=0", src],
            timeout=3600, context="скан DTS").decode()
    except UnicodeDecodeError:
        raise ScanError("скан DTS: некорректный ответ ffprobe") from None
    dts = []
    for line in out.splitlines():
        line = line.strip().rstrip(",")
        if not line or line == "N/A":
            continue
        try:
            dts.append(float(line))
        except ValueError:
            continue
    if not dts:
        raise ScanError("скан DTS: видеопакеты не найдены")
    return dts


def find_regions(dts):
    """[(start, end)] непрерывных участков по видео-DTS."""
    regions = []
    if not dts:
        return regions
    a = prev = dts[0]
    for t in dts[1:]:
        if t - prev > GAP_T:
            regions.append((a, prev))
            a = t
        prev = t
    regions.append((a, prev))
    return regions


def scan(s3, key):
    url = presign(s3, key)
    dts = video_dts_list(url)
    regions = find_regions(dts)
    fmt_dur = None
    try:
        fmt_dur = float(ffprobe_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", url],
            timeout=300, context="чтение длительности").decode().strip())
    except (ScanError, UnicodeDecodeError, ValueError):
        pass
    content = sum(b - a for a, b in regions)
    broken = len(regions) > 1 or (regions and regions[0][0] > START_T)
    return {"key": key, "regions": regions, "content": content,
            "container": fmt_dur, "broken": broken}


def parse_record_key(key):
    parts = key.split("/")
    if len(parts) != 4 or parts[0] != "stream_records" or parts[-1] != "source.mp4":
        raise RuntimeError("неверный формат ключа stream_records")
    return parts[1], parts[2]


def stage_annotation(s3, target_key, duration, stage_key_):
    """Готовит обновлённую аннотацию в staging, не меняя публикуемый объект."""
    date, rec_id = parse_record_key(target_key)
    target_annotation = f"annotations/{date}/{rec_id}/{rec_id}_annotation.json"
    try:
        body = s3.get_object(Bucket=C.S3_BUCKET, Key=target_annotation)["Body"].read()
    except Exception as exc:
        if is_not_found(exc):
            return "", target_annotation
        raise RuntimeError("не удалось прочитать аннотацию для staging") from exc
    try:
        ann = json.loads(body)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("аннотация для staging повреждена") from exc
    ann.setdefault("technical_metadata", {})["session_duration_seconds"] = duration
    s3_retry(
        lambda: s3.put_object(Bucket=C.S3_BUCKET, Key=stage_key_,
                              Body=json.dumps(ann, ensure_ascii=False).encode("utf-8"),
                              ContentType="application/json"),
        f"upload {stage_key_}")
    return stage_key_, target_annotation


def rebuild(s3, info):
    """Готовит исправленный MP4 в staging и возвращает журнал для безопасной публикации."""
    key = info["key"]
    regions = info["regions"]
    tmp = tempfile.mkdtemp(prefix="balchug_fix_", dir="/var/tmp")
    try:
        src = os.path.join(tmp, "src.mp4")
        log(f"    ↓ скачивание ({key})…")
        s3.download_file(C.S3_BUCKET, key, src)

        parts = []
        for i, (a, b) in enumerate(regions):
            p = os.path.join(tmp, f"part{i:03d}.mp4")
            # ВАЖНО: -ss/-to как OUTPUT-опции (точный дроп пакетов, без сика
            # к keyframe ПЕРЕД целью — иначе захватывается хвост предыдущего
            # региона вместе с дырой). Регион начинается с keyframe нового
            # паблиша. Сдвиг к нулю — явный (-output_ts_offset), т.к. make_zero
            # не сдвигает положительные старты.
            cmd = ["ffmpeg", "-y", "-loglevel", "error", "-i", src,
                   "-ss", f"{a:.3f}", "-to", f"{b + 0.05:.3f}",
                   "-c", "copy", "-output_ts_offset", f"-{a:.3f}",
                   "-avoid_negative_ts", "make_zero", p]
            subprocess.run(cmd, check=True, timeout=7200)
            parts.append(p)

        fixed = os.path.join(tmp, "fixed.mp4")
        if len(parts) == 1:
            # уже вырезан с нормализацией — только faststart
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", parts[0],
                            "-c", "copy", "-movflags", "+faststart", fixed],
                           check=True, timeout=3600)
        else:
            # Склейка встык через MP4Box (gpac): в отличие от ffmpeg concat demuxer
            # он пересчитывает таймлайны дорожек, игнорируя стартовые смещения
            # и расхождения контейнерных длительностей частей.
            raw = os.path.join(tmp, "joined.mp4")
            cmd = ["MP4Box"]
            for p in parts:
                cmd += ["-cat", p]
            cmd += ["-new", raw]
            subprocess.run(cmd, check=True, timeout=7200,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", raw,
                            "-c", "copy", "-movflags", "+faststart", fixed],
                           check=True, timeout=3600)
            try:
                os.remove(raw)
            except OSError:
                pass

        # контроль: непрерывность результата
        chk = find_regions(video_dts_list(fixed))
        if len(chk) != 1 or chk[0][0] > START_T:
            raise RuntimeError(f"после пересборки таймлайн всё ещё дырявый: {chk[:5]}")
        new_dur = chk[0][1] - chk[0][0]

        # превью из середины
        jpg = os.path.join(tmp, "thumb.jpg")
        try:
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error",
                            "-ss", str(max(1, int(new_dur / 2))), "-i", fixed,
                            "-vframes", "1", "-vf", "scale=640:-1", jpg],
                           check=True, timeout=600)
        except Exception:
            jpg = None

        date, rec_id = parse_record_key(key)
        prefix = stage_prefix(key)
        staged_video = f"{prefix}/source.mp4"
        staged_thumb = f"{prefix}/thumb.jpg" if jpg and os.path.exists(jpg) else ""
        staged_annotation, target_annotation = stage_annotation(
            s3, key, new_dur, f"{prefix}/annotation.json")
        s3_retry(lambda: s3.upload_file(fixed, C.S3_BUCKET, staged_video,
                                         ExtraArgs={"ContentType": "video/mp4"}),
                 f"upload {staged_video}")
        if staged_thumb:
            s3_retry(lambda: s3.upload_file(jpg, C.S3_BUCKET, staged_thumb,
                                             ExtraArgs={"ContentType": "image/jpeg"}),
                     f"upload {staged_thumb}")

        state = {
            "version": 1,
            "state_key": state_key(key),
            "target_key": key,
            "duration": new_dur,
            "target_thumb_key": f"thumbnails/{date}/{rec_id}/source.mp4.jpg",
            "target_annotation_key": target_annotation,
            "staged_video_key": staged_video,
            "staged_thumb_key": staged_thumb,
            "staged_annotation_key": staged_annotation,
        }
        save_state(s3, state)
        log(f"    staging готов: контент {new_dur / 60:.1f} мин")
        return state
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def commit_state(s3, state):
    """Публикует staged repair; повторный запуск завершает те же идемпотентные шаги."""
    key = state["target_key"]
    copy_staged(s3, state["staged_video_key"], key)
    if state.get("staged_thumb_key"):
        copy_staged(s3, state["staged_thumb_key"], state["target_thumb_key"])
    if state.get("staged_annotation_key"):
        copy_staged(s3, state["staged_annotation_key"], state["target_annotation_key"])

    item_id = C.make_id(key)
    conn = C.db()
    try:
        conn.execute("UPDATE items SET duration=?, hls_key='' WHERE id=?", (state["duration"], item_id))
        conn.commit()
    finally:
        conn.close()
    delete_prefix(s3, f"hls_vod/{item_id}/")
    os.makedirs(HLS_QUEUE, exist_ok=True)
    with open(os.path.join(HLS_QUEUE, f"{item_id}.job"), "w") as f:
        f.write(key)
    # Журнал удаляется раньше staging: сбой очистки оставит только мусор,
    # но не заблокирует следующий запуск state без staged source.
    delete_object(s3, state["state_key"])
    delete_prefix(s3, os.path.dirname(state["staged_video_key"]) + "/")
    return state["duration"]


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="чинить (без флага — только скан)")
    ap.add_argument("--key", action="append", help="конкретный ключ (можно несколько)")
    args = ap.parse_args(argv)

    s3 = C.s3()
    if args.key:
        keys = args.key
    else:
        keys = []
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=C.S3_BUCKET, Prefix="stream_records/"):
            for o in page.get("Contents", []):
                if o["Key"].endswith(".mp4"):
                    keys.append(o["Key"])

    broken_n = fixed_n = scan_errors = fix_errors = 0
    repair_infos = []
    pending_states = []
    for key in keys:
        if args.apply:
            try:
                state = load_state(s3, key)
            except Exception as exc:
                scan_errors += 1
                log(f"[SCAN ERR] {key}: журнал ремонта недоступен ({type(exc).__name__})")
                continue
            if state:
                pending_states.append(state)
                broken_n += 1
                log(f"[PENDING] {key}: найден незавершённый безопасный repair, будет resume")
                continue
        try:
            info = scan(s3, key)
        except ScanError as e:
            scan_errors += 1
            log(f"[SCAN ERR] {key}: {e}")
            continue
        except Exception as e:
            scan_errors += 1
            # Не выводим текст исключения: он может содержать presigned URL.
            log(f"[SCAN ERR] {key}: непредвиденная ошибка ({type(e).__name__})")
            continue
        if not info["regions"]:
            scan_errors += 1
            log(f"[SCAN ERR] {key}: видеопакеты не найдены")
            continue
        holes = len(info["regions"]) - 1
        shift = info["regions"][0][0]
        status = "БИТЫЙ" if info["broken"] else "ок"
        log(f"[{status}] {key}: регионов {len(info['regions'])} (дыр {holes}, "
            f"старт {shift:.1f}с), контент {info['content'] / 60:.1f} мин, "
            f"контейнер {(info['container'] or 0) / 60:.1f} мин")
        if info["broken"]:
            broken_n += 1
            repair_infos.append(info)

    # Не начинаем частичный --apply: сперва нужен успешный скан всех выбранных
    # записей, а затем проверка MP4Box до первой перезаписи S3.
    if args.apply and scan_errors:
        log("[FIX ERR] ремонт не начат: сканирование завершилось с ошибками")
    elif args.apply and any(len(info["regions"]) > 1 for info in repair_infos) and \
            shutil.which("MP4Box") is None:
        fix_errors += 1
        log("[FIX ERR] MP4Box не найден; установите пакет gpac и повторите")
    elif args.apply:
        for state in pending_states:
            key = state["target_key"]
            try:
                new_dur = commit_state(s3, state)
                fixed_n += 1
                log(f"    возобновлён: {new_dur / 60:.1f} мин непрерывно")
            except Exception as exc:
                fix_errors += 1
                log(f"    FIX ERR: {key}: непредвиденная ошибка ({type(exc).__name__})")
        for info in repair_infos:
            key = info["key"]
            try:
                state = rebuild(s3, info)
                new_dur = commit_state(s3, state)
                fixed_n += 1
                log(f"    починен: {new_dur / 60:.1f} мин непрерывно")
            except Exception as e:
                fix_errors += 1
                # Аналогично scan: детали subprocess/S3-исключений не безопасны для лога.
                log(f"    FIX ERR: {key}: непредвиденная ошибка ({type(e).__name__})")

    log(f"\nИтого: битых {broken_n}" + (f", починено {fixed_n}" if args.apply else "  [скан]"))
    if scan_errors or fix_errors:
        log(f"Ошибки: скан {scan_errors}, ремонт {fix_errors}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
