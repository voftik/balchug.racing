#!/usr/bin/env python3
"""
Склейщик сессий записи эфира Balchug Racing (systemd-сервис balchug-merger).

Проблема: LiveU при просадках канала переподключается, nginx (record_unique on)
пишет каждый RTMP-паблиш в отдельный FLV — за гоночный день копятся десятки
кусков, и раньше каждый попадал в архив отдельным «видео».

Решение: фоновый цикл раз в POLL_SEC сканирует /var/rec, группирует куски в
«сессии вещания» (зазор между концом предыдущего и началом следующего меньше
SESSION_GAP_MIN минут). Когда сессия «остыла» (последний кусок старше зазора,
все файлы закрыты nginx'ом) — куски склеиваются ffmpeg concat БЕЗ перекодирования
в один MP4 (faststart), грузятся в S3, аннотируются (annotator.py: календарь
сезона + vision-LLM), каталог переиндексируется, запись встаёт в очередь HLS VOD.
Все FLV сессии (включая нулевые обрывки) после этого удаляются.

Отдельные сессии дня (тренировка/квалификация/гонка) остаются отдельными видео.

Окружение (systemd EnvironmentFile=/etc/balchug/secrets.env):
  S3_* (обяз.), SESSION_GAP_MIN (деф. 30), MIN_PART_BYTES (деф. 524288),
  LLM_* (опц., см. annotator.py)
"""
import datetime
import glob
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

REC_DIR = os.environ.get("REC_DIR", "/var/rec")
# Порог отделяет именно разные эфиры. Длинный обрыв нельзя молча склеивать с
# другой программой дня: это хуже, чем два соседних ролика одной гонки.
GAP_SEC = int(float(os.environ.get("SESSION_GAP_MIN", "30")) * 60)
MIN_PART_BYTES = int(os.environ.get("MIN_PART_BYTES", str(512 * 1024)))
MIN_AGE_SEC = 60            # свежие файлы не трогаем (могут ещё дописываться)
POLL_SEC = 30
HLS_QUEUE = "/var/lib/balchug/hls_queue"

TS_RE = re.compile(r"_(\d{4}-\d{2}-\d{2})_(\d{2})-(\d{2})-(\d{2})\.flv$")


def log(*a):
    print(datetime.datetime.now().strftime("%F %T"), *a, flush=True)


def run(cmd, **kw):
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, **kw)


def ffprobe_duration(path):
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", path], timeout=120)
        return float(out.decode().strip())
    except Exception:
        return None


def part_start_time(path):
    """Стартовый таймстемп куска (сек) — у LiveU может быть сильно больше нуля."""
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-show_entries", "format=start_time",
             "-of", "default=noprint_wrappers=1:nokey=1", path], timeout=120)
        return float(out.decode().strip())
    except Exception:
        return 0.0


def open_files_in(dir_):
    """Файлы каталога, открытые каким-либо процессом (через /proc/*/fd)."""
    out = set()
    prefix = dir_.rstrip("/") + "/"
    for fd in glob.glob("/proc/[0-9]*/fd/*"):
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        if target.startswith(prefix):
            out.add(target)
    return out


def scan_parts():
    """Все FLV в /var/rec с распарсенным временем старта."""
    open_set = open_files_in(REC_DIR)
    now = time.time()
    parts = []
    for path in glob.glob(os.path.join(REC_DIR, "*.flv")):
        m = TS_RE.search(os.path.basename(path))
        if not m:
            continue
        try:
            st = os.stat(path)
        except OSError:
            continue
        start_dt = datetime.datetime.strptime(
            f"{m.group(1)} {m.group(2)}:{m.group(3)}:{m.group(4)}", "%Y-%m-%d %H:%M:%S")
        parts.append({
            "path": path,
            "start_ts": start_dt.timestamp(),
            "start_dt": start_dt,
            "end_ts": st.st_mtime,          # mtime == момент закрытия записи
            "size": st.st_size,
            "busy": path in open_set or (now - st.st_mtime) < MIN_AGE_SEC,
        })
    parts.sort(key=lambda p: (p["start_ts"], p["path"]))
    return parts


def group_sessions(parts):
    groups, cur = [], []
    for p in parts:
        if cur and p["start_ts"] - max(x["end_ts"] for x in cur) > GAP_SEC:
            groups.append(cur)
            cur = []
        cur.append(p)
    if cur:
        groups.append(cur)
    return groups


def ready(group):
    now = time.time()
    return (all(not p["busy"] for p in group)
            and now - max(p["end_ts"] for p in group) > GAP_SEC)


def cleanup(paths):
    for p in paths:
        try:
            os.remove(p)
        except OSError:
            pass


def finalize(group):
    real = [p for p in group if p["size"] >= MIN_PART_BYTES]
    if not real:
        log(f"сессия из {len(group)} обрывков < {MIN_PART_BYTES}Б — удаляю без загрузки")
        cleanup([p["path"] for p in group])
        return

    first = real[0]
    date = first["start_dt"].strftime("%Y-%m-%d")
    rec_id = "live_" + first["start_dt"].strftime("%H%M%S")
    tstr = first["start_dt"].strftime("%H:%M:%S")
    log(f"финализация сессии {date} {tstr}: частей {len(real)} (+{len(group) - len(real)} обрывков)")

    tmp = tempfile.mkdtemp(prefix="balchug_merge_")
    try:
        # ВАЖНО: таймстемпы куска могут начинаться НЕ с нуля (клок энкодера LiveU).
        # Без нормализации concat создаёт дыры в таймлайне (плеер замирает на
        # границе куска). Сдвиг к нулю — явный через -output_ts_offset на величину
        # фактического старта (make_zero положительные старты не сдвигает).
        norm = []
        for i, p in enumerate(real):
            np_ = os.path.join(tmp, f"norm{i:03d}.mp4")
            start = part_start_time(p["path"])
            cmd = ["ffmpeg", "-y", "-i", p["path"], "-c", "copy"]
            if start and start > 0.01:
                cmd += ["-output_ts_offset", f"-{start:.3f}"]
            cmd += ["-avoid_negative_ts", "make_zero", np_]
            run(cmd)
            norm.append(np_)

        mp4 = os.path.join(tmp, "source.mp4")
        if len(norm) == 1:
            run(["ffmpeg", "-y", "-i", norm[0],
                 "-c", "copy", "-movflags", "+faststart", mp4])
        else:
            lst = os.path.join(tmp, "list.txt")
            with open(lst, "w") as f:
                for np_ in norm:
                    f.write(f"file '{np_}'\n")
            run(["ffmpeg", "-y", "-f", "concat", "-safe", "0",
                 "-i", lst, "-c", "copy", "-movflags", "+faststart", mp4])
        for np_ in norm:
            try:
                os.remove(np_)
            except OSError:
                pass

        dur = ffprobe_duration(mp4)
        jpg = os.path.join(tmp, "thumb.jpg")
        try:
            run(["ffmpeg", "-y", "-ss", str(max(1, int((dur or 10) / 2))), "-i", mp4,
                 "-vframes", "1", "-vf", "scale=640:-1", jpg])
        except Exception:
            jpg = None

        s3 = C.s3()
        video_key = f"stream_records/{date}/{rec_id}/source.mp4"
        thumb_key = f"thumbnails/{date}/{rec_id}/source.mp4.jpg"
        log(f"  → S3 {video_key} ({(dur or 0):.0f}с, {os.path.getsize(mp4) / 1e6:.0f} МБ)")
        s3.upload_file(mp4, C.S3_BUCKET, video_key, ExtraArgs={"ContentType": "video/mp4"})
        if jpg and os.path.exists(jpg):
            s3.upload_file(jpg, C.S3_BUCKET, thumb_key, ExtraArgs={"ContentType": "image/jpeg"})

        parts_meta = [{
            "start": p["start_dt"].strftime("%H:%M:%S"),
            "duration_seconds": round(max(0.0, p["end_ts"] - p["start_ts"]), 1),
        } for p in real]
        ann = annotator.annotate(date=date, start_hms=tstr, duration_sec=dur,
                                 video_key=video_key, video_path=mp4, parts=parts_meta)
        annotator.upload(s3, C.S3_BUCKET, ann, date, rec_id)
        log(f"  аннотация: {ann['ui_metadata']['display_title']}")

        try:
            import indexer
            indexer.main()
        except Exception as e:
            log("  reindex warn:", e)

        try:
            os.makedirs(HLS_QUEUE, exist_ok=True)
            with open(os.path.join(HLS_QUEUE, f"{C.make_id(video_key)}.job"), "w") as f:
                f.write(video_key)
        except Exception as e:
            log("  hls queue warn:", e)

        cleanup([p["path"] for p in group])
        log(f"  готово: {rec_id}")
    except Exception:
        # Исходные FLV остаются на месте для повторной попытки. Перемещение их
        # в failed/ после временной ошибки S3/ffmpeg лишало архив единственной
        # исходной записи и делало инцидент невосстановимым без ручной работы.
        raise
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    log(f"session_merger запущен: gap={GAP_SEC}с, min_part={MIN_PART_BYTES}Б, dir={REC_DIR}")
    while True:
        try:
            parts = scan_parts()
            for group in group_sessions(parts):
                if ready(group):
                    finalize(group)
        except Exception as e:
            log("ERR:", repr(e))
        time.sleep(POLL_SEC)


if __name__ == "__main__":
    main()
