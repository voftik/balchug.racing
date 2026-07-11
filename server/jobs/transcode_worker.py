#!/usr/bin/env python3
"""
Фоновый воркер HLS VOD. Берёт задачи из /var/lib/balchug/hls_queue/*.job
(содержимое — S3-ключ исходного MP4), генерирует адаптивный HLS (1080/720/480),
грузит в s3:hls_vod/<item_id>/ (public-read) и проставляет hls_key в каталоге.
Источник читается напрямую из S3 по presigned-URL (без полной загрузки).
Один файл за раз (ленивая обработка, низкий приоритет). systemd-сервис.
"""
import os, sys, time, glob, tempfile, shutil, subprocess

sys.path.insert(0, "/opt/balchug_racing/server/api")
import common as C  # noqa: E402

QUEUE = "/var/lib/balchug/hls_queue"
CT = {".m3u8": "application/vnd.apple.mpegurl", ".ts": "video/mp2t"}

# Только 720p + 480p (адаптивный стрим). 1080p НЕ кодируем — это дорого по CPU
# (особенно из 60fps), а макс. качество остаётся в исходном MP4 (скачивание).
# Это держит нагрузку низкой и не мешает живому эфиру.
VF = ("[0:v]split=2[v1][v2];"
      "[v1]scale=w=1280:h=720:force_original_aspect_ratio=decrease:force_divisible_by=2[v1o];"
      "[v2]scale=w=854:h=480:force_original_aspect_ratio=decrease:force_divisible_by=2[v2o]")


class LiveBroadcastStarted(RuntimeError):
    """A VOD encode was preempted so the live broadcast keeps priority."""


def run_ffmpeg(cmd):
    """Run one VOD encode, yielding its CPU as soon as a live stream appears."""
    process = subprocess.Popen(cmd)
    while process.poll() is None:
        if live_active():
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise LiveBroadcastStarted("live broadcast started; VOD encode deferred")
        time.sleep(5)
    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, cmd)


def transcode(video_key):
    item_id = C.make_id(video_key)
    s3 = C.s3()
    # уже готово?
    conn = C.db()
    row = conn.execute("SELECT hls_key FROM items WHERE id=?", (item_id,)).fetchone()
    conn.close()
    if row and row["hls_key"]:
        return "skip(exists)"

    src = s3.generate_presigned_url("get_object",
        Params={"Bucket": C.S3_BUCKET, "Key": video_key}, ExpiresIn=6 * 3600)
    out = tempfile.mkdtemp(prefix="balchug_hls_")
    try:
        for v in ("v0", "v1"):
            os.makedirs(os.path.join(out, v), exist_ok=True)
        cmd = [
            "nice", "-n", "19", "ffmpeg", "-y", "-loglevel", "error",
            # LiveU reconnects can leave a damaged final packet in a copied FLV.
            # VOD must keep the valid media, rather than fail the entire archive job.
            "-fflags", "+genpts+discardcorrupt", "-err_detect", "ignore_err",
            "-threads", "2", "-i", src,
            "-filter_complex", VF,
            "-map", "[v1o]", "-map", "[v2o]",
            "-map", "0:a?", "-map", "0:a?",
            "-c:v", "libx264", "-preset", "veryfast", "-g", "48", "-keyint_min", "48", "-sc_threshold", "0",
            "-b:v:0", "3000k", "-maxrate:v:0", "3200k", "-bufsize:v:0", "6000k",
            "-b:v:1", "1200k", "-maxrate:v:1", "1400k", "-bufsize:v:1", "2800k",
            "-c:a", "aac", "-b:a", "128k", "-ac", "2",
            "-f", "hls", "-hls_time", "4", "-hls_playlist_type", "vod",
            "-hls_flags", "independent_segments",
            "-hls_segment_filename", os.path.join(out, "v%v", "seg_%03d.ts"),
            "-master_pl_name", "master.m3u8",
            "-var_stream_map", "v:0,a:0 v:1,a:1",
            os.path.join(out, "v%v", "index.m3u8"),
        ]
        run_ffmpeg(cmd)

        prefix = f"hls_vod/{item_id}"
        for root, _, files in os.walk(out):
            for fn in files:
                full = os.path.join(root, fn)
                rel = os.path.relpath(full, out)
                ct = CT.get(os.path.splitext(fn)[1], "application/octet-stream")
                # Приватная загрузка (как и весь бакет); отдаём через /api/hls (same-origin).
                s3.upload_file(full, C.S3_BUCKET, f"{prefix}/{rel}",
                               ExtraArgs={"ContentType": ct})

        hls_key = f"{prefix}/master.m3u8"
        conn = C.db()
        try:
            conn.execute("UPDATE items SET hls_key=? WHERE id=?", (hls_key, item_id))
            conn.commit()
        finally:
            conn.close()
        return f"ok → {hls_key}"
    finally:
        shutil.rmtree(out, ignore_errors=True)


def live_active():
    """Идёт ли сейчас живой эфир (работает exec-ffmpeg приложения live)."""
    try:
        return subprocess.run(["pgrep", "-f", "rtmp://127.0.0.1:1935/live/"],
                              stdout=subprocess.DEVNULL).returncode == 0
    except Exception:
        return False


def main():
    os.makedirs(QUEUE, exist_ok=True)
    print("HLS worker запущен", flush=True)
    while True:
        # ЭФИР В ПРИОРИТЕТЕ: пока идёт трансляция — не транскодим (ждём).
        if live_active():
            time.sleep(20); continue
        jobs = sorted(glob.glob(os.path.join(QUEUE, "*.job")))
        if not jobs:
            time.sleep(5); continue
        job = jobs[0]                      # по одному файлу за итерацию (перепроверяем эфир)
        try:
            with open(job) as f:
                key = f.read().strip()
            print("transcode:", key, flush=True)
            print("  ", transcode(key), flush=True)
        except Exception as e:
            print("  ERR:", e, file=sys.stderr, flush=True)
            # Keep the durable queue item. A transient S3/ffmpeg failure must
            # not silently remove the only path to archive HLS playback.
            time.sleep(60)
        else:
            try: os.remove(job)
            except Exception: pass
        time.sleep(2)


if __name__ == "__main__":
    main()
