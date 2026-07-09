#!/usr/bin/env python3
"""
Аннотатор записей трансляций Balchug Racing (схема 2.1.0).

Два слоя:
1. Детерминированный (работает всегда): по календарю сезона
   (server/api/race_calendar_2026.json) определяет серию/этап/трассу, а по дню
   уик-энда и длительности — тип сессии. Собирает заголовок вида
   «REC 2026 · Этап 3 · Игора Драйв · Гонка · 11.07.2026».
2. Vision-LLM (опционально): если в окружении задан LLM_API_KEY, извлекает
   несколько кадров из видео и просит OpenAI-совместимый vision-API (например
   OpenRouter) уточнить тип сессии, заголовок и написать короткое описание.
   Любая ошибка LLM не блокирует пайплайн — остаётся слой 1.

Окружение: LLM_API_KEY, LLM_MODEL (деф. google/gemini-3.5-flash),
           LLM_BASE_URL (деф. https://openrouter.ai/api/v1).

Использование:
    import annotator
    ann = annotator.annotate(date="2026-07-11", start_hms="14:02:11",
                             duration_sec=14523, video_path="/tmp/x/source.mp4",
                             video_key="stream_records/.../source.mp4",
                             parts=[{"start": "14:02:11", "duration_seconds": 730}, ...])
"""
import base64
import datetime
import json
import os
import re
import subprocess
import sys
import tempfile
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
CALENDAR_PATH = os.path.join(HERE, "..", "api", "race_calendar_2026.json")

RACE_MIN_SEC = 100 * 60          # трансляция ≥ 100 минут считается гонкой
SESSION_TYPES = ["Гонка", "Квалификация", "Тренировка", "Трансляция"]

_calendar = None


def calendar():
    global _calendar
    if _calendar is None:
        with open(CALENDAR_PATH, encoding="utf-8") as f:
            _calendar = json.load(f)
    return _calendar


def find_event(date_str):
    """Событие календаря, в чей интервал (включая тренировочные дни) попадает дата."""
    for ev in calendar()["events"]:
        if ev["start_date"] <= date_str <= ev["end_date"]:
            return ev
    return None


def guess_session_type(ev, date_str, start_hms, duration_sec):
    if duration_sec and duration_sec >= RACE_MIN_SEC:
        return "Гонка"
    if not ev:
        return "Трансляция"
    if date_str < ev["race_date"]:
        return "Тренировка"
    try:
        hour = int(start_hms.split(":")[0])
    except Exception:
        hour = 12
    if date_str == ev["race_date"] and hour < 12:
        return "Квалификация"
    return "Трансляция"


def _ru_date(date_str):
    y, m, d = date_str.split("-")
    return f"{d}.{m}.{y}"


def build_title(ev, date_str, start_hms, stype):
    if not ev:
        return f"Прямой эфир · {date_str} {start_hms}"
    bits = [f"{ev['series']} {ev['season']}", ev["stage"], ev["track_display"], stype, _ru_date(date_str)]
    return " · ".join(b for b in bits if b)


# --------------------------- LLM-слой ---------------------------

def _extract_frames(video_path, duration_sec, n=4, width=640):
    """Достаёт n кадров, равномерно распределённых по видео → список base64-JPEG."""
    frames = []
    if not video_path or not duration_sec or duration_sec < 2:
        return frames
    tmp = tempfile.mkdtemp(prefix="balchug_frames_")
    for i in range(n):
        ts = duration_sec * (i + 0.5) / n
        out = os.path.join(tmp, f"f{i}.jpg")
        try:
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-ss", f"{ts:.1f}", "-i", video_path,
                 "-vframes", "1", "-vf", f"scale={width}:-2", "-q:v", "7", out],
                check=True, timeout=120)
            with open(out, "rb") as f:
                frames.append(base64.b64encode(f.read()).decode())
        except Exception:
            continue
        finally:
            try:
                os.remove(out)
            except OSError:
                pass
    try:
        os.rmdir(tmp)
    except OSError:
        pass
    return frames


def _llm_config():
    key = os.environ.get("LLM_API_KEY", "").strip()
    if not key:
        return None
    return {
        "key": key,
        "model": os.environ.get("LLM_MODEL", "google/gemini-3.5-flash").strip(),
        "base": os.environ.get("LLM_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/"),
    }


def _llm_prompt(ev, date_str, start_hms, duration_sec, stype_guess, parts):
    team = calendar()["team"]
    dur_min = round((duration_sec or 0) / 60)
    ctx = [
        "Ты — ассистент гоночной команды, аннотирующий видеозаписи прямых трансляций с гоночных уик-эндов.",
        f"Команда: {team['name']} №{team['number']}, класс {team['class']} ({team['car']}), пилоты: {', '.join(team['pilots'])}.",
        f"Дата записи: {date_str}, старт трансляции {start_hms} (UTC), длительность ≈ {dur_min} мин.",
    ]
    if ev:
        ctx.append(
            f"По календарю в этот день идёт событие: {ev['series']} {ev['season']}, {ev['stage']}"
            f"{(' «' + ev['name'] + '»') if ev.get('name') else ''}, трасса {ev['track_display']}, "
            f"формат: {ev.get('format', '')}, день гонки: {ev['race_date']}.")
    else:
        ctx.append("В календаре сезона на эту дату события нет (вероятно, тесты или частная трансляция).")
    if parts and len(parts) > 1:
        ctx.append(f"Запись склеена из {len(parts)} фрагментов эфира (обрывы связи LiveU) — это одна сессия.")
    ctx.append(f"Предварительная оценка типа сессии: {stype_guess}.")
    ctx.append(
        "На основе кадров из видео и контекста верни СТРОГО JSON без пояснений, формат:\n"
        '{"session_type": "<один из: ' + ", ".join(SESSION_TYPES) + '>", '
        '"display_title": "<краткий заголовок на русском, ≤ 90 символов, формат: серия · этап · трасса · тип · дата>", '
        '"summary": "<1–3 предложения на русском: что видно в кадре (трасса/пит-лейн/паддок), погода/время суток, чем примечательна запись. Без выдумок о результатах.>"}')
    return "\n".join(ctx)


def _llm_refine(ev, date_str, start_hms, duration_sec, stype_guess, video_path, parts):
    cfg = _llm_config()
    if not cfg:
        return None
    frames = _extract_frames(video_path, duration_sec)
    if not frames:
        return None
    content = [{"type": "text", "text": _llm_prompt(ev, date_str, start_hms, duration_sec, stype_guess, parts)}]
    for b in frames:
        content.append({"type": "image_url", "image_url": {"url": "data:image/jpeg;base64," + b}})
    body = {
        "model": cfg["model"],
        "messages": [{"role": "user", "content": content}],
        "temperature": 0.2,
        "max_tokens": 700,
    }
    req = urllib.request.Request(
        cfg["base"] + "/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": "Bearer " + cfg["key"],
            "Content-Type": "application/json",
            "HTTP-Referer": "https://balchug.racing",
            "X-Title": "Balchug Racing Annotator",
        })
    with urllib.request.urlopen(req, timeout=180) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    txt = data["choices"][0]["message"]["content"] or ""
    m = re.search(r"\{.*\}", txt, re.S)
    if not m:
        return None
    out = json.loads(m.group(0))
    if not isinstance(out, dict):
        return None
    return out


# --------------------------- сборка аннотации ---------------------------

def annotate(date, start_hms, duration_sec, video_key, video_path=None, parts=None,
             source="LiveU Live", use_llm=True):
    """Возвращает dict аннотации (схема 2.1.0). Ошибки LLM подавляются."""
    ev = find_event(date)
    team = calendar()["team"]
    stype = guess_session_type(ev, date, start_hms, duration_sec)
    title = build_title(ev, date, start_hms, stype)

    ann = {
        "annotation_version": "2.1.0",
        "created_by": "BALCHUG Racing Stream Annotator",
        "created_at": datetime.datetime.utcnow().isoformat(),
        "session_info": {
            "pilot_full_name": team["name"],  # трансляция командная, пилот не определяется
            "pilot_code": "",
            "track_full_name": ev["track"] if ev else "",
            "car_full_name": team["car"] if ev else "",
            "session_type": stype,
            "session_date": date,
        },
        "technical_metadata": {
            "session_duration_seconds": duration_sec,
            "source": source,
            "start_time": start_hms,
        },
        "ui_metadata": {"display_title": title},
        "file_metadata": {"s3_key": video_key},
    }
    if ev:
        ann["event_info"] = {
            "series": ev["series"], "season": ev["season"], "stage": ev["stage"],
            "event_name": ev.get("name", ""), "format": ev.get("format", ""),
            "race_date": ev["race_date"],
        }
    if parts:
        ann["technical_metadata"]["parts"] = parts
        ann["technical_metadata"]["parts_count"] = len(parts)

    if use_llm:
        try:
            llm = _llm_refine(ev, date, start_hms, duration_sec, stype, video_path, parts)
        except Exception as e:
            print(f"annotator: LLM недоступен ({e}), остаёмся на календарной аннотации",
                  file=sys.stderr, flush=True)
            llm = None
        if llm:
            st = str(llm.get("session_type", "")).strip()
            if st in SESSION_TYPES and st != stype:
                ann["session_info"]["session_type"] = st
                ann["ui_metadata"]["display_title"] = build_title(ev, date, start_hms, st)
            dt = str(llm.get("display_title", "")).strip()
            if dt and len(dt) <= 120:
                ann["ui_metadata"]["display_title"] = dt
            summary = str(llm.get("summary", "")).strip()
            if summary:
                ann["ai_annotation"] = {"session_summary": summary}
            ann["created_by"] = "BALCHUG Racing Stream Annotator (LLM-assisted)"
    return ann


def annotation_key(date, rec_id):
    return f"annotations/{date}/{rec_id}/{rec_id}_annotation.json"


def upload(s3_client, bucket, ann, date, rec_id):
    key = annotation_key(date, rec_id)
    s3_client.put_object(Bucket=bucket, Key=key,
                         Body=json.dumps(ann, ensure_ascii=False).encode("utf-8"),
                         ContentType="application/json")
    return key
