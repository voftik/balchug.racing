#!/bin/bash
# =============================================================================
#  Balchug Racing — orphan ffmpeg reaper
#  Снимает «осиротевшие» exec-ffmpeg приложения live, оставшиеся после обрыва
#  публикации LiveU (когда nginx не убил процесс сигналом). Запускается из cron
#  раз в минуту. Действует ТОЛЬКО когда stat достоверно получен и показывает,
#  что для потока нет активного паблишера, а ffmpeg висит дольше grace-периода.
#
#  Безопасность: при любой неуверенности (stat недоступен/не распарсился) —
#  выходим, ничего не трогая. Так живой эфир не пострадает.
# =============================================================================
set -o pipefail

STAT_URL="https://balchug.racing/stat"
GRACE=45                                   # сек: моложе — не трогаем (старт публикации)
LOG=/var/log/balchug-orphan-reaper.log

# 1) Достаём stat. Любая ошибка сети/таймаут → выходим (НЕ риповать вслепую).
STAT=$(curl -fsS --max-time 5 "$STAT_URL" 2>/dev/null) || exit 0
# Это точно stat-страница, а не ошибка? Корневой тег модуля nginx-http-flv —
# <http-flv> (у классического nginx-rtmp было бы <rtmp>); принимаем оба.
case "$STAT" in *"<http-flv>"*|*"<rtmp>"*) : ;; *) exit 0 ;; esac

# 2) Имена потоков live с АКТИВНЫМ паблишером (publishing). Если XML битый —
#    python вернёт ненулевой код → выходим, ничего не снимая.
PUBS=$(printf '%s' "$STAT" | python3 -c '
import sys, xml.etree.ElementTree as ET
r = ET.fromstring(sys.stdin.read())
for app in r.iter("application"):
    if app.findtext("name") == "live":
        for s in app.iter("stream"):
            if s.find("publishing") is not None:
                print(s.findtext("name"))
') || exit 0

# 3) Перебираем exec-ffmpeg приложения live; снимаем тех, чьего паблишера нет.
for pid in $(pgrep -f "rtmp://127.0.0.1:1935/live/" 2>/dev/null); do
    cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null) || continue
    case "$cmd" in *ffmpeg*) : ;; *) continue ;; esac     # только ffmpeg
    name=$(printf '%s' "$cmd" | grep -oE "rtmp://127\.0\.0\.1:1935/live/[A-Za-z0-9_]+" | head -1 | sed 's#.*/##')
    [ -n "$name" ] || continue
    age=$(ps -o etimes= -p "$pid" 2>/dev/null | tr -d ' ')
    [ -n "$age" ] || continue
    if ! printf '%s\n' "$PUBS" | grep -qxF "$name" && [ "$age" -gt "$GRACE" ]; then
        echo "$(date '+%F %T') reap orphan pid=$pid name=$name age=${age}s" >> "$LOG"
        kill "$pid" 2>/dev/null || true
        sleep 2
        kill -0 "$pid" 2>/dev/null && kill -9 "$pid" 2>/dev/null || true
    fi
done
