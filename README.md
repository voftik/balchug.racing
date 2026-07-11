# 🏁 balchug.racing — live-трансляции и видеоархив команды BALCHUG Racing

Самодельная (self-hosted) платформа гоночной команды **BALCHUG Racing** (№21, класс CN Pro,
[Russian Endurance Challenge](https://rusendurance.com/)) для прямых трансляций с болида
и автоматического видеоархива: [https://balchug.racing](https://balchug.racing)

- **Прямой эфир**: приём RTMP-потока с онборд-энкодера LiveU Solo, ABR-транскодинг
  (1080p60/720p/480p), веб-плеер с регулируемым буфером и телеметрией.
- **Автоархив**: каждый эфир записывается, куски одной трансляции автоматически
  склеиваются в целые сессии, загружаются в S3, аннотируются (календарь сезона + LLM)
  и появляются в каталоге [/archive/](https://balchug.racing/archive/) с поиском и фильтрами.
- **Страницы команды**: телеметрия трассы, гонки СМП РСКГ, радар осадков.

## Архитектура

```
LiveU Solo 1080p60 ──RTMP──► nginx (app "live", ingest)
 rtmp://<домен>/live/<key>       │
                                 ├─ exec ffmpeg ──► app "show": _src 1080p60 (copy)
                                 │                              _720  720p30 ~2.8M
                                 │                              _480  480p30 ~1.2M
                                 │        hls_variant → master /hls/<key>.m3u8
                                 │                        ▼
                                 │   Браузер ◄── hls.js (ABR, буфер 5–50 с, телеметрия)
                                 │
                                 └─ record all ──► FLV-куски в /var/rec
                                          (LiveU при обрывах переподключается —
                                           каждый reconnect = новый файл)
                                                    ▼
                          session_merger (systemd): группирует куски по зазору
                          < SESSION_GAP_MIN, склеивает ffmpeg concat (без перекодирования)
                                                    ▼
                          MP4 (faststart) + превью ──► S3 stream_records/<дата>/<id>/
                                                    ▼
                          annotator: календарь сезона (REC + СМП РСКГ Эндуранс)
                          + кадры видео → LLM (опц.) → annotations/<дата>/<id>/*.json
                                                    ▼
                          indexer → SQLite-каталог → FastAPI /api/ → /archive/
                                                    ▼
                          transcode_worker (фон, nice 19): HLS VOD 720/480 → S3 hls_vod/
```

Ключевые решения:

- **nginx из исходников + [nginx-http-flv-module](https://github.com/winshining/nginx-http-flv-module)**:
  RTMP-приём, HLS, HTTP-FLV, GOP-кэш, `/stat`. Ровно `worker_processes 1` (поток живёт
  в памяти воркера), без HTTP/2 на 443 (ограничение HTTP-FLV).
- **ABR-транскодинг** на входе: три версии в `application show`, master-плейлист собирает
  `hls_variant`; плеер (hls.js) адаптируется сам + ручной выбор качества.
  Аудио переэнкодится в AAC 44.1 кГц (HLS-муксер nginx-rtmp неверно таймстемпит 48 кГц при copy).
- **Запись — копия источника** (`record all`, видео без перекодирования = макс. качество).
  Из-за реконнектов LiveU куски склеиваются пост-фактум (`session_merger.py`), а не в nginx.
- **Аннотации** (схема 2.1.0, JSON в S3): двухслойный аннотатор — детерминированный слой
  по календарю сезона (`server/api/race_calendar_2026.json`) всегда даёт трассу/этап/тип
  сессии; при наличии `LLM_API_KEY` кадры видео уходят в vision-LLM за уточнением и
  человеческим описанием. Ошибки LLM не блокируют пайплайн.
- **Каталог** — SQLite (`/var/lib/balchug/catalog.db`), пересобирается индексером из S3;
  ручные правки (`edited=1`) не перетираются.

## Структура проекта

```
balchug_racing/
├── README.md
├── CREDENTIALS.example.md    # шаблон настроек LiveU (реальный CREDENTIALS.md — в .gitignore)
├── secrets.env.example       # шаблон /etc/balchug/secrets.env
├── requirements.txt
├── server/
│   ├── install.sh            # полная установка: сборка nginx + конфиги + сертификат
│   ├── deploy.sh             # деплой с локальной машины (rsync + рендер stream key)
│   ├── nginx.conf            # боевой конфиг → /etc/nginx/nginx.conf
│   ├── nginx.service         # systemd unit nginx
│   ├── certbot-deploy-hook.sh
│   ├── wireproxy.conf.example# шаблон WG-туннеля для LLM-запросов (см. «Безопасность»)
│   ├── api/
│   │   ├── app.py            # FastAPI: каталог, фильтры, presigned-ссылки, HLS-прокси, админ
│   │   ├── common.py         # S3-клиент, парсер имён, схема SQLite
│   │   ├── indexer.py        # S3 → SQLite (связывает видео/превью/телеметрию/аннотации)
│   │   ├── dictionaries.json # справочники: пилоты, трассы, машины
│   │   └── race_calendar_2026.json  # календарь сезона для аннотатора
│   ├── jobs/
│   │   ├── session_merger.py # склейка кусков эфира → MP4 → S3 → аннотация → каталог
│   │   ├── annotator.py      # аннотатор: календарь + vision-LLM (опционально)
│   │   ├── transcode_worker.py # фоновый HLS VOD транскодер
│   │   ├── repair_archive.py # одноразовый ретро-ремонт фрагментированного архива
│   │   ├── orphan_reaper.sh  # cron: снимает осиротевшие exec-ffmpeg после обрывов
│   │   └── reindex.sh        # cron: периодическая переиндексация каталога
│   └── systemd/
│       ├── balchug-api.service
│       ├── balchug-transcode.service
│       ├── balchug-merger.service
│       └── wireproxy.service     # туннель LLM-трафика (опционально)
└── web/
    ├── index.html            # плеер прямого эфира (ABR, буфер, телеметрия)
    ├── archive/index.html    # каталог записей
    ├── telemetry/ weather/              # страницы-разделы
    └── assets/               # site.css, archive.js, hls.js, шрифты, логотип
```

## Пайплайн записи и аннотирования

1. **Запись**: nginx пишет каждый RTMP-паблиш в `/var/rec/<key>-<uid>_<дата>_<время>.flv`
   (`record_unique on`). LiveU при просадках канала переподключается — за гоночный день
   накапливаются десятки кусков.
2. **Склейка** (`session_merger.py`, systemd-сервис): куски группируются в «сессии
   вещания» — зазор между концом предыдущего и началом следующего меньше
   `SESSION_GAP_MIN` (по умолчанию 30 мин). Когда сессия «остыла» (последний кусок
   старше зазора) и активной публикации нет — куски склеиваются `ffmpeg concat`
   (без перекодирования), получается один MP4 с faststart, превью — кадр из середины.
   Длинный обрыв образует отдельную запись: это исключает смешение разных программ дня.
3. **Загрузка в S3**: `stream_records/<дата>/live_<HHMMSS>/source.mp4` +
   `thumbnails/...jpg`. FLV-куски (включая нулевые) удаляются.
4. **Аннотация** (`annotator.py`):
   - слой 1 — по `race_calendar_2026.json` определяются серия/этап/трасса, тип сессии
     (по дню уик-энда и длительности), собирается заголовок вида
     «REC 2026 · Этап 3 · Игора Драйв · Гонка · 11.07.2026»; из `team_entry` события
     в описание и атрибуты попадают состав экипажа и результат этапа
     (записи ищутся в архиве по фамилиям пилотов и названию Гран-при);
   - слой 2 (опционально) — 4 кадра видео + контекст календаря уходят в
     OpenAI-совместимый vision-API (`LLM_API_KEY`/`LLM_MODEL`/`LLM_BASE_URL`,
     напр. OpenRouter + google/gemini-3.5-flash) → уточнённый тип сессии, заголовок и
     короткое описание (`ai_annotation.session_summary`).
   - результат — JSON схемы 2.1.0 в `annotations/<дата>/<id>/<id>_annotation.json`
     с точной привязкой к видео через `file_metadata.s3_key`.
5. **Индексация** (`indexer.py`): каталог пересобирается из S3; аннотация матчится
   к видео по `s3_key` (для онборд-видео других систем — fallback по дате+коду пилота).
6. **HLS VOD** (`transcode_worker.py`): фоновый транскод 720/480 для стриминга из
   архива (эфир всегда в приоритете — во время live транскод ждёт).

## API каталога (`/api/`)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/api/catalog?q=&pilot=&track=&season=&stype=&source=&sort=&limit=&offset=` | список записей |
| GET | `/api/filters` | значения фильтров |
| GET | `/api/item/{id}` | карточка: presigned MP4, HLS, аннотация, связанные файлы |
| GET | `/api/hls/{id}/{path}` | same-origin прокси HLS VOD из приватного бакета |
| POST | `/api/enqueue_hls/{id}` | поставить запись в очередь HLS-транскода |
| POST | `/api/admin/item/{id}` | правка метаданных (Bearer `ADMIN_TOKEN`) |
| DELETE | `/api/admin/item/{id}` | удаление записи и её объектов (Bearer `ADMIN_TOKEN`) |
| GET | `/api/viewers?id=&watching=1` | счётчик зрителей эфира (heartbeat) |
| GET | `/api/health` | здоровье сервиса |

## Установка и деплой

Требования: Ubuntu 22.04+, домен с A-записью, S3-совместимое хранилище.

```bash
# 1. Секреты
cp secrets.env.example secrets.env         # заполнить и скопировать на сервер:
scp secrets.env <host>:/etc/balchug/secrets.env   # chmod 600
echo "<ваш-stream-key>" > .stream_key      # ключ публикации RTMP (в .gitignore)

# 2. Первая установка (на сервере соберёт nginx, выпустит сертификат)
bash server/deploy.sh --full

# 3. Обычное обновление (web/конфиг/сервисы, без пересборки nginx)
bash server/deploy.sh
```

`deploy.sh` синхронизирует проект в `/opt/balchug_racing/`, кладёт stream key в
`/etc/balchug/stream.key` и подставляет его в плеер вместо `__STREAM_KEY__`.

Cron на сервере:
```
* * * * *  /opt/balchug_racing/server/jobs/orphan_reaper.sh >/dev/null 2>&1
17 * * * * /opt/balchug_racing/server/jobs/reindex.sh >>/var/log/balchug-reindex.log 2>&1
```

## Эксплуатация

| Действие | Команда |
|----------|---------|
| Статус сервисов | `systemctl status nginx balchug-api balchug-transcode balchug-merger` |
| Активные потоки | `curl -s https://<домен>/stat` |
| Логи записи/склейки | `journalctl -u balchug-merger -f` |
| Логи API | `journalctl -u balchug-api -f` |
| Переиндексация вручную | `bash server/jobs/reindex.sh` |
| Тестовый эфир без LiveU | см. ниже |

```bash
ffmpeg -re -f lavfi -i testsrc2=size=1920x1080:rate=60 -f lavfi -i sine=frequency=1000 \
  -c:v libx264 -preset veryfast -tune zerolatency -b:v 6000k -c:a aac -ar 44100 \
  -f flv rtmp://<домен>/live/<STREAM_KEY>
```

## Безопасность

- Публикация RTMP защищена секретным stream key; ключ не хранится в репозитории
  (`.stream_key` локально, `/etc/balchug/stream.key` на сервере, рендер при деплое).
- Бакет S3 приватный: видео и превью отдаются presigned-ссылками, HLS VOD — через
  same-origin прокси `/api/hls/`.
- Секреты сервисов — только в `/etc/balchug/secrets.env` (600, root).
- Если LLM-провайдер блокирует IP сервера (например, OpenRouter из РФ), LLM-трафик
  аннотатора уводится в WireGuard-туннель через [wireproxy](https://github.com/pufferffish/wireproxy)
  (userspace, без изменения маршрутизации сервера — RTMP/SSH/сайт ходят напрямую):
  локальный HTTP-прокси `127.0.0.1:25345` → `LLM_PROXY` в secrets.env.
  Шаблоны: `server/wireproxy.conf.example` + `server/systemd/wireproxy.service`.
  После установки бинаря и защищённого конфига проверьте
  `wireproxy -n -c /etc/wireproxy/wireproxy.conf`; при локальном `LLM_PROXY`
  обычный deploy валидирует и перезапускает сервис до запуска склейщика.
- ⚠️ Осознанная особенность: `GET /api/boris` возвращает `ADMIN_TOKEN` любому — это
  шуточная «кнопка для Бориса» для редактирования публичного архива командой.
  Права токена ограничены правкой/удалением карточек каталога. Если вам нужен
  настоящий контроль доступа — уберите эндпоинт `/boris` и раздавайте токен вручную.

## Сезон 2026 (контекст аннотатора)

Календарь в `server/api/race_calendar_2026.json`: Russian Endurance Challenge
(этапы 30.05 MRW, 27.06 MRW, 10–11.07 Игора Драйв, 01.08 MRW, 03.10 «500 вёрст» MRW)
и СМП РСКГ Эндуранс (Спортпрототип CN). Обновляйте файл под новый сезон.

## Лицензия

MIT (веб-шрифт Russo One — OFL; hls.js/flv.js — Apache-2.0, локальные копии в `web/assets/`).
