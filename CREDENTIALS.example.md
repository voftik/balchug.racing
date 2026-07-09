# 🏁 Balchug Racing — данные для подключения LiveU Solo (ШАБЛОН)

> Скопируйте этот файл в `CREDENTIALS.md` (он в `.gitignore`) и заполните реальными
> значениями. Значения вставляются в **Solo Portal** (solo.liveu.tv) при добавлении
> назначения (destination). Тип назначения — **Generic RTMP** (без аутентификации).

## Настройки в Solo Portal

| Поле в LiveU Solo            | Значение                                   |
|------------------------------|--------------------------------------------|
| **Destination Name**         | `Balchug Racing`                           |
| **Profile**                  | `1920x1080 60fps`                          |
| **Primary Ingress URL**      | `rtmp://<ВАШ_ДОМЕН>/live`                  |
| **Secondary Ingress URL**    | *(оставить пустым)*                        |
| **Stream Name (Stream Key)** | `<STREAM_KEY>`                             |
| **Username**                 | *(оставить пустым)*                        |
| **Password**                 | *(оставить пустым)*                        |
| **Bit rate (Kbps)**          | `6000` (для 1080p60; до `8000` при хорошем канале) |

Полный RTMP-адрес публикации:
```
rtmp://<ВАШ_ДОМЕН>/live/<STREAM_KEY>
```

Stream key также хранится в файле `.stream_key` в корне проекта (в `.gitignore`) —
его читает `server/deploy.sh` и подставляет в веб-плеер при деплое.

## 📺 Просмотр трансляции

| Назначение                | Ссылка                                             |
|---------------------------|----------------------------------------------------|
| **Веб-плеер (смотреть)**  | https://<ВАШ_ДОМЕН>/                               |
| Прямой HLS (для VLC/др.)  | https://<ВАШ_ДОМЕН>/hls/<STREAM_KEY>.m3u8          |
| Прямой HTTP-FLV (опц.)    | https://<ВАШ_ДОМЕН>/live?app=live&stream=<STREAM_KEY> |
| Статистика RTMP / монитор | https://<ВАШ_ДОМЕН>/stat                           |

## 🔧 Сервер

| Параметр            | Значение                                    |
|---------------------|---------------------------------------------|
| Хост                | `<SSH_ALIAS>` → `<IP>` (Ubuntu 24.04)       |
| RTMP-приём          | порт `1935`, приложение `live`              |
| Веб                 | порты `80` → редирект на `443` (HTTPS)      |
| TLS                 | Let's Encrypt (авто-продление)              |
| Конфиг              | `/etc/nginx/nginx.conf`                     |
| Веб-корень          | `/var/www/balchug/`                         |
| Зеркало проекта     | `/opt/balchug_racing/`                      |
| Секреты сервисов    | `/etc/balchug/secrets.env` (см. `secrets.env.example`) |
