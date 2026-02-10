# Публикация приложения на вашем сервере

Ниже — рабочий путь без туннелей: выкладка на ваш Linux-сервер и публикация через Nginx.

## Что уже добавлено в репозиторий

- Готовый Nginx-конфиг: `deploy/nginx/ai-orchestrator.conf`
- Скрипт автодеплоя по SSH: `scripts/publish_to_server.sh`

## 1) Что нужно на сервере

- Ubuntu/Debian сервер с публичным IP
- SSH-доступ пользователем с `sudo`
- Открытый порт 80 в firewall/security-group

## 2) Первый деплой (из этого репозитория)

```bash
SERVER_HOST=<IP_СЕРВЕРА> \
SERVER_USER=<SSH_ПОЛЬЗОВАТЕЛЬ> \
SERVER_PORT=22 \
SERVER_NAME=<ДОМЕН_ИЛИ__> \
./scripts/publish_to_server.sh
```

Пример:

```bash
SERVER_HOST=203.0.113.10 \
SERVER_USER=ubuntu \
SERVER_PORT=22 \
SERVER_NAME=app.example.com \
./scripts/publish_to_server.sh
```

Что делает скрипт:

1. Копирует `site/` на сервер в `/var/www/ai-orchestrator/site`
2. Устанавливает/обновляет Nginx-конфиг
3. Проверяет конфиг (`nginx -t`)
4. Перезагружает Nginx

После этого сайт доступен снаружи по адресу сервера (`http://IP`) или домена.

## 3) HTTPS (обязательно для production)

Когда домен уже смотрит на ваш сервер:

```bash
sudo apt-get update
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d <ВАШ_ДОМЕН>
```

## 4) Обновление после изменений

После правок в `site/index.html` просто снова запустите:

```bash
SERVER_HOST=<IP_СЕРВЕРА> SERVER_USER=<SSH_ПОЛЬЗОВАТЕЛЬ> ./scripts/publish_to_server.sh
```

## 5) Локальная проверка перед выкладкой

```bash
python -m http.server 4173 -d site
```

Открыть: `http://localhost:4173`.
