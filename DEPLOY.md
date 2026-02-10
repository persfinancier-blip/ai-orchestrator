# Публикация страницы на вашем сервере (без туннеля)

Туннель нужен только для временных демо. Для постоянного внешнего доступа лучше размещать статику на вашем сервере.

## 1) Подготовить сервер

Нужны:

- Linux-сервер с публичным IP
- домен (опционально, но рекомендуется)
- установленный Nginx

## 2) Загрузить страницу на сервер

В репозитории страница лежит в `site/index.html`.

Скопируйте её в веб-директорию сервера:

```bash
sudo mkdir -p /var/www/ai-orchestrator/site
sudo cp site/index.html /var/www/ai-orchestrator/site/index.html
```

## 3) Настроить Nginx

Пример конфига `/etc/nginx/sites-available/ai-orchestrator`:

```nginx
server {
    listen 80;
    server_name your-domain.com;

    root /var/www/ai-orchestrator/site;
    index index.html;

    location / {
        try_files $uri $uri/ =404;
    }
}
```

Активировать сайт:

```bash
sudo ln -s /etc/nginx/sites-available/ai-orchestrator /etc/nginx/sites-enabled/ai-orchestrator
sudo nginx -t
sudo systemctl reload nginx
```

После этого страница будет доступна снаружи по адресу:

- `http://your-domain.com`

## 4) Включить HTTPS (рекомендуется)

Если домен направлен на сервер, подключите TLS:

```bash
sudo apt-get update
sudo apt-get install -y certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## 5) Обновление страницы

При изменениях достаточно заново скопировать `site/index.html` и перезагрузить Nginx:

```bash
sudo cp site/index.html /var/www/ai-orchestrator/site/index.html
sudo systemctl reload nginx
```

---

## Быстрая локальная проверка перед деплоем

```bash
python -m http.server 4173 -d site
```

Проверка в браузере: `http://localhost:4173`.
