# Публикация страницы на вашем сервере (Apache)

Цель: после деплоя страница из `site/index.html` должна открываться по `http://<SSH_HOST>` на 80 порту.

## Какие секреты нужны в GitHub

Обязательные:

- `SSH_HOST` — IP сервера (пример: `155.212.162.184`)
- `SSH_USER` — SSH-пользователь (пример: `admin`)
- `SSH_PRIVATE_KEY` — приватный ключ для входа по SSH

Опциональные:

- `SSH_PORT` — SSH порт (по умолчанию `22`)
- `WEB_ROOT` — куда копировать сайт (по умолчанию `/var/www/html`)
- `SERVER_NAME` — домен для внешней проверки (если нет домена, не заполняйте)

## Что делает деплой теперь

Скрипт `scripts/publish_to_server.sh`:

1. Копирует `site/*` в `/var/www/html/` (или в `WEB_ROOT`)
2. Убеждается, что `apache2` установлен
3. Проверяет конфиг Apache (`apache2ctl configtest`)
4. Перезагружает Apache (`reload`/`restart`)

## Запуск

1. GitHub → `Actions`
2. Выберите `Deploy public site`
3. Нажмите `Run workflow`

## Автоматическая проверка после деплоя

Workflow делает `curl` по реальному URL:

- если `SERVER_NAME` не задан (или мусор вроде `prod`) → проверяет `http://$SSH_HOST/`
- если `SERVER_NAME` задан корректным доменом/IP → проверяет `http://$SERVER_NAME/`

Если код ответа не `HTTP 200` — workflow падает с ошибкой.

## Безопасность

Секреты храните только в GitHub Secrets, не в репозитории.
