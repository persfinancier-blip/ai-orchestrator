# Публикация страницы на вашем сервере (Apache, без sudo)

Цель: после деплоя страница из `site/index.html` открывается по `http://<SSH_HOST>` на 80 порту.

## Обязательные GitHub Secrets

- `SSH_HOST` (пример: `155.212.162.184`)
- `SSH_USER` (пример: `admin`)
- `SSH_PORT` (обычно `22`)
- `SSH_PRIVATE_KEY`
- `WEB_ROOT` (для вашего сервера: `/sourcecraft.dev/app/ai-orchestrator`)

## Что делает деплой

Скрипт `scripts/publish_to_server.sh` теперь работает **без sudo**:

1. Проверяет, что папка `WEB_ROOT` существует и доступна на запись для `SSH_USER`
2. Копирует `site/*` в `WEB_ROOT` через `rsync --delete`
3. Проверяет наличие `${WEB_ROOT}/index.html`

Если деплой падает на правах доступа, проверьте на сервере:

```bash
ls -ld /sourcecraft.dev/app /sourcecraft.dev/app/ai-orchestrator
id
```

Пользователь `SSH_USER` должен иметь запись в `WEB_ROOT`.

## Запуск

1. GitHub → `Actions`
2. Выберите `Deploy public site`
3. Нажмите `Run workflow`

## Автопроверка после деплоя

Workflow проверяет только реальный URL по IP:

- `http://$SSH_HOST/`

Если не `HTTP 200`, workflow падает.

## Примечание про Apache

Раз у вас `/var/www/html` уже симлинк на `/sourcecraft.dev/app/ai-orchestrator`,
достаточно копировать в `WEB_ROOT=/sourcecraft.dev/app/ai-orchestrator`.
