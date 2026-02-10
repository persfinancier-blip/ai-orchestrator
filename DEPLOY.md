# Deploy to production server

Target server setup:

- `SSH_HOST=155.212.162.184`
- `SSH_USER=sourcecraft`
- `SSH_PORT=22`
- `WEB_ROOT=/sourcecraft.dev/app/ai-orchestrator`
- Apache serves this folder (`/var/www/html` is symlinked to it)

## Required GitHub Secrets

- `SSH_HOST`
- `SSH_USER`
- `SSH_PORT`
- `SSH_PRIVATE_KEY`
- `WEB_ROOT`

## Deployment model

- Trigger only: `workflow_dispatch`
- Branch allowed for deploy: `main` only (workflow explicitly fails if launched from another branch)
- Build happens in Actions (`./scripts/build_site.sh`)
- Only build artifacts are uploaded:
  - `core/orchestrator/modules/advertising/interface/dist`
- Upload mechanism: `tar-over-ssh`
- `sudo` is not used

## How to run deploy

1. Push/merge changes to `main`.
2. Open GitHub → `Actions` → `Deploy site`.
3. Click `Run workflow` on branch `main`.
4. Wait for green status.

## Post-deploy verification

Workflow checks:

```bash
curl http://$SSH_HOST/
```

Expected response code: `HTTP 200`.
