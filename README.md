# AI Orchestrator

Repository with legacy AI Orchestrator materials plus deployable web interface.

## Project type

This project includes a Svelte + Vite frontend in:

- `core/orchestrator/modules/advertising/interface`

So deployment requires a build step (`vite build`) and then publishing build artifacts.

## Local run

```bash
cd core/orchestrator/modules/advertising/interface
npm install
npm run dev
```

## Local production build

```bash
./scripts/build_site.sh
```

Build output:

- `core/orchestrator/modules/advertising/interface/dist`

## Deployment

Deployment is done via GitHub Actions workflow dispatch from `main` only.
See `DEPLOY.md` for secrets and runbook.
