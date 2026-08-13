# AI Orchestrator — Product Contract

AI Orchestrator is a self-hosted operating system for marketplace advertising operations.

## Primary user flow

1. Select a client once in the global workspace context.
2. Verify marketplace connections, goals and KPI in the client passport.
3. Describe a new integration to the Assistant or review an existing API definition manually.
4. Build, publish and run a durable automation scenario.
5. Inspect operational data, run errors and resulting tables/views.
6. Use advanced analytics only after a real Gold/Showcase source is connected.

## Product surfaces

- **Overview** — readiness, clients, connections and recent workflow health.
- **Clients** — client passport, legal/contract data, goals, KPI and marketplace access.
- **Advertising** — client-centered operations hub. It must not expose mock campaign controls as real actions.
- **Assistant** — natural-language/documentation to validated, inactive API draft using the existing internal Node Assistant.
- **Scenarios** — durable PostgreSQL-backed workflow execution, publication, schedules and history.
- **API manually** — expert review and editing of API templates created manually or by the Assistant.
- **Data** — source tables, transformations and Gold views.
- **Advanced 3D analytics** — not a production surface until it reads a verified real showcase. Generated demo points are forbidden in the product path.

## Architecture rules

- PostgreSQL is the system state and durable queue.
- External workflow runtimes, donor repositories and hosted donor servers are not production dependencies.
- Marketplace integrations are adapters behind internal contracts.
- UI never owns security or orchestration truth.
- A browser-supplied role is never authentication. The server validates the signed HttpOnly session and injects the trusted role itself.
- The selected client is server-side product context, not a separate copy of client state in every screen.
- Client credentials must never be returned to browser JavaScript in clear text. Existing values are represented by a mask and a masked edit must preserve the stored value.
- Assistant-generated API templates are inactive by default, require an HTTP(S) endpoint, use an HTTP method whitelist and must strip detected credentials before persistence.
- Existing working engines are reused behind the product shell instead of being rewritten without need.
- New modules must have one responsibility and explicit contracts.
- A feature that only logs an action, generates random business data or simulates a result is not a production feature and must not be presented as one.

## Release blockers for the current product branch

- **#10** — the legacy manual workflow trigger drops the selected `client_id` at the monolithic trigger-handler boundary. Do not repair this by updating run rows after enqueue because the worker may already claim the job.
- **#11** — production deployment must provision `AO_CONTROL_TOKEN` and validate the authenticated product before declaring deploy success.
- Client access values are masked at the API boundary, but encryption-at-rest for marketplace credentials still requires a dedicated key/migration before storing production secrets at scale.
- Full product `npm test` + `npm run build` must pass in CI or an equivalent checked-out environment before this draft PR can become release-ready.

## Productization order

1. Keep one product shell and one trusted API boundary.
2. Make Clients the primary operational context.
3. Prefer Assistant -> inactive draft -> human review over manual API form entry.
4. Keep workflow publish/run/history on the existing durable PostgreSQL runtime.
5. Remove or quarantine demo/mock surfaces instead of polishing them.
6. Close release blockers #10 and #11.
7. Add regression CI, production smoke tests and encrypted credential storage.
8. Reintroduce the 3D Space only on verified real marketplace data.
