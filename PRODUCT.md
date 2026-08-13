# AI Orchestrator — Product Contract

AI Orchestrator is a self-hosted operating system for marketplace advertising operations.

## Primary user flow

1. Open a client.
2. Verify marketplace connections, goals and KPI.
3. Review advertising state and problems.
4. Run or schedule an automation scenario.
5. Inspect the result, errors and history.
6. Use data tables and multidimensional Space only when deeper analysis is needed.

## Product surfaces

- **Overview** — operational entry point.
- **Clients** — client passport, goals, KPI, agreements and marketplace access.
- **Advertising** — campaigns, bids, actions and performance.
- **Scenarios** — durable workflow execution and schedules.
- **Integrations** — external API definitions and data exchange.
- **Data** — source tables, transformations and Gold views.
- **Space** — advanced multidimensional analysis.

## Architecture rules

- PostgreSQL is the system state and durable queue.
- External workflow runtimes are not production dependencies.
- Marketplace integrations are adapters behind internal contracts.
- UI never owns business logic.
- A browser-supplied role is not an authentication mechanism.
- Existing working engines are reused behind the product shell instead of being rewritten without need.
- New modules must have one responsibility and explicit contracts.

## Productization order

1. Unify navigation and entry point.
2. Replace browser role spoofing with real authentication/session handling.
3. Connect Overview to operational state.
4. Make Clients the primary context for advertising, data and automation.
5. Add agent-assisted scenario/integration authoring on top of existing node schemas.
6. Consolidate deployment and remove legacy entry points/artifacts.
7. Add regression CI and production smoke tests.
