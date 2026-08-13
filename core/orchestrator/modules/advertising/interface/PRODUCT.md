# AI Orchestrator — Product Contract

## Product loop

The product is not a visual workflow builder. The primary user loop is:

`Container → Collect → Prepare → Explore → Forecast → Decide → Act`

The visual workflow runtime remains the durable execution layer underneath this loop and an expert-mode editor.

## 1. Container is the top-level working object

A container may represent a client, process group, project or experiment. A container owns or references:

- workflows/scenarios;
- data products;
- analytical models and their latest results;
- client context when applicable;
- future recommendations and actions.

Selecting a container once should be enough for downstream product screens to know the working context.

## 2. Simple scenarios compile to workflows

Default UX asks what should happen rather than which nodes should be connected.

The first compiler flow is:

`intent/documentation → inactive API draft → Start → API → Finish workflow draft`

Generated workflows are reviewable but are not automatically published or executed. The existing graph remains expert mode.

## 3. Data Product is the boundary between collection and analytics

Raw tables are implementation details. Product UX should infer a semantic schema from a source:

- time;
- entity;
- scope;
- metric;
- dimension.

The inferred schema, warnings and preparation rules are stored as a Data Product. Physical clean/business materialization may be implemented by workflows or dedicated data operations without changing the product contract.

## 4. Analytics discovers structure automatically

The user should not have to guess useful X/Y/Z axes first. The analytics layer scans numerical features, ranks relationships and then offers the 3D Space as an exploration surface.

3D is a production feature only when backed by real rows. Random/generated business rows must never be presented as real analytics.

## 5. Forecasting is expressed as a business question

Users specify a metric, horizon and optionally a target. The product chooses a suitable model. The first built-in model is deterministic-seed Monte Carlo around an observed trend and residual variance. Results expose a distribution, not a single false-precision number:

- median;
- uncertainty interval;
- probability of reaching a target;
- trend/noise diagnostics.

Future model adapters can add time-series, Bayesian, regression/boosting, anomaly detection and clustering behind the same product behavior.

## 6. Decisions close the loop

Insights and forecasts should be convertible into recommendations and then into controlled workflow actions. Execution remains durable, observable and reviewable through the existing PostgreSQL workflow runtime.

## Runtime independence

The product must remain self-hosted and operational without donor repositories or hosted donor workflow services. External projects may inform design; production runtime must use repository-owned code and explicit external APIs selected by the user.
