# OmniKube

**High-performance multi-tenant FinOps and cloud metrics daemon platform for Kubernetes.**

OmniKube unifies real-time cluster telemetry, automated cost optimization, and SaaS subscription lifecycle management in a single management plane. The platform is designed for operators who need production-grade observability, rightsizing intelligence, and billing automation without sacrificing deployment simplicity.

---

## Overview

OmniKube collects metrics from Kubernetes workloads, persists tenant-scoped telemetry in SQLite, and surfaces actionable FinOps recommendations through a polished operator console. A background calculation daemon continuously analyzes cluster utilization and writes savings opportunities to the ORM layer, while the management API exposes versioned REST endpoints for dashboards, optimization workflows, compliance, and billing.

| Capability | Description |
|------------|-------------|
| **Multi-tenant telemetry** | Organization-scoped metrics ingestion, analytics, and RBAC-aware API access |
| **FinOps daemon** | Background savings analysis with idle-node and rightsizing detection |
| **Cluster-native collection** | In-cluster metrics collector with discovery-driven scrape targets |
| **SaaS billing** | Stripe Checkout sessions and signed webhook subscription activation |
| **Production delivery** | Multi-stage Docker builds, K3s manifests, and GitHub Actions CI/CD |

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────────────┐
│  Agent DaemonSet │────▶│    Aggregator    │────▶│  Management Server (app.py) │
│  (node metrics)  │     │  (normalization) │     │  API · ORM · FinOps · UI    │
└─────────────────┘     └──────────────────┘     └──────────────┬──────────────┘
                                                                 │
                    ┌────────────────────────────────────────────┼────────────────┐
                    ▼                                            ▼                ▼
            Discovery Service                          Metrics Collector    Cost Optimizer
            (auto target registration)                 (K8s poll loop)      (savings daemon)
```

**Core runtime components**

- **Server** (`server/`) — HTTP management gateway, ORM persistence, authentication, billing, and HTML dashboards
- **Agent** (`agent/`) — DaemonSet-oriented node and pod metrics collection
- **Aggregator** (`aggregator/`) — Central telemetry normalization and forwarding
- **Infrastructure** (`infra/`) — Kubernetes manifests and Helm chart for cluster deployment

---

## Front-End Experience

OmniKube ships a custom **emerald-themed** operator interface built with a dark glass aesthetic, gradient accents (`#10b981` / `emerald-400`), and high-contrast metric cards.

| Surface | Route | Purpose |
|---------|-------|---------|
| Landing | `/` | Public marketing page, pricing tiers, and product narrative |
| Login | `/login` | Authenticated session entry |
| Dashboard | `/dashboard` | Live cluster telemetry and utilization cards |
| Cost Optimization | `/cost-optimization` | FinOps recommendations, overrides, and migration actions |
| Admin Settings | `/admin/settings` | Budget toggles, cloud account badges, and API key rotation |

Templates live under `server/templates/` with static assets in `server/static/`.

---

## FinOps Calculation Daemon

The FinOps engine runs as a resilient background service inside the management server process.

**`server/core/cost_optimizer.py`** analyzes ORM `ClusterMetrics` samples and persists `CostRecommendations` rows when utilization patterns indicate waste:

- **Idle node detection** — average CPU and memory below 15% → up to 60% projected savings
- **Rightsizing** — moderate utilization (15–40%) → up to 35% projected savings

**`start_savings_analysis_loop()`** in `server/app.py` launches a daemon thread on startup. Initialization is wrapped in try/except guards so transient cluster network loss does not take down the HTTP gateway.

Related API endpoints:

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/v1/optimization/recommendations` | Active savings recommendations |
| `POST` | `/api/v1/optimization/apply` | Apply remediation workflow |
| `POST` | `/api/v1/optimization/override` | Guardrail override token consumption |
| `POST` | `/api/v1/optimization/migrate` | Cross-cloud migration execution |
| `GET` | `/api/v1/cost/optimize` | Aggregated optimization report |

---

## Billing and Stripe Integration

Subscription lifecycle management is implemented in **`server/core/billing.py`** with three public tiers:

| Plan | Tier key | Monthly price |
|------|----------|---------------|
| Developer | `developer` | Free |
| Growth Scale | `growth` | $79 |
| Enterprise Core | `enterprise` | $299 |

**Checkout** — `POST /api/v1/billing/checkout` creates a Stripe Checkout session and returns a hosted payment URL.

**Webhook** — `POST /api/v1/billing/stripe/webhook` verifies the `Stripe-Signature` header against `STRIPE_WEBHOOK_SECRET`, constructs the event with `stripe.Webhook.construct_event`, and activates the matching ORM user on `checkout.session.completed`.

Required environment variables:

```bash
STRIPE_SECRET_KEY=sk_live_...
STRIPE_WEBHOOK_SECRET=whsec_...
STRIPE_PRICE_GROWTH=price_...
STRIPE_PRICE_ENTERPRISE=price_...
```

---

## Repository Structure

```
cloudmetrics/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD pipeline (main branch)
├── agent/                      # Node-level metrics collector
├── aggregator/                 # Telemetry aggregation service
├── infra/                      # Kubernetes & Helm deployment assets
│   └── charts/omnikube/        # Helm chart
├── scripts/                    # Operational helper scripts
├── server/                     # Management server (primary application)
│   ├── app.py                  # HTTP gateway and route dispatcher
│   ├── core/                   # Platform modules (auth, billing, FinOps, ORM)
│   ├── templates/              # Emerald-themed HTML dashboards
│   ├── static/                 # Brand assets and front-end resources
│   ├── Dockerfile              # Development container image
│   ├── Dockerfile.prod         # Multi-stage production image
│   ├── production-stack.yaml   # Unified K3s manifest (PVC, Deployment, Ingress)
│   ├── requirements.txt        # Python dependencies
│   └── rbac.yaml               # In-cluster metrics RBAC
└── README.md
```

---

## Local Development

### Prerequisites

- Python 3.11+
- pip
- Kubernetes cluster (Kind, Minikube, or K3s) for in-cluster collection features
- `kubectl` configured against your target cluster

### Run the management server

```bash
cd server
pip install -r requirements.txt

export OMNIKUBE_DATA_DIR=./.local-data
export STRIPE_SECRET_KEY=sk_test_placeholder
export STRIPE_WEBHOOK_SECRET=whsec_placeholder

python app.py
```

The gateway listens on **http://0.0.0.0:5000**.

| URL | Access |
|-----|--------|
| http://localhost:5000/ | Public landing |
| http://localhost:5000/dashboard | Protected dashboard |
| http://localhost:5000/cost-optimization | FinOps console |
| http://localhost:5000/admin/settings | Admin settings |

Mock accounts for local testing: `admin_user`, `editor_user`, `viewer_user` — password `changeme`.

### Build the production image

```bash
docker build -f server/Dockerfile.prod -t omnikube-server:latest server/
```

---

## Production Deployment

**K3s / Kubernetes**

```bash
# Build and tag the production image
docker build -f server/Dockerfile.prod -t omnikube:production server/

# Apply RBAC and the unified production stack
kubectl apply -f server/rbac.yaml
kubectl apply -f server/production-stack.yaml
```

`server/production-stack.yaml` provisions:

- **PVC** `omnikube-db-pvc` (5Gi, ReadWriteOnce) for durable `omnikube.db` storage
- **Deployment** `omnikube-prod` with non-root security context and Stripe env placeholders
- **Service** `omnikube-prod-svc` (ClusterIP, port 5000)
- **Ingress** with cert-manager Let's Encrypt and forced HTTPS redirect

---

## CI/CD Pipeline

Every push to **`main`** triggers **`.github/workflows/deploy.yml`**.

The `build-and-validate` job runs on `ubuntu-latest` with `working-directory: ./server` for all Python steps.

| Stage | Action |
|-------|--------|
| **Checkout** | Full repository fetch via `actions/checkout@v4` |
| **Python setup** | Python 3.11 with pip cache (`server/requirements.txt`) |
| **Dependencies** | `pip install -r requirements.txt` |
| **Code integrity** | `python -m compileall -q .` plus `import app` smoke test |
| **Docker Buildx** | `docker/setup-buildx-action@v3` with GHA layer cache |
| **Image build** | `docker/build-push-action@v6` — `context: ./server`, `file: ./server/Dockerfile.prod` |

The validation step injects CI-safe environment variables so module import does not fail on missing secrets:

```yaml
STRIPE_SECRET_KEY: "mock_key_for_ci"
STRIPE_WEBHOOK_SECRET: "mock_secret_for_ci"
LOG_LEVEL: "DEBUG"
OMNIKUBE_DATA_DIR: "./ci-data"
PYTHONPATH: "."
```

`PYTHONPATH: "."` ensures `core.*` relative imports resolve cleanly from the `server/` working directory.

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| Runtime | Python 3.11, `http.server` management gateway |
| ORM / storage | SQLAlchemy 2.x, SQLite (`omnikube.db`) |
| Auth | Werkzeug password hashing, session cookies, OAuth2/OIDC hooks |
| Billing | Stripe Checkout + signed webhooks |
| Orchestration | Kubernetes DaemonSets, Deployments, Ingress |
| CI/CD | GitHub Actions, Docker Buildx multi-stage builds |
| Front-end | Server-rendered HTML, Tailwind-style emerald dark-glass theme |

---

## License

This project is licensed under the **MIT License**. See [LICENSE](LICENSE) for details.
