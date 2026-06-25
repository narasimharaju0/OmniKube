# Helm chart for OmniKube

The canonical Kubernetes manifests live in `infra/charts/omnikube/`. Legacy flat YAML files in `infra/` are retained for reference; prefer Helm for installs.

## Install

```bash
helm install omnikube ./infra/charts/omnikube -n default --create-namespace
```

## Upgrade with custom thresholds and stress mocks

```bash
helm upgrade omnikube ./infra/charts/omnikube -n default \
  --set server.image.tag=latest \
  --set server.thresholds.cpu=85 \
  --set server.thresholds.memory=90 \
  --set mockHighCpu.enabled=true
```

## Template dry-run

```bash
helm template omnikube ./infra/charts/omnikube
```

## Values highlights

| Key | Default | Description |
|-----|---------|-------------|
| `server.image.repository` | `cloudmetrics-server` | Management server image |
| `server.image.tag` | `latest` | Server image tag |
| `server.thresholds.cpu` | `80.0` | Default CPU alert threshold (%) |
| `server.thresholds.memory` | `80.0` | Default memory alert threshold (%) |
| `agent.enabled` | `true` | Deploy per-node metrics DaemonSet |
| `mockHighCpu.enabled` | `false` | Deploy mock high-CPU scrape targets |
