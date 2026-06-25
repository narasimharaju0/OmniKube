# OmniKube Infrastructure

## Helm chart (recommended)

Production-style installs use the **`omnikube`** Helm chart:

```
infra/charts/omnikube/
├── Chart.yaml
├── values.yaml
└── templates/
    ├── server-deployment.yaml
    ├── server-service.yaml
    ├── server-persistence.yaml
    ├── server-rbac.yaml
    ├── aggregator-deployment.yaml
    ├── agent-daemonset.yaml
    ├── mock-high-cpu-deployment.yaml
    ├── cpu-stress-daemonset.yaml
    └── load-generator.yaml
```

```bash
helm install omnikube ./infra/charts/omnikube -n default --create-namespace
```

## Legacy flat manifests

The YAML files in this directory (`server-deployment.yaml`, `agent-daemonset.yaml`, etc.) are kept for quick Kind/local reference. They have been superseded by the Helm chart templates above.

`kind-cluster.yaml` is used only for local Kind cluster bootstrap and is not part of the Helm chart.
