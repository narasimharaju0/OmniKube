# OmniKube Alert Deduplication Stress Test
# Deploys mock high-CPU agents + cluster-wide CPU stress, then tails server logs
# for AlertBuffer catch / group / flush events.

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Write-Host "=== OmniKube Alert Stress Test ===" -ForegroundColor Cyan

Write-Host "`n[1/5] Applying agent DaemonSet (scrape label)..." -ForegroundColor Yellow
kubectl apply -f "$Root\infra\agent-daemonset.yaml"

Write-Host "`n[2/5] Labeling existing agent pods (if any pre-date manifest)..." -ForegroundColor Yellow
kubectl label pod -l app=omnikube-agent omnikube.io/scrape=true --overwrite 2>$null

Write-Host "`n[3/5] Deploying mock high-CPU metric agents (3 replicas @ CPU 92%)..." -ForegroundColor Yellow
kubectl apply -f "$Root\infra\mock-high-cpu-agents.yaml"

Write-Host "`n[4/5] Deploying cluster-wide CPU stress DaemonSet..." -ForegroundColor Yellow
kubectl apply -f "$Root\infra\cpu-stress-daemonset.yaml"

Write-Host "`n[5/5] Waiting for mock agents to become Ready..." -ForegroundColor Yellow
kubectl rollout status deployment/omnikube-mock-high-cpu --timeout=120s

Write-Host "`n=== Workload Status ===" -ForegroundColor Cyan
kubectl get pods -l omnikube.io/scrape=true -o wide
kubectl get daemonset omnikube-cpu-stress
kubectl get deployment omnikube-mock-high-cpu

Write-Host "`n=== Verification Commands ===" -ForegroundColor Green
Write-Host @"

# Watch AlertBuffer lifecycle (catch -> group -> flush) — wait up to 60s for window flush
kubectl logs -f deployment/omnikube-server | Select-String -Pattern "Alert caught|Alert buffer|deduplication|Alert group|Alert flush"

# One-shot grep of recent alert activity
kubectl logs deployment/omnikube-server --tail=200 | Select-String -Pattern "Alert caught|deduplication grouped|Alert buffer flush complete|Alert flush suppressed"

# Confirm discovery sees labeled scrape targets
kubectl port-forward svc/omnikube-server-service 8080:80
curl -H "X-OmniKube-Token: premium_secret_2026" http://localhost:8080/api/targets

# Sample mock agent metrics directly
`$mockPod = kubectl get pod -l app=mock-high-cpu -o jsonpath='{.items[0].metadata.name}'
kubectl exec `$mockPod -- wget -qO- http://127.0.0.1:8080/metrics

"@

Write-Host "Tailing server logs now (Ctrl+C to stop)..." -ForegroundColor Yellow
kubectl logs -f deployment/omnikube-server 2>&1 | Select-String -Pattern "Scrape cycle|Alert caught|Alert buffer|deduplication|Alert group|Alert flush|mock-high-cpu"
