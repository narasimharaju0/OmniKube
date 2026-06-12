# OmniKube 🚀

OmniKube is a lightweight, cluster-native observability platform built to provide real-time telemetry, automated service discovery, and proactive alerting for Kubernetes infrastructure.

## 🏗️ Architecture
* **Agent:** Deployed as a DaemonSet to collect metrics from all nodes.
* **Aggregator:** Centralized service to process and normalize telemetry data.
* **Server:** Backend API managing data storage, analytics, and alert routing.
* **Discovery Service:** Automates the registration and monitoring of cluster targets.

## ⚡ Key Features
* **Automated Discovery:** Dynamically detects pods labeled `omnikube.io/scrape=true`.
* **Interactive Dashboard:** Real-time visualization of metrics with adjustable timeframes (1h, 6h, 12h, 24h).
* **Proactive Alerting:** Built-in system to maintain infrastructure health.

## 🚀 Getting Started

### Prerequisites
* Kubernetes cluster (e.g., Kind, Minikube)
* Python 3.x installed locally
* `kubectl` configured to access your cluster

### Installation
1. Clone the repository:
```bash
git clone https://github.com/narasimharaju0/OmniKube.git
cd OmniKube
```
### Usage
Once deployed, the `DiscoveryService` will automatically begin monitoring pods.
* **Access Dashboard:** Forward the port to your local machine:
```bash
  kubectl port-forward svc/omnikube-server 5000:5000
```
## 🛠️ Tech Stack
* **Languages:** Python
* **Orchestration:** Kubernetes (DaemonSets, Deployments)
* **Storage:** SQLite

## 📄 License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
