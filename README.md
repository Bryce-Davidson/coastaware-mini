# CoastAware-Mini

A hands-on, lab-scale clone of MarineLabs’ real-time coastal intelligence stack.
It ingests buoy telemetry, spots anomalies, predicts wave conditions, and serves everything over an API and live dashboard.

## Resources

https://www.ndbc.noaa.gov/faq/measdes.shtml

---

## 🌊 What's inside?

### Live Ingest (mirrors CoastAware)

Kinesis ➜ Lambda ➜ Timestream & S3 for sub-second streaming persistence

### Anomaly Engine (mirrors BerthWatch)

Containerised Lambda/Fargate workers flag rogue waves & sensor faults

### Forecast Jobs (mirrors CoastInsights)

Nightly SageMaker batch trains an XGBoost model for 6-hour wave predictions

### API Service (mirrors CoastAware API)

FastAPI on ECS (`/latest`, `/forecast`, `/anomaly`) + WebSockets

### Dashboard (mirrors CoastAware Dashboard)

Grafana board shows live traces, forecasts, and system health

### Ops Backbone (mirrors Shared Platform)

Terraform IaC, Docker, GitHub Actions CI/CD, observability with CloudWatch & Prometheus

---

## Quick start

TODO
