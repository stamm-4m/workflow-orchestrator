# STAMM — An Orchestrated ML Platform for Bioprocess Monitoring

**STAMM** is a workflow orchestration platform for automating the full
lifecycle of soft-sensor models in industrial bioprocesses. It connects
real-time sensor streams from fermentation, food manufacturing, and water
treatment processes to a registry of machine-learning models, runs
predictions continuously, monitors model drift against laboratory ground
truth, and orchestrates user-driven or drift-triggered retraining — all
through a single API-mediated interface.

This repository hosts the orchestration layer of the STAMM stack, built on
Apache Airflow 3.0.6. It is developed at INRAE within the Bioindustry 4.0
initiative.

---

## Motivation

Industrial bioprocesses generate dense, high-frequency multivariate data
(pH, dissolved oxygen, temperature, off-gas composition, feed rates) but
rarely grant real-time access to the state variables that matter most —
biomass concentration, product titer, specific growth rate. Soft-sensor
models estimate those variables from routinely measured signals, but
moving them from a notebook prototype into continuous operation requires
infrastructure that is typically absent in research settings:

1. A reproducible pipeline from raw ingestion to model inference.
2. Continuous performance monitoring against delayed ground truth.
3. Orchestrated retraining triggered by detected drift or by the engineer.
4. A clean separation between high-frequency time-series and the
   relational state that describes experiments, models, and user actions.

STAMM addresses the four concerns jointly, as an integrated system rather
than a collection of scripts.

---

## Contributions

- **End-to-end orchestration.** A single Airflow-based pipeline covers
  ingestion, prediction, drift analysis, and retraining.
- **Experiment-centric data model.** Every raw point, prediction, and
  model version is bound to a registered experiment, enabling exact
  replay and reproducible reporting.
- **API-mediated storage.** A backend service isolates DAGs from the
  underlying stores (InfluxDB for time-series, PostgreSQL for state),
  allowing DAGs and user-facing tools to share one contract.
- **Closed-loop drift-aware retraining.** Drift detectors feed a
  retraining job queue that users can also populate manually from the UI.
- **Domain-agnostic by design.** The system is currently validated on
  penicillin, *E. coli*, and *Pichia pastoris* fermentations and is
  structured to accept arbitrary bioprocess projects via configuration.

---

## Architecture


- **Physical layer.** Sensors, actuators, and computed variables from
  bioreactors stream through LEAF (edge gateway) into the time-series
  database.
- **Time-series layer.** InfluxDB holds raw sensor observations and
  prediction series (the only stores that require temporal-window access).
- **State layer (in development).** PostgreSQL holds the experiment
  registry, model-run history, retraining jobs, drift results, and data
  quality records.
- **Backend API (in development).** A FastAPI service unifies access to
  both stores and exposes a single surface to DAGs and user interfaces.
- **Model Registry.** A separate FastAPI + Streamlit service for model
  discovery, versioning, and inference.
- **Orchestration.** Apache Airflow executes the DAGs specified in
  [docs/dags.md](docs/dags.md).

---

## Current state and trajectory

One DAG, `STAMM_Predictions`, is in production today. It covers the
baseline cycle — health check, snapshot construction, prediction,
write-back — and currently communicates with InfluxDB directly.

Active work introduces the PostgreSQL state layer and the backend API,
and adds five new DAGs: experiment lifecycle, drift detection, retraining
trigger, experiment close, and data quality..

---

## Documentation map

| Document | Audience | Purpose |
|---|---|---|
| [docs/dags.md](docs/dags.md) | Developers, reviewers | Specification of every current and planned DAG |
| [docs/user-manual.md](docs/user-manual.md) | Researchers, bioprocess engineers | Day-to-day operation of STAMM |

---

## Affiliation and contact

Developed at **INRAE** within the **Bioindustry 4.0** program.
Contact: Alexander Astudillo — `jairo.astudillo-lagos@inrae.fr`

## Citation

A citation entry will be added once the companion publication is available.