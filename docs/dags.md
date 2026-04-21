# STAMM DAGs — Specification

This document specifies every Airflow DAG in the STAMM platform: the one
in production today and the ones under active development. Each DAG is
described with its purpose, trigger, task list, API dependencies, and
downstream effects.

All planned DAGs assume the backend API + PostgreSQL state layer described
in [workflows.md](workflows.md). Until that layer is available,
`stamm_predictions` continues to operate against InfluxDB directly in its
legacy form.

---

## Index

1. [stamm_predictions](#1-stamm_predictions) — in production, refactor planned
2. [experiment_lifecycle](#2-experiment_lifecycle) — planned
3. [drift_detection](#3-drift_detection) — planned
4. [retraining_trigger](#4-retraining_trigger) — planned
5. [experiment_close](#5-experiment_close) — planned
6. [data_quality](#6-data_quality) — planned

---

## 1. `stamm_predictions`

**Status.** In production; refactor planned for the API migration.

**Purpose.** Continuously generate soft-sensor predictions for every active
bioreactor experiment.

**Trigger.** 20-second polling schedule.

**Legacy implementation.** Single DAG in [dags/airflow_dag.py](dags/airflow_dag.py),
supported by [dags/tasks/influx.py](dags/tasks/influx.py) and
[dags/tasks/prediction.py](dags/tasks/prediction.py). Reads from and writes to
InfluxDB directly; auto-discovers models from the Model Registry per run.

**Target implementation.** Reads snapshots through the backend API; posts
predictions through the backend API; Postgres records an audit row per
(experiment, model, version, timestamp); InfluxDB continues to hold the
time-series value.

### Tasks (target state)

    # 1. Health check
    check_api_health_task = PythonOperator(
        task_id="check_api_health",
        python_callable=ping_backend_api,
        doc_md="Pings the STAMM backend API before any read or write.",
    )

    # 2. Fetch latest snapshots for active experiments
    fetch_snapshots_task = PythonOperator(
        task_id="fetch_snapshots",
        python_callable=get_snapshots_from_api,
        doc_md="GET /experiments/active/snapshots — pivoted feature rows.",
    )

    # 3. Load active models per experiment
    load_models_task = PythonOperator(
        task_id="load_active_models",
        python_callable=get_experiment_models,
        doc_md="GET /experiments/{id}/models — resolves model id + version set.",
    )

    # 4. Run predictions
    run_predictions_task = PythonOperator(
        task_id="run_model_predictions",
        python_callable=call_models_from_snapshots,
        doc_md="Posts each (snapshot, model) pair to the Model Registry.",
    )

    # 5. Persist predictions
    store_predictions_task = PythonOperator(
        task_id="store_predictions",
        python_callable=post_predictions_to_api,
        doc_md="POST /experiments/{id}/predictions — dual-write via backend.",
    )

---

## 2. `experiment_lifecycle`

**Status.** Planned.

**Purpose.** Detect new bioreactor batches in the raw bucket and register
them as experiments in Postgres. Downstream DAGs reference the resulting
`experiment_id` rather than re-deriving tags.

**Trigger.** 1-minute polling schedule.

### Tasks

    # 1. Verify backend availability
    check_api_health_task = PythonOperator(
        task_id="check_api_health",
        python_callable=ping_backend_api,
        doc_md="Confirms the STAMM API is reachable before registration work.",
    )

    # 2. Discover new batches
    detect_batches_task = PythonOperator(
        task_id="detect_new_batches",
        python_callable=find_unregistered_batches,
        doc_md="Scans InfluxDB for (device_id, project, batch) groups absent from Postgres.",
    )

    # 3. Register experiments
    register_experiments_task = PythonOperator(
        task_id="register_experiments",
        python_callable=post_new_experiments,
        doc_md="POST /experiments — inserts one row per newly detected batch.",
    )

    # 4. Attach applicable models
    attach_models_task = PythonOperator(
        task_id="attach_applicable_models",
        python_callable=link_models_to_experiment,
        doc_md="POST /experiments/{id}/models — binds project-matched models and versions.",
    )

---

## 3. `drift_detection`

**Status.** Planned. Wraps the existing standalone drift-detection package.

**Purpose.** Quantify divergence between predictions and laboratory ground
truth over recent completed experiments; raise alerts and optionally seed
retraining jobs when thresholds are breached.

**Trigger.** Daily scheduled run; additional on-demand trigger exposed
through the UI.

### Tasks

    # 1. Fetch historical ground truth & predictions
    fetch_evaluation_data_task = PythonOperator(
        task_id="fetch_evaluation_data",
        python_callable=get_historical_predictions_and_actuals,
        doc_md="""
        Calls the backend API to retrieve a window of past predictions alongside
        the actual verified laboratory results (ground truth) stored in Postgres
        for completed batches.
        """
    )

    # 2. Compute drift metrics
    compute_drift_task = PythonOperator(
        task_id="compute_drift_metrics",
        python_callable=run_drift_detectors,
        doc_md="Runs PSI, KS and residual-distribution tests per (model, version).",
    )

    # 3. Persist drift results
    persist_drift_task = PythonOperator(
        task_id="persist_drift_results",
        python_callable=post_drift_results,
        doc_md="POST /drift/results — feeds the UI's Model Health view.",
    )

    # 4. Raise alerts
    raise_alerts_task = PythonOperator(
        task_id="raise_drift_alerts",
        python_callable=emit_drift_alerts,
        doc_md="POST /alerts for any metric exceeding its configured threshold.",
    )

    # 5. Auto-queue retraining (conditional)
    auto_queue_task = PythonOperator(
        task_id="auto_queue_retraining_if_needed",
        python_callable=enqueue_retraining_on_drift,
        doc_md="POST /retraining/jobs with status='pending' when auto-retrain fires.",
    )

---

## 4. `retraining_trigger`

**Status.** Planned.

**Purpose.** Execute retraining jobs submitted by users from the UI or
enqueued by the drift DAG; update the Model Registry with the resulting
model version.

**Trigger.** 5-minute polling; fast path when a new job is inserted.

### Tasks

    # 1. Poll pending retraining jobs
    poll_jobs_task = PythonOperator(
        task_id="poll_pending_retraining_jobs",
        python_callable=fetch_pending_retraining_jobs,
        doc_md="GET /retraining/jobs?status=pending.",
    )

    # 2. Assemble training dataset
    assemble_dataset_task = PythonOperator(
        task_id="assemble_training_dataset",
        python_callable=build_training_dataset,
        doc_md="Joins Postgres experiment metadata with InfluxDB sensor series.",
    )

    # 3. Trigger Model Registry training
    trigger_training_task = PythonOperator(
        task_id="trigger_model_registry_training",
        python_callable=submit_training_to_registry,
        doc_md="POST /{project_id}/train/{model_id} and await new version id.",
    )

    # 4. Promote or stage the new version
    promote_model_task = PythonOperator(
        task_id="promote_new_model_version",
        python_callable=promote_or_stage_model,
        doc_md="POST /models/{id}/versions/{v}/stage — staging or production.",
    )

    # 5. Close out the job
    update_job_task = PythonOperator(
        task_id="update_retraining_job_status",
        python_callable=mark_retraining_job_complete,
        doc_md="PATCH /retraining/jobs/{id} — terminal status and metrics.",
    )

---

## 5. `experiment_close`

**Status.** Planned.

**Purpose.** Transition running experiments to `completed` when raw data
stops arriving, and precompute summaries that the UI's "past experiments"
view depends on.

**Trigger.** Hourly schedule.

### Tasks

    # 1. List running experiments
    list_active_task = PythonOperator(
        task_id="list_active_experiments",
        python_callable=fetch_running_experiments,
        doc_md="GET /experiments?status=running.",
    )

    # 2. Evaluate staleness
    evaluate_staleness_task = PythonOperator(
        task_id="evaluate_experiment_staleness",
        python_callable=check_experiment_last_seen,
        doc_md="Checks the most recent raw timestamp against the idle threshold.",
    )

    # 3. Close stale experiments
    mark_completed_task = PythonOperator(
        task_id="mark_experiments_completed",
        python_callable=close_stale_experiments,
        doc_md="PATCH /experiments/{id} — status=completed, end_timestamp.",
    )

    # 4. Archive summaries
    archive_task = PythonOperator(
        task_id="archive_experiment_artifacts",
        python_callable=persist_final_summary,
        doc_md="POST /experiments/{id}/summary — KPIs for fast UI rendering.",
    )

---

## 6. `data_quality`

**Status.** Planned.

**Purpose.** Gate raw sensor data before it reaches the models; quarantine
experiments whose recent window violates critical checks.

**Trigger.** 10-minute schedule.

### Tasks

    # 1. Fetch recent raw window
    fetch_window_task = PythonOperator(
        task_id="fetch_recent_raw_window",
        python_callable=get_recent_raw_data,
        doc_md="GET /raw/window?minutes=N — backend-proxied Flux query.",
    )

    # 2. Run quality checks
    checks_task = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=validate_sensor_window,
        doc_md="Range, stuck-sensor, sampling-rate and clock-skew rules.",
    )

    # 3. Report issues
    report_issues_task = PythonOperator(
        task_id="report_data_quality_issues",
        python_callable=post_quality_issues,
        doc_md="POST /data-quality/issues — one row per violation.",
    )

    # 4. Quarantine unhealthy experiments
    quarantine_task = PythonOperator(
        task_id="quarantine_unhealthy_experiments",
        python_callable=flag_experiments_for_quarantine,
        doc_md="PATCH /experiments/{id} — status=quarantined on critical failure.",
    )

---

## DAG dependency graph

    experiment_lifecycle  ──► stamm_predictions  ──► experiment_close
                                    │
                                    ▼
                            data_quality (gate)
                                    │
                                    ▼
                             drift_detection  ──►  retraining_trigger

All planned DAGs share a single abstraction: `experiment_id`. That is what
makes the pipeline reproducible end-to-end.