# `drift_detection`

[← back to DAG index](../dags.md)

**Status.** Planned. Wraps the existing standalone drift-detection package.

**Purpose.** Quantify divergence between predictions and laboratory ground
truth over recent completed experiments; raise alerts and optionally seed
retraining jobs when thresholds are breached.

**Trigger.** Daily scheduled run; additional on-demand trigger exposed
through the UI.

## Tasks

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
