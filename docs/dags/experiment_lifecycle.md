# `experiment_lifecycle`

[← back to DAG index](../dags.md)

**Status.** Planned.

**Purpose.** Detect new bioreactor batches in the raw bucket and register
them as experiments in Postgres. Downstream DAGs reference the resulting
`experiment_id` rather than re-deriving tags.

**Trigger.** 1-minute polling schedule.

## Tasks

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
