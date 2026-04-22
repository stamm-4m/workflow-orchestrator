# `experiment_close`

[← back to DAG index](../dags.md)

**Status.** Planned.

**Purpose.** Transition running experiments to `completed` when raw data
stops arriving, and precompute summaries that the UI's "past experiments"
view depends on.

**Trigger.** Hourly schedule.

## Tasks

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
