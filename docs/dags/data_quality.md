# `data_quality`

[← back to DAG index](../dags.md)

**Status.** Planned.

**Purpose.** Gate raw sensor data before it reaches the models; quarantine
experiments whose recent window violates critical checks.

**Trigger.** 10-minute schedule.

## Tasks

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
