# `retraining_trigger`

[← back to DAG index](../dags.md)

**Status.** Planned.

**Purpose.** Execute retraining jobs submitted by users from the UI or
enqueued by the drift DAG; update the Model Registry with the resulting
model version.

**Trigger.** 5-minute polling; fast path when a new job is inserted.

## Tasks

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
