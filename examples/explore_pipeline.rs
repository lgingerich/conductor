//! Hands-on tour of Conductor's current define / seed-run API.
//!
//! ```bash
//! cargo run --example explore_pipeline
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use conductor::{Artifact, Pipeline, Task, TaskState};

fn main() {
    println!("=== Conductor explore_pipeline ===\n");
    println!("Crate: {}\n", conductor::crate_name());

    // Define artifacts
    let bq_users = Artifact::new("bigquery/analytics/users");
    let gcs_users = Artifact::new("gcs/analytics/users.parquet");
    let pg_users = Artifact::new("postgres/app/users");

    // Define tasks
    let run_sql = Task::new("run_sql").with_outputs([bq_users.clone()]);
    let bq_to_gcs = Task::new("bq_to_gcs")
        .with_inputs([bq_users])
        .with_outputs([gcs_users.clone()]);
    let gcs_to_postgres = Task::new("gcs_to_postgres")
        .with_inputs([gcs_users])
        .with_outputs([pg_users.clone()]);
    let create_indexes = Task::new("create_indexes")
        .with_inputs([pg_users.clone()])
        .with_after([&gcs_to_postgres]);
    let vacuum = Task::new("vacuum").with_after([&create_indexes]);

    // Define pipeline
    let pipeline = Pipeline::new(
        "load",
        [run_sql, bq_to_gcs, gcs_to_postgres, create_indexes, vacuum],
    );

    println!("Pipeline: {}", pipeline.name());
    println!("Tasks (definition order — not execution order):\n");
    for task in pipeline.tasks() {
        println!("  • {}", task.name());
        print_artifacts("inputs ", task.inputs());
        print_artifacts("outputs", task.outputs());
        if !task.after().is_empty() {
            println!("      after:   {}", task.after().join(", "));
        }
    }

    println!("\nCatalog artifact (example): {pg_users}");

    let run = pipeline.run("load-manual-001");

    println!("\nPipeline run: {}", run.run_id());
    println!("Seeded task runs (all still Pending):\n");
    for task_run in run.tasks() {
        let state = match task_run.state() {
            TaskState::Pending => "Pending",
            TaskState::Running { .. } => "Running",
            TaskState::Completed { .. } => "Completed",
            TaskState::Failed { .. } => "Failed",
        };
        println!(
            "  • {}  run_id={}  state={state}",
            task_run.task(),
            task_run.run_id()
        );
    }
}

fn print_artifacts(label: &str, artifacts: &[Artifact]) {
    if artifacts.is_empty() {
        return;
    }
    let slugs: Vec<&str> = artifacts.iter().map(Artifact::slug).collect();
    println!("      {label}: {}", slugs.join(", "));
}
