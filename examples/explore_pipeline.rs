//! Hands-on tour of Conductor's define / plan API.
//!
//! Domain: video-on-demand packaging — ingest a master file, encode renditions
//! in parallel, package HLS, publish, then control-ordered CDN/catalog work.
//!
//! ```bash
//! cargo run --example explore_pipeline
//! ```

#![allow(clippy::print_stdout)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use std::collections::HashMap;

use ascii_dag::Graph;
use conductor::{Artifact, EdgeKind, Pipeline, Task, TaskGraph, TaskState};

fn main() {
    println!("=== Conductor explore_pipeline (VOD packaging) ===\n");

    // Artifacts
    let master = Artifact::new("s3/raw/episode-42/master.mov");
    let mezzanine = Artifact::new("s3/mezzanine/episode-42.mov");
    let r1080 = Artifact::new("s3/renditions/episode-42/1080p.mp4");
    let r720 = Artifact::new("s3/renditions/episode-42/720p.mp4");
    let audio = Artifact::new("s3/renditions/episode-42/audio.m4a");
    let hls = Artifact::new("s3/packages/episode-42/hls/");
    let origin = Artifact::new("cdn/origin/vod/episode-42/");

    // Ingest + validate (linear data)
    let ingest_master = Task::new("ingest_master").with_outputs([master.clone()]);
    let validate_source = Task::new("validate_source")
        .with_inputs([master])
        .with_outputs([mezzanine.clone()]);

    // Parallel encodes from the same mezzanine
    let encode_1080p = Task::new("encode_1080p")
        .with_inputs([mezzanine.clone()])
        .with_outputs([r1080.clone()]);
    let encode_720p = Task::new("encode_720p")
        .with_inputs([mezzanine.clone()])
        .with_outputs([r720.clone()]);
    let extract_audio = Task::new("extract_audio")
        .with_inputs([mezzanine])
        .with_outputs([audio.clone()]);

    // Fan-in: package needs all three renditions
    let package_hls = Task::new("package_hls")
        .with_inputs([r1080, r720, audio])
        .with_outputs([hls.clone()]);

    let upload_origin = Task::new("upload_origin")
        .with_inputs([hls])
        .with_outputs([origin]);

    // Control-only fan-out after publish (no new catalog artifacts)
    let purge_cdn = Task::new("purge_cdn").with_after([&upload_origin]);
    let update_catalog = Task::new("update_catalog").with_after([&upload_origin]);

    // Smoke test waits on both control branches
    let smoke_test_playback =
        Task::new("smoke_test_playback").with_after([&purge_cdn, &update_catalog]);

    let pipeline = Pipeline::new(
        "vod_episode_42",
        [
            ingest_master,
            validate_source,
            encode_1080p,
            encode_720p,
            extract_audio,
            package_hls,
            upload_origin,
            purge_cdn,
            update_catalog,
            smoke_test_playback,
        ],
    );

    println!("Pipeline: {}", pipeline.name());
    println!("Tasks (definition order):\n");
    for task in pipeline.tasks() {
        println!("  • {}", task.name());
        print_artifacts("inputs ", task.inputs());
        print_artifacts("outputs", task.outputs());
        if !task.after().is_empty() {
            let after: Vec<&str> = task
                .after()
                .iter()
                .map(conductor::TaskName::as_str)
                .collect();
            println!("      after:   {}", after.join(", "));
        }
    }

    let graph = pipeline.plan().expect("pipeline should plan");

    println!("\nTask graph (topological order):");
    for (i, task) in graph.topological_order().iter().enumerate() {
        println!("  {}. {}", i + 1, task.name());
    }

    println!("\nEdges:");
    for edge in graph.edges() {
        let kind = match edge.kind() {
            EdgeKind::Data { artifact } => format!("data({})", artifact.slug()),
            EdgeKind::Control => "control".to_owned(),
        };
        println!(
            "  {} -> {}  [{kind}]",
            graph.edge_from(edge).name(),
            graph.edge_to(edge).name()
        );
    }

    print_ascii_dag(&graph);

    let run = pipeline.run("vod-manual-001");
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

/// Render a laid-out ASCII DAG with `ascii-dag` (Sugiyama layout).
fn print_ascii_dag(graph: &TaskGraph) {
    let nodes: Vec<(usize, &str)> = graph
        .tasks()
        .iter()
        .enumerate()
        .map(|(id, task)| (id, task.name().as_str()))
        .collect();

    let name_to_id: HashMap<&str, usize> =
        nodes.iter().copied().map(|(id, name)| (name, id)).collect();

    // Own edge labels so we can borrow them into ascii-dag's Graph<'a>.
    let mut label_storage: Vec<String> = Vec::new();
    let mut edge_specs: Vec<(usize, usize, Option<usize>)> = Vec::new();

    for edge in graph.edges() {
        let from = name_to_id[graph.edge_from(edge).name().as_str()];
        let to = name_to_id[graph.edge_to(edge).name().as_str()];
        let label_idx = match edge.kind() {
            EdgeKind::Data { artifact } => {
                let idx = label_storage.len();
                label_storage.push(format!("data:{}", artifact.slug()));
                Some(idx)
            }
            EdgeKind::Control => {
                let idx = label_storage.len();
                label_storage.push("control".to_owned());
                Some(idx)
            }
        };
        edge_specs.push((from, to, label_idx));
    }

    let edges: Vec<(usize, usize, Option<&str>)> = edge_specs
        .iter()
        .map(|&(from, to, label_idx)| (from, to, label_idx.map(|idx| label_storage[idx].as_str())))
        .collect();

    let dag = Graph::from_edges_labeled(&nodes, &edges);

    println!("\nTask graph (ascii-dag):\n");
    println!("{}", dag.render());
}
