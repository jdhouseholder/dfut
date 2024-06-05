use std::io;
use std::path::Path;

use tonic_build;

fn compile_global_scheduler_proto() -> io::Result<()> {
    let proto = "proto/global_scheduler_service.proto";

    let proto_path: &Path = proto.as_ref();

    let proto_dir = proto_path
        .parent()
        .expect("proto file should reside in a directory");

    tonic_build::configure()
        .type_attribute(
            ".global_scheduler_service.RequestId",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".global_scheduler_service.FnStats",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".global_scheduler_service.Stats",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".global_scheduler_service.TaskFailure",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .type_attribute(
            ".global_scheduler_service.RuntimeInfo",
            "#[derive(serde::Serialize, serde::Deserialize)]",
        )
        .compile(&[proto_path], &[proto_dir])?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    compile_global_scheduler_proto()?;
    tonic_build::compile_protos("proto/d_store_service.proto")?;
    tonic_build::compile_protos("proto/worker_service.proto")?;
    Ok(())
}
