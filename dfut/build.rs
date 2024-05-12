fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/global_scheduler_service.proto")?;
    tonic_build::compile_protos("proto/d_store_service.proto")?;
    tonic_build::compile_protos("proto/worker_service.proto")?;
    Ok(())
}
