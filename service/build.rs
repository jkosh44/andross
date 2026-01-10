fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(&["proto/kv/v1/kv.proto"], &["proto"])?;
    Ok(())
}
