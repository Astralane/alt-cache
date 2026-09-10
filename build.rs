fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = tonic_prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.extern_path(".geyser", "::yellowstone_grpc_proto::geyser");
    tonic_prost_build::configure().compile_with_config(config, &["proto/alt.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/alt.proto");
    println!("cargo:rerun-if-changed=proto/geyser.proto");
    Ok(())
}
