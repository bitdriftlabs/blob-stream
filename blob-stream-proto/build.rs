use protobuf_codegen::Customize;

fn main() {
  if std::env::var("SKIP_PROTO_GEN").is_ok() {
    return;
  }

  println!("cargo:rerun-if-changed=proto/");

  std::fs::create_dir_all("src/protos/blobstream/v1").unwrap();

  protobuf_codegen::Codegen::new()
    .protoc()
    .customize(
      Customize::default()
        .gen_mod_rs(false)
        .tokio_bytes(true)
        .tokio_bytes_for_string(true)
        .oneofs_non_exhaustive(false)
        .file_header(String::new()),
    )
    .includes(["proto"])
    .inputs([
      "proto/blobstream/v1/broker.proto",
      "proto/blobstream/v1/config.proto",
    ])
    .out_dir("src/protos/blobstream/v1")
    .capture_stderr()
    .run_from_script();
}
