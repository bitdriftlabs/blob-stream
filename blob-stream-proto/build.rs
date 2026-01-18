// blob-stream - broker + client APIs
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code is governed by a source available license that can be found in the
// LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt

use protobuf_codegen::Customize;

const GENERATED_HEADER: &str = r"// proto - blob-stream API definitions
// Copyright Bitdrift, Inc. All rights reserved.
//
// Use of this source code and APIs are governed by a source available license that can be found in
// the LICENSE file or at:
// https://polyformproject.org/wp-content/uploads/2020/06/PolyForm-Shield-1.0.0.txt
";

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
        .file_header(GENERATED_HEADER.to_string()),
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
