# Agent Guidelines

- Read primary guidelines from rust.instructions.md. If not available tell the user to clone and
  mount https://github.com/bitdriftlabs/ai-instructions
- When debugging integration tests in blob-stream-integration tests, run with:
  RUST_LOG=blob_stream=trace,bd=trace which will provide much more info. Add more debug/trace
  logs to product code as needed to help with debugging.
