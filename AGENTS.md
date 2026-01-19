# Agent Guidelines

## Build/Lint/Test Commands
- Build: `cargo build --workspace`
- Lint: `cargo clippy --workspace --bins --examples --tests -- --no-deps`
- Format: `cargo +nightly fmt`
- Test (all): `cargo nextest run`
- Test (single): `cargo nextest run test_name`
- Test (specific crate): `cargo nextest run -p crate-name`

## Code Style Guidelines
- Use 2-space indentation (no tabs)
- Max line width: 100 characters
- Error handling: Use `anyhow` for general errors, `thiserror` for custom error types
- Use `#[cfg(test)]` and separate test files with `_test.rs` suffix
- Imports: Group imports with `One` style, module granularity, and `HorizontalVertical` layout
- Use workspace dependencies from Cargo.toml in child crates
- Edition: Rust 2024
- Make sure to run `cargo +nightly fmt` after making changes to apply default formatting rules.
- Use pattern matching with if-let and match expressions for error handling
- When you write comments, flow them out to 100 columns for wrapping
- Add separator comments above each struct to distinguish struct blocks and their impls
- Add succinct comments for trait methods to document intent without restating signatures

## Documentation Guidelines
- Avoid redundant documentation for the sake of convention. For example
    - Don't include an Errors section if the only errors are generic failures.
    - Don't include an Arguments section if the arguments are obvious based on the function signature.

## Test File Conventions
1. Test files should be placed adjacent to the implementation file they're testing
2. Test files should be named with a `_test.rs` suffix (e.g., `network_quality_test.rs`)
3. Link test files in the implementation file using the following pattern at the top of the file, right below the license header and optional module-level docs.
   ```rust
   #[cfg(test)]
   #[path = "./file_name_test.rs"]
   mod tests;
   ```
4. Tests in the same file as the implementation code should be avoided
5. Test names should *not* start with `test_`, as this is redundant

## Code Quality Checks
- After changing code, always run tests, clippy, and format (per above instructions) in
  impacted crates per instructions.
