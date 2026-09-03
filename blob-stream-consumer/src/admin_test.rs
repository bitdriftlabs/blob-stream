use super::ArmFreshStartRequest;

#[test]
fn dry_run_defaults_loss_confirmation_to_false() {
  let request: ArmFreshStartRequest =
    serde_json::from_str(r#"{"dry_run":true,"virtual_partition_ids":[126]}"#)
      .expect("documented dry-run request must deserialize");

  assert!(request.dry_run);
  assert!(!request.confirm_loss);
  assert_eq!(request.virtual_partition_ids, [126]);
}
