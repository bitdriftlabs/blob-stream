use crate::{ConsumerGroupMembershipStore, InMemoryConsumerGroupMembershipStore};

#[tokio::test]
async fn register_heartbeat_list_and_deregister() {
  let store = InMemoryConsumerGroupMembershipStore::new();

  store
    .register_member("topic-a", "group-a", "member-a", 1_000, 100)
    .await
    .expect("register member-a");
  store
    .register_member("topic-a", "group-a", "member-b", 1_000, 100)
    .await
    .expect("register member-b");
  store
    .register_member("topic-a", "group-b", "member-c", 1_000, 100)
    .await
    .expect("register member-c");

  let members = store
    .list_active_members("topic-a", "group-a", 1_050)
    .await
    .expect("list members");
  assert_eq!(
    members,
    vec!["member-a".to_string(), "member-b".to_string()]
  );

  store
    .heartbeat_member("topic-a", "group-a", "member-a", 1_120, 100)
    .await
    .expect("heartbeat member-a");

  let members = store
    .list_active_members("topic-a", "group-a", 1_150)
    .await
    .expect("list members");
  assert_eq!(members, vec!["member-a".to_string()]);

  store
    .deregister_member("topic-a", "group-a", "member-a")
    .await
    .expect("deregister member-a");

  let members = store
    .list_active_members("topic-a", "group-a", 1_151)
    .await
    .expect("list members");
  assert!(members.is_empty());
}

#[tokio::test]
async fn register_rejects_invalid_ttl() {
  let store = InMemoryConsumerGroupMembershipStore::new();
  let err = store
    .register_member("topic-a", "group-a", "member-a", 1_000, 0)
    .await
    .expect_err("register should reject zero ttl");
  assert!(
    err
      .to_string()
      .contains("membership ttl must be greater than zero")
  );
}
