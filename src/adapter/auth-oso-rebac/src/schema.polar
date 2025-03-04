actor UserActor {
    # Fields:
    # - account_id: String
    # - anonymous: bool
    # - is_admin: bool
}

resource DatasetResource {
    # Fields:
    # - owner_account_id: String
    # - allows_public_read: bool
    # - authorized_users: Dictionary<account_id: String, role: String>

    permissions = ["read", "write"];
}

has_permission(actor: UserActor, "read", dataset: DatasetResource) if
    actor.is_admin or
    dataset.allows_public_read or
    dataset.owner_account_id == actor.account_id or (
        actor_account_id = actor.account_id and
        dataset.authorized_users.(actor_account_id) in ["Reader", "Editor"]
    );

has_permission(actor: UserActor, "write", dataset: DatasetResource) if
    actor.is_admin or
    dataset.owner_account_id == actor.account_id or (
        actor_account_id = actor.account_id and
        dataset.authorized_users.(actor_account_id) == "Editor"
    );

allow(actor: UserActor, action: String, dataset: DatasetResource) if
    has_permission(actor, action, dataset);
