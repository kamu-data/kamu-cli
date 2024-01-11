actor UserActor {}

resource DatasetResource {
    permissions = ["read", "write"];
}

has_permission(actor: UserActor, "read", dataset: DatasetResource) if
    actor.is_admin or
    dataset.allows_public_read or
    dataset.created_by == actor.name or (
        actor_name = actor.name and
        dataset.authorized_users.(actor_name) in ["Reader", "Editor"]
    );

has_permission(actor: UserActor, "write", dataset: DatasetResource) if
    actor.is_admin or
    dataset.created_by == actor.name or (
        actor_name = actor.name and
        dataset.authorized_users.(actor_name) == "Editor"
    );

allow(actor: UserActor, action: String, dataset: DatasetResource) if
    has_permission(actor, action, dataset);
