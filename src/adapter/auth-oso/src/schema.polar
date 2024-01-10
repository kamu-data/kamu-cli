actor UserActor {}

resource DatasetResource {
    permissions = ["read", "write"];
}

has_permission(actor: UserActor, "read", dataset: DatasetResource) if
    dataset.allows_public_read or
    dataset.created_by == actor.name or (
        actor_name = actor.name and
        dataset.authorized_users.(actor_name) in ["Reader", "Editor"]
    );

has_permission(actor: UserActor, "write", dataset: DatasetResource) if
    dataset.created_by == actor.name or (
        actor_name = actor.name and
        dataset.authorized_users.(actor_name) == "Editor"
    );

allow(actor: UserActor, action: String, dataset: DatasetResource) if
    actor.is_admin or
    has_permission(actor, action, dataset);
