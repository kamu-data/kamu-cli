########################################################################################################################
# Resources
########################################################################################################################

actor UserActor {
    # ┌─────────────────────────┬────────┐
    # │        Field            │  Type  │
    # ├─────────────────────────┼────────┤
    # │ account_id              │ String │
    # │ anonymous (reserved)    │ bool   │
    # │ is_admin                │ bool   │
    # │ can_provision_accounts  │ bool   │
    # └─────────────────────────┴────────┘
}

resource DatasetResource {
    # ┌───────────────────────┬───────────────────────────────────────────────┐
    # │         Field         │                      Type                     │
    # ├───────────────────────┼───────────────────────────────────────────────┤
    # │ owner_account_id      │ String                                        │
    # │ allows_public_read    │ bool                                          │
    # │ allows_anonymous_read │ bool                                          │
    # │ authorized_users      │ Dictionary<                                   │
    # │                       │   authorized_account_id: String,              │
    # │                       │   role: ["Reader", "Editor", "Maintainer"]    │
    # │                       │ >                                             │
    # └───────────────────────┴───────────────────────────────────────────────┘

    permissions = ["read", "write", "maintain", "own"];
}

########################################################################################################################
# Rules
########################################################################################################################

allow(user: UserActor, action: String, dataset: DatasetResource) if
    has_permission(user, action, dataset);

########################################################################################################################

has_permission(user: UserActor, "read", dataset: DatasetResource) if
    dataset_readable_by_public(user, dataset) or
    user.is_admin or
    user_owns_dataset(user, dataset) or
    user_authorized_for_dataset(user, dataset, ["Reader", "Editor", "Maintainer"]);

has_permission(user: UserActor, "write", dataset: DatasetResource) if
    user.is_admin or
    user_owns_dataset(user, dataset) or
    user_authorized_for_dataset(user, dataset, ["Editor", "Maintainer"]);

has_permission(user: UserActor, "maintain", dataset: DatasetResource) if
    user.is_admin or
    user_owns_dataset(user, dataset) or
    user_authorized_for_dataset(user, dataset, ["Maintainer"]);

has_permission(user: UserActor, "own", dataset: DatasetResource) if
    user.is_admin or
    user_owns_dataset(user, dataset);

########################################################################################################################
# Helpers
########################################################################################################################

dataset_readable_by_public(user: UserActor, dataset: DatasetResource) if
    dataset.allows_public_read and
    (not user.anonymous or dataset.allows_anonymous_read);

user_owns_dataset(user: UserActor, dataset: DatasetResource) if
    dataset.owner_account_id == user.account_id;

user_authorized_for_dataset(user: UserActor, dataset: DatasetResource, allowed_roles: List) if
    user_id = user.account_id and
    dataset.authorized_users.(user_id) in allowed_roles;

########################################################################################################################
