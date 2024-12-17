alter table dataset_env_vars
add constraint dataset_env_var_dataset_entry
   foreign key (dataset_id)
   references dataset_entries(dataset_id)
   on delete cascade;