/* ------------------------------ */

CREATE OR REPLACE FUNCTION create_app_user_role_if_not_exists(db_name NAME, rolename NAME, role_password TEXT) RETURNS TEXT AS
$$
BEGIN
    -- Check role is not present
    IF NOT EXISTS (SELECT * FROM pg_roles WHERE rolname = rolename) THEN
        -- Create role and grant basic access
        EXECUTE format('CREATE ROLE %I WITH LOGIN PASSWORD ''%I''', rolename, role_password);
        EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', db_name, rolename);
        EXECUTE format('GRANT USAGE ON SCHEMA public TO %I', rolename);
        -- Privileges for existing objects
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %I', rolename);
        EXECUTE format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO %I', rolename);
        -- Default privileges (affects future objects)
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I', rolename);
        EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE ON SEQUENCES TO %I', rolename);
        RETURN 'CREATE ROLE';
    ELSE
        -- Role already exists
        RETURN format('ROLE ''%I'' ALREADY EXISTS', rolename);
    END IF;
END;
$$
LANGUAGE plpgsql;
