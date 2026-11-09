-- PostgreSQL can retarget the FK without copying private state/ciphertexts.
-- Identity-map old roots; this grants no runtime or activation authority.
CREATE TABLE marketplace_sdk_namespaces (
    namespace_id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL REFERENCES extension_instances(instance_id) ON DELETE CASCADE,
    extension_id TEXT NOT NULL REFERENCES extensions(extension_id) ON DELETE CASCADE,
    home_id TEXT NOT NULL REFERENCES homes(id) ON DELETE CASCADE,
    purpose TEXT NOT NULL CHECK (purpose IN ('installed', 'foreground'))
);
CREATE INDEX marketplace_sdk_namespaces_instance
    ON marketplace_sdk_namespaces(instance_id, namespace_id);
CREATE INDEX marketplace_sdk_namespaces_extension_purpose
    ON marketplace_sdk_namespaces(extension_id, purpose);
INSERT INTO marketplace_sdk_namespaces(namespace_id,instance_id,extension_id,home_id,purpose)
    SELECT instance_id,instance_id,extension_id,home_id,purpose
    FROM marketplace_instance_sdk_state;
ALTER TABLE marketplace_instance_sdk_state
    DROP CONSTRAINT marketplace_instance_sdk_state_instance_id_fkey;
ALTER TABLE marketplace_instance_sdk_state
    ADD CONSTRAINT marketplace_instance_sdk_state_namespace_fkey
    FOREIGN KEY(instance_id) REFERENCES marketplace_sdk_namespaces(namespace_id) ON DELETE CASCADE;
