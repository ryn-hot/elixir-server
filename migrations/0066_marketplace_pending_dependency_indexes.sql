-- Pending-install cancellation must check for owned media without scanning a
-- large library. Also supports normal instance-scoped ownership lookups.
CREATE INDEX IF NOT EXISTS idx_media_ownerships_instance
ON media_ownerships(owner_instance_id)
WHERE owner_instance_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_runtime_logs_instance
ON runtime_logs(instance_id);
