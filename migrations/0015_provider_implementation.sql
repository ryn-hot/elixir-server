ALTER TABLE providers ADD COLUMN implementation TEXT;
CREATE INDEX IF NOT EXISTS idx_providers_implementation ON providers(implementation);
