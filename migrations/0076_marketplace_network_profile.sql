-- One optional connection profile; never a new active downloader selection.
-- Same partial-index syntax on SQLite and PostgreSQL. No existing profile is
-- reclassified, activated, deleted, or assigned new credentials by migration.
CREATE UNIQUE INDEX marketplace_network_profile_singleton
ON download_network_profiles(scope) WHERE scope='marketplace_network';
