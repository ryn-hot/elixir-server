-- Store extension metadata for ingested media files.

ALTER TABLE media_files ADD COLUMN extension_metadata TEXT;
