-- Enforce unique TVDB series ids (NULL allowed).
CREATE UNIQUE INDEX IF NOT EXISTS idx_series_external_tvdb_series
    ON series(external_tvdb_series);
