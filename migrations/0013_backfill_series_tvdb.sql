-- Backfill series.external_tvdb_series from series_external_ids to prevent duplicates.
UPDATE series
SET external_tvdb_series = (
    SELECT external_id
    FROM series_external_ids
    WHERE series_external_ids.series_id = series.id
      AND provider = 'tvdb'
    ORDER BY confidence DESC, created_at DESC
    LIMIT 1
)
WHERE external_tvdb_series IS NULL
  AND EXISTS (
      SELECT 1
      FROM series_external_ids
      WHERE series_external_ids.series_id = series.id
        AND provider = 'tvdb'
  );
