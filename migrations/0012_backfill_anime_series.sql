-- Backfill anime series classification based on AniList-linked seasons/series.

UPDATE series
SET library_type = 'anime', updated_at = CURRENT_TIMESTAMP
WHERE library_type != 'anime'
  AND (
    external_anilist IS NOT NULL AND external_anilist != ''
    OR EXISTS (
        SELECT 1
        FROM seasons
        WHERE seasons.series_id = series.id
          AND seasons.external_anilist IS NOT NULL
          AND seasons.external_anilist != ''
    )
    OR EXISTS (
        SELECT 1
        FROM season_external_ids se
        JOIN seasons s ON s.id = se.season_id
        WHERE s.series_id = series.id
          AND se.provider = 'anilist'
    )
  );

UPDATE media_items
SET type = 'anime', updated_at = CURRENT_TIMESTAMP
WHERE id IN (SELECT id FROM series WHERE library_type = 'anime');
