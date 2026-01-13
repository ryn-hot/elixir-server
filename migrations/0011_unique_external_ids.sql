-- Enforce unique external ids for core entities (NULL allowed).
CREATE UNIQUE INDEX IF NOT EXISTS idx_series_external_anilist
    ON series(external_anilist);
CREATE UNIQUE INDEX IF NOT EXISTS idx_series_external_imdb
    ON series(external_imdb);
CREATE UNIQUE INDEX IF NOT EXISTS idx_movies_external_imdb
    ON movies(external_imdb);
CREATE UNIQUE INDEX IF NOT EXISTS idx_movies_external_tmdb
    ON movies(external_tmdb);
