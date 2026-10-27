
CREATE INDEX IF NOT EXISTS idx_stream_snapshots_collected_at
    ON public.stream_snapshots (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_stream_snapshots_game_name
    ON public.stream_snapshots (game_name);

