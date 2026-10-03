-- One row per tick, suite and slice: how many probe replies landed there, and how many passed.
CREATE TABLE IF NOT EXISTS slice_stats (
    tick           int  NOT NULL,          -- the time bucket; an hour in production
    suite          text NOT NULL,
    pool           text NOT NULL,
    hardware       text NOT NULL,
    batch_bucket   text NOT NULL,
    model_version  text NOT NULL,
    prompt_version text NOT NULL,
    client_version text NOT NULL,
    total          int  NOT NULL,
    passed         int  NOT NULL,          -- replies with the right answer
    script_ok      int  NOT NULL,          -- replies with no letters from an unexpected script
    PRIMARY KEY (tick, suite, pool, hardware, batch_bucket,
                 model_version, prompt_version, client_version)
);

-- Every event the consumer has counted. A redelivered event finds its ID here and is skipped.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     uuid        PRIMARY KEY,
    processed_at timestamptz NOT NULL DEFAULT now()
);

-- Offline eval results: what the release gate and the prompt ablation compare.
CREATE TABLE IF NOT EXISTS eval_runs (
    run_id         text        NOT NULL,
    model_version  text        NOT NULL,
    prompt_version text        NOT NULL,
    suite          text        NOT NULL,
    passed         int         NOT NULL,
    total          int         NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, model_version, prompt_version, suite)
);
