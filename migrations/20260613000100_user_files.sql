-- Phase 3: Persistent user file library
-- Every file (user-uploaded OR AI-generated) gets tracked here.
-- Replaces the per-run workspace-only model with a permanent library.

CREATE TABLE IF NOT EXISTS user_files (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conversation_id uuid        REFERENCES conversations(id) ON DELETE SET NULL,
    name            text        NOT NULL,
    storage_type    text        NOT NULL CHECK (storage_type IN ('s3', 'local')),
    storage_key     text        NOT NULL,
    url             text        NOT NULL,
    mime_type       text,
    size_bytes      bigint,
    source          text        NOT NULL DEFAULT 'upload' CHECK (source IN ('upload', 'ai_generated', 'system')),
    metadata        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_files_user_id_idx ON user_files(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS user_files_conversation_id_idx ON user_files(conversation_id, created_at DESC);
CREATE INDEX IF NOT EXISTS user_files_source_idx ON user_files(user_id, source);
