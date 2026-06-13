-- Migration: MongoDB → PostgreSQL consolidation
-- Adds all remaining tables that were previously stored in MongoDB,
-- plus pgvector extension for semantic memory search.
--
-- Run with: sqlx migrate run

-- Enable pgvector for memories semantic search.
-- Wrapped in a DO block so the migration succeeds even on PostgreSQL
-- installations that don't have pgvector installed (e.g. plain Postgres on
-- Windows without the pgvector package). Semantic memory search will simply
-- be unavailable; all other features work normally.
DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS vector;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'pgvector extension not available (%), skipping vector features', SQLERRM;
END;
$$;

-- ── auth_profiles ──────────────────────────────────────────────────────────
-- Encrypted API keys and OAuth tokens per provider per user.
-- (Previously stored in MongoDB `authProfiles` collection.)
CREATE TABLE IF NOT EXISTS auth_profiles (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider        text        NOT NULL,
    encrypted_api_key       text,
    encrypted_oauth_token   text,
    models          jsonb       NOT NULL DEFAULT '[]'::jsonb,
    default_model   text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE(user_id, provider)
);
CREATE INDEX IF NOT EXISTS auth_profiles_user_id_idx ON auth_profiles(user_id);

-- ── user_settings ───────────────────────────────────────────────────────────
-- Per-user preferences (persona name, communication style, language, etc.)
-- (Previously stored in MongoDB `userSettings` collection.)
CREATE TABLE IF NOT EXISTS user_settings (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id             uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE UNIQUE,
    ai_name             text,
    communication_style text,
    language_preference text,
    memory_depth        text,
    payload             jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS user_settings_user_id_idx ON user_settings(user_id);

-- ── skills ─────────────────────────────────────────────────────────────────
-- Global skill registry with per-user payload overrides.
-- (Previously stored in MongoDB `skills` collection.)
CREATE TABLE IF NOT EXISTS skills (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        text        NOT NULL,
    description text,
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS skills_user_id_idx ON skills(user_id);

-- ── agent_skills ───────────────────────────────────────────────────────────
-- User-created procedural skill recipes.
-- (Previously stored in MongoDB `agentSkills` collection.)
CREATE TABLE IF NOT EXISTS agent_skills (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name            text        NOT NULL,
    description     text,
    trigger         text,
    tags            text[],
    steps           jsonb       NOT NULL DEFAULT '[]'::jsonb,
    invocation_count int       NOT NULL DEFAULT 0,
    success_count    int       NOT NULL DEFAULT 0,
    failure_count    int       NOT NULL DEFAULT 0,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS agent_skills_user_id_idx ON agent_skills(user_id);

-- ── mcp_servers ────────────────────────────────────────────────────────────
-- MCP server configurations per user.
-- (Previously stored in MongoDB `mcpServers` collection.)
CREATE TABLE IF NOT EXISTS mcp_servers (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        text        NOT NULL,
    config      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS mcp_servers_user_id_idx ON mcp_servers(user_id);

-- ── coding_plans ───────────────────────────────────────────────────────────
-- Per-conversation coding plan (items with completion status).
-- (Previously stored in MongoDB `coding_plans` collection.)
CREATE TABLE IF NOT EXISTS coding_plans (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    conversation_id  uuid        NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    plan            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS coding_plans_conversation_id_idx ON coding_plans(conversation_id);
CREATE INDEX IF NOT EXISTS coding_plans_user_id_idx ON coding_plans(user_id);

-- ── integrations ────────────────────────────────────────────────────────────
-- Integration connection state per provider per user.
-- (Previously stored in MongoDB `integrations` collection.)
CREATE TABLE IF NOT EXISTS integrations (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider    text        NOT NULL,
    state       jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE(user_id, provider)
);
CREATE INDEX IF NOT EXISTS integrations_user_id_idx ON integrations(user_id);

-- ── jobs ───────────────────────────────────────────────────────────────────
-- Scheduled background jobs per user.
-- (Previously stored in MongoDB `jobs` collection.)
CREATE TABLE IF NOT EXISTS jobs (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind        text        NOT NULL,
    schedule    text        NOT NULL,
    payload     jsonb       NOT NULL DEFAULT '{}'::jsonb,
    last_run_at timestamptz,
    next_run_at timestamptz,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_user_id_idx ON jobs(user_id);
CREATE INDEX IF NOT EXISTS jobs_next_run_at_idx ON jobs(next_run_at);

-- ── memories (existing table) ───────────────────────────────────────────────
-- Add pgvector embedding column and stale flag for re-embedding.
-- These are also wrapped in DO blocks so a Postgres instance without pgvector
-- still runs the rest of the migration cleanly.
DO $$
BEGIN
    ALTER TABLE memories
        ADD COLUMN IF NOT EXISTS embedding vector(1536),
        ADD COLUMN IF NOT EXISTS embedding_stale boolean DEFAULT true;
EXCEPTION
    WHEN OTHERS THEN
        -- pgvector not installed — add only the non-vector stale flag
        BEGIN
            ALTER TABLE memories
                ADD COLUMN IF NOT EXISTS embedding_stale boolean DEFAULT true;
        EXCEPTION WHEN duplicate_column THEN NULL;
        END;
        RAISE NOTICE 'pgvector not available, skipping embedding column on memories';
END;
$$;

-- Create the ivfflat index only when the vector extension and column exist.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_extension WHERE extname = 'vector'
    ) AND EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'memories' AND column_name = 'embedding'
    ) THEN
        EXECUTE $idx$
            CREATE INDEX IF NOT EXISTS memories_embedding_idx
                ON memories USING ivfflat (embedding vector_cosine_ops)
        $idx$;
    END IF;
END;
$$;

-- ── uploads ────────────────────────────────────────────────────────────────
-- File upload metadata.
-- (Previously stored in MongoDB `uploads` collection.)
CREATE TABLE IF NOT EXISTS uploads (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filename        text        NOT NULL,
    content_type    text,
    size_bytes       bigint,
    storage_key     text        NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS uploads_user_id_idx ON uploads(user_id);