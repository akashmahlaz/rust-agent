-- Completes the Next.js MongoDB removal by adding tables that only the
-- Next service layer previously created dynamically in MongoDB.

CREATE TABLE IF NOT EXISTS agents (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id       uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name          text        NOT NULL,
    description   text        NOT NULL DEFAULT '',
    system_prompt text        NOT NULL DEFAULT '',
    tools         text[]      NOT NULL DEFAULT '{}',
    enabled       boolean     NOT NULL DEFAULT true,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS agents_user_id_created_at_idx ON agents(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS agents_user_id_enabled_idx ON agents(user_id, enabled);

CREATE TABLE IF NOT EXISTS workspace_files (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    kind        text        NOT NULL CHECK (kind IN ('bootstrap', 'soul', 'user')),
    content     text        NOT NULL DEFAULT '',
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE(user_id, kind)
);
CREATE INDEX IF NOT EXISTS workspace_files_user_id_updated_at_idx ON workspace_files(user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS logs (
    id          uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     uuid        REFERENCES users(id) ON DELETE SET NULL,
    level       text        NOT NULL CHECK (level IN ('info', 'warn', 'error', 'debug')),
    source      text        NOT NULL,
    message     text        NOT NULL,
    metadata    jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS logs_created_at_idx ON logs(created_at DESC);
CREATE INDEX IF NOT EXISTS logs_user_id_created_at_idx ON logs(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS logs_source_created_at_idx ON logs(source, created_at DESC);

CREATE TABLE IF NOT EXISTS pending_confirmations (
    token       text        PRIMARY KEY,
    user_id     uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tool        text        NOT NULL,
    args        jsonb       NOT NULL DEFAULT '{}'::jsonb,
    summary     text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    expires_at  timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS pending_confirmations_user_id_idx ON pending_confirmations(user_id);
CREATE INDEX IF NOT EXISTS pending_confirmations_expires_at_idx ON pending_confirmations(expires_at);

CREATE UNIQUE INDEX IF NOT EXISTS skills_user_slug_unique_idx
    ON skills(user_id, (payload->>'slug'))
    WHERE payload ? 'slug';

CREATE UNIQUE INDEX IF NOT EXISTS agent_skills_user_name_unique_idx ON agent_skills(user_id, name);
CREATE INDEX IF NOT EXISTS agent_skills_user_updated_at_idx ON agent_skills(user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS agent_skills_user_invocation_idx ON agent_skills(user_id, invocation_count DESC, updated_at DESC);

-- Per-user memory fact extensions (replaces the MongoDB schema previously used by lib/memory.ts).
ALTER TABLE memories
    ADD COLUMN IF NOT EXISTS kind                text,
    ADD COLUMN IF NOT EXISTS importance          int,
    ADD COLUMN IF NOT EXISTS source              text,
    ADD COLUMN IF NOT EXISTS subject_type        text,
    ADD COLUMN IF NOT EXISTS normalized_content  text,
    ADD COLUMN IF NOT EXISTS keywords            text[],
    ADD COLUMN IF NOT EXISTS token_count         int,
    ADD COLUMN IF NOT EXISTS last_used_at        timestamptz;

CREATE UNIQUE INDEX IF NOT EXISTS memories_user_normalized_unique_idx
    ON memories(user_id, normalized_content)
    WHERE normalized_content IS NOT NULL;

CREATE INDEX IF NOT EXISTS memories_user_importance_updated_idx
    ON memories(user_id, importance DESC NULLS LAST, updated_at DESC);

CREATE INDEX IF NOT EXISTS memories_user_subject_idx
    ON memories(user_id, subject_type, subject_id);

CREATE INDEX IF NOT EXISTS memories_keywords_gin_idx
    ON memories USING GIN(keywords);

-- Multi-profile-per-provider support for the Next.js auth_profiles consumer
-- (one row per provider+type — e.g. openai/api_key vs github/oauth).
ALTER TABLE auth_profiles
    ADD COLUMN IF NOT EXISTS type      text NOT NULL DEFAULT 'api_key',
    ADD COLUMN IF NOT EXISTS token_ref text,
    ADD COLUMN IF NOT EXISTS base_url  text,
    ADD COLUMN IF NOT EXISTS metadata  jsonb NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE auth_profiles
    DROP CONSTRAINT IF EXISTS auth_profiles_user_id_provider_key;

CREATE UNIQUE INDEX IF NOT EXISTS auth_profiles_user_provider_type_unique_idx
    ON auth_profiles(user_id, provider, type);
CREATE INDEX IF NOT EXISTS auth_profiles_user_updated_at_idx
    ON auth_profiles(user_id, updated_at DESC);

-- coding_plans is one row per conversation; enforce that for the ON CONFLICT upserts.
CREATE UNIQUE INDEX IF NOT EXISTS coding_plans_conversation_id_unique_idx
    ON coding_plans(conversation_id);
