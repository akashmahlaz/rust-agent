-- Personalization: user profiles and enhanced memory system.
-- Stores learned preferences, patterns, and domain knowledge for each user.

-- ── user_profiles ──────────────────────────────────────────────────────────
-- OpenClaw-style user profile (equivalent to USER.md + IDENTITY.md in DB form).
-- Built up progressively through usage and bootstrap onboarding.
CREATE TABLE IF NOT EXISTS user_profiles (
    user_id         uuid        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    -- Basic info (filled during bootstrap)
    display_name    text,
    role            text,           -- e.g. "AdOps Manager", "Publisher", "Fraud Analyst"
    organization    text,           -- e.g. "Matterfull"
    timezone        text,
    language        text            DEFAULT 'en',
    -- Communication preferences
    communication_style text,       -- e.g. "direct", "detailed", "brief"
    expertise_level text            DEFAULT 'intermediate', -- beginner/intermediate/expert
    -- Domain knowledge (auto-filled from conversations)
    known_domains   text[]          DEFAULT '{}',   -- domains they investigate often
    known_exchanges text[]          DEFAULT '{}',   -- exchanges they work with
    known_publishers text[]         DEFAULT '{}',  -- publishers in their portfolio
    -- Agent behavior preferences (learned over time)
    preferred_tools text[]          DEFAULT '{}',   -- tools they use most
    preferred_checks text[]         DEFAULT '{}',   -- what they usually check first
    -- Onboarding state
    bootstrap_completed boolean     DEFAULT false,
    bootstrap_completed_at timestamptz,
    -- Personalization depth (increases with usage)
    interaction_count integer       DEFAULT 0,
    last_interaction_at timestamptz,
    -- Raw profile data (flexible, for future fields)
    profile_data    jsonb           DEFAULT '{}'::jsonb,
    created_at      timestamptz     NOT NULL DEFAULT now(),
    updated_at      timestamptz     NOT NULL DEFAULT now()
);

-- ── memory categories ──────────────────────────────────────────────────────
-- Add a category column to memories for better organization.
ALTER TABLE memories
    ADD COLUMN IF NOT EXISTS category text DEFAULT 'general';

-- Categories: general, preference, pattern, domain_knowledge, investigation,
--             feedback, insight, tool_usage

-- ── investigation_patterns ─────────────────────────────────────────────────
-- Track repeated investigation patterns so the agent can suggest shortcuts.
CREATE TABLE IF NOT EXISTS investigation_patterns (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    pattern_type    text        NOT NULL,   -- 'domain_check', 'fraud_scan', 'supply_chain_audit'
    domain          text,
    parameters      jsonb       DEFAULT '{}'::jsonb,  -- what tools/params they typically use
    frequency       integer     DEFAULT 1,
    last_used_at    timestamptz DEFAULT now(),
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS investigation_patterns_user_idx
    ON investigation_patterns(user_id, frequency DESC);

-- ── conversation_insights ──────────────────────────────────────────────────
-- Auto-extracted insights from completed conversations.
-- The agent reviews each conversation and stores learnings here.
CREATE TABLE IF NOT EXISTS conversation_insights (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id         uuid        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conversation_id uuid        NOT NULL,
    insight_type    text        NOT NULL,   -- 'preference', 'correction', 'domain_fact', 'workflow'
    content         text        NOT NULL,
    confidence      real        DEFAULT 0.8,
    applied         boolean     DEFAULT false,
    created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS conversation_insights_user_idx
    ON conversation_insights(user_id, created_at DESC);
