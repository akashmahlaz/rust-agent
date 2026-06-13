create extension if not exists pgcrypto;

create table if not exists users (
  id uuid primary key,
  email text not null unique,
  display_name text,
  password_hash text,
  email_verified_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create table if not exists auth_sessions (
  id uuid primary key,
  user_id uuid not null references users(id) on delete cascade,
  refresh_token_hash text not null unique,
  user_agent text,
  ip_address inet,
  expires_at timestamptz not null,
  revoked_at timestamptz,
  created_at timestamptz not null default now()
);

create index if not exists auth_sessions_user_id_idx on auth_sessions(user_id);
create index if not exists auth_sessions_expires_at_idx on auth_sessions(expires_at);

create table if not exists oauth_accounts (
  id uuid primary key,
  user_id uuid not null references users(id) on delete cascade,
  provider text not null,
  provider_account_id text not null,
  access_token_ciphertext text,
  refresh_token_ciphertext text,
  expires_at timestamptz,
  scopes text[] not null default '{}',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (provider, provider_account_id)
);

create table if not exists provider_profiles (
  id uuid primary key,
  user_id uuid not null references users(id) on delete cascade,
  provider text not null,
  api_key_ciphertext text,
  models jsonb not null default '[]'::jsonb,
  default_model text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (user_id, provider)
);

create table if not exists conversations (
  id uuid primary key,
  user_id uuid not null references users(id) on delete cascade,
  title text not null,
  channel text not null default 'app',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists conversations_user_id_updated_at_idx on conversations(user_id, updated_at desc);

create table if not exists messages (
  id uuid primary key,
  conversation_id uuid not null references conversations(id) on delete cascade,
  user_id uuid references users(id) on delete set null,
  role text not null check (role in ('system', 'user', 'assistant', 'tool')),
  content text not null default '',
  parts jsonb not null default '[]'::jsonb,
  model text,
  created_at timestamptz not null default now()
);

create index if not exists messages_conversation_id_created_at_idx on messages(conversation_id, created_at asc);

create table if not exists runs (
  id uuid primary key,
  conversation_id uuid not null references conversations(id) on delete cascade,
  user_id uuid not null references users(id) on delete cascade,
  status text not null check (status in ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled')),
  model text not null,
  reasoning_level text,
  started_at timestamptz,
  completed_at timestamptz,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists runs_conversation_id_created_at_idx on runs(conversation_id, created_at desc);
create index if not exists runs_status_updated_at_idx on runs(status, updated_at desc);

create table if not exists run_events (
  id uuid primary key,
  run_id uuid not null references runs(id) on delete cascade,
  sequence bigint not null,
  event_type text not null,
  payload jsonb not null,
  created_at timestamptz not null default now(),
  unique (run_id, sequence)
);

create index if not exists run_events_run_id_sequence_idx on run_events(run_id, sequence asc);

create table if not exists memories (
  id uuid primary key,
  user_id uuid not null references users(id) on delete cascade,
  scope text not null check (scope in ('user', 'workspace', 'conversation')),
  subject_id uuid,
  content text not null,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists memories_user_scope_idx on memories(user_id, scope, updated_at desc);

create table if not exists audit_logs (
  id uuid primary key,
  user_id uuid references users(id) on delete set null,
  action text not null,
  subject_type text,
  subject_id uuid,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now()
);

create index if not exists audit_logs_user_created_at_idx on audit_logs(user_id, created_at desc);
create index if not exists audit_logs_action_created_at_idx on audit_logs(action, created_at desc);
