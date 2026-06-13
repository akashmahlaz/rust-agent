-- File storage for conversations
create table if not exists conversation_files (
    id uuid primary key,
    conversation_id uuid not null references conversations(id) on delete cascade,
    user_id uuid not null references users(id) on delete cascade,
    original_filename text not null,
    storage_key text not null,
    storage_type text not null check (storage_type in ('s3', 'local')),
    content_type text,
    size_bytes bigint not null,
    url text not null,
    created_at timestamptz not null default now()
);

create index if not exists conversation_files_conversation_id_idx on conversation_files(conversation_id);
create index if not exists conversation_files_user_id_idx on conversation_files(user_id);

-- User preferences and learned behavior
create table if not exists user_preferences (
    id uuid primary key,
    user_id uuid not null references users(id) on delete cascade,
    key text not null,
    value jsonb not null default '{}',
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (user_id, key)
);

create index if not exists user_preferences_user_id_idx on user_preferences(user_id);

-- Project context (remembers project structure, key files, etc.)
create table if not exists project_context (
    id uuid primary key,
    user_id uuid not null references users(id) on delete cascade,
    project_path text not null,
    project_name text,
    description text,
    key_files jsonb not null default '[]',
    readme_content text,
    last_accessed_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    unique (user_id, project_path)
);

create index if not exists project_context_user_id_idx on project_context(user_id);