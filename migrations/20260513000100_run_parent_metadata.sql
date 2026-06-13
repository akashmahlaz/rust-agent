alter table runs
  add column if not exists parent_run_id uuid references runs(id) on delete set null,
  add column if not exists parent_request_id text,
  add column if not exists parent_tool_call_id text,
  add column if not exists metadata jsonb not null default '{}'::jsonb;

create index if not exists runs_parent_run_id_created_at_idx
  on runs(parent_run_id, created_at desc)
  where parent_run_id is not null;
