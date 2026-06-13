-- Track the provider-side request id (OpenAI x-request-id, Anthropic
-- request-id) on each run so the UI can surface it for log correlation.
alter table runs
  add column if not exists provider_request_id text;
