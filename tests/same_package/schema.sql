CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TYPE analysis_state AS ENUM ('pending', 'complete');

CREATE TABLE analyses (
    id BIGINT PRIMARY KEY,
    tenant_id BIGINT NOT NULL,
    summary TEXT,
    state analysis_state,
    source INET NOT NULL,
    active_window TSTZRANGE NOT NULL
);

CREATE DOMAIN public.xid AS BIGINT;

CREATE TABLE message (
    id public.xid PRIMARY KEY,
    user_id BIGINT NOT NULL,
    to_user_or_group_id BIGINT NOT NULL,
    in_group BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    deleted_at TIMESTAMPTZ
);

CREATE TABLE message_inbox (
    id BIGINT PRIMARY KEY,
    to_user_or_group_id BIGINT
);
