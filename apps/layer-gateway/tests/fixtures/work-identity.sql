CREATE TABLE work_identity_workspace (
    id uuid PRIMARY KEY,
    label text NOT NULL CHECK (label ~ '^[a-z][a-z0-9]{0,15}$'),
    next_ordinal bigint NOT NULL DEFAULT 1,
    next_change bigint NOT NULL DEFAULT 1
);

CREATE TABLE work_identity_ask (
    workspace uuid NOT NULL REFERENCES work_identity_workspace(id),
    id uuid NOT NULL,
    ordinal bigint NOT NULL,
    body jsonb NOT NULL,
    PRIMARY KEY (workspace, id),
    UNIQUE (workspace, ordinal)
);

CREATE TABLE work_identity_address (
    workspace uuid NOT NULL,
    address text NOT NULL,
    id uuid NOT NULL,
    PRIMARY KEY (workspace, address),
    FOREIGN KEY (workspace, id) REFERENCES work_identity_ask(workspace, id)
);

CREATE TABLE work_identity_create (
    workspace uuid NOT NULL,
    key uuid NOT NULL,
    payload bytea NOT NULL,
    id uuid NOT NULL,
    PRIMARY KEY (workspace, key),
    FOREIGN KEY (workspace, id) REFERENCES work_identity_ask(workspace, id)
);

CREATE TABLE work_identity_change (
    workspace uuid NOT NULL,
    ordinal bigint NOT NULL,
    id uuid NOT NULL,
    event text NOT NULL,
    body jsonb NOT NULL,
    PRIMARY KEY (workspace, ordinal),
    FOREIGN KEY (workspace, id) REFERENCES work_identity_ask(workspace, id)
);
