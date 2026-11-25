use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{types::Json, PgConnection, PgPool};
use uuid::Uuid;

use super::{Failure, LinkKind, Query, Write};

const MAX_ASKS: i64 = 512;
const MAX_CHANGES: i32 = 8192;
// Every new reservation records a change in the same transaction. One more
// candidate than the history budget guarantees a free slot at any prefix.
const ALLOCATION_CANDIDATES: i32 = MAX_CHANGES + 1;
const MAX_DEPTH: usize = 12;
const MAX_LINKS: usize = 32;

#[derive(Clone, Deserialize, Serialize)]
struct Ask {
    id: Uuid,
    workspace_id: Uuid,
    address: String,
    segments: Vec<i32>,
    origin_address: String,
    origin_parent_id: Option<Uuid>,
    parent_id: Option<Uuid>,
    request_ref: Uuid,
    scope_ref: Uuid,
    revision: i64,
    scope_revision: i64,
    links: Vec<Link>,
    acceptance: Option<Acceptance>,
}

#[derive(Clone, Deserialize, Serialize)]
struct Link {
    kind: LinkKind,
    reference: Uuid,
}

#[derive(Clone, Deserialize, Serialize)]
struct Acceptance {
    scope_revision: i64,
    evidence_ref: Uuid,
}

async fn get(db: &mut PgConnection, workspace: Uuid, id: Uuid) -> Result<Ask, Failure> {
    sqlx::query_scalar::<_, Json<Ask>>(
        "SELECT body FROM work_identity.work_identity_ask WHERE workspace = $1 AND id = $2",
    )
    .bind(workspace)
    .bind(id)
    .fetch_optional(db)
    .await?
    .map(|body| body.0)
    .ok_or(Failure::NotFound)
}

fn to_json(ask: Ask) -> Result<Value, Failure> {
    serde_json::to_value(ask).map_err(|_| Failure::Unavailable)
}

pub(super) async fn query(pool: &PgPool, workspace: Uuid, query: Query) -> Result<Value, Failure> {
    let mut tx = pool.begin().await?;
    configure(&mut tx).await?;
    // Give all reads for an absent workspace the same result, including lists.
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM work_identity.work_identity_workspace WHERE id = $1",
    )
    .bind(workspace)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(Failure::NotFound)?;
    let value = match query {
        Query::Get { id } => to_json(get(&mut tx, workspace, id).await?)?,
        Query::Resolve { address } => {
            if !valid_address(&address) {
                return Err(Failure::Invalid);
            }
            let body = sqlx::query_scalar::<_, Json<Ask>>(
                "SELECT ask.body FROM work_identity.work_identity_address alias JOIN work_identity.work_identity_ask ask
                   USING (workspace, id) WHERE alias.workspace = $1 AND alias.address = $2",
            )
            .bind(workspace)
            .bind(address)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(Failure::NotFound)?;
            to_json(body.0)?
        }
        Query::List { after, limit } => page(&mut tx, workspace, after, limit, false).await?,
        Query::History { after, limit } => page(&mut tx, workspace, after, limit, true).await?,
    };
    tx.commit().await?;
    Ok(value)
}

fn valid_address(address: &str) -> bool {
    let Some((label, segments)) = address.split_once('-') else {
        return false;
    };
    address.len() <= 148
        && (1..=16).contains(&label.len())
        && label.as_bytes()[0].is_ascii_lowercase()
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && segments
            .split('.')
            .take(MAX_DEPTH + 1)
            .enumerate()
            .all(|(i, s)| {
                i < MAX_DEPTH
                    && s.as_bytes()
                        .first()
                        .is_some_and(|first| (b'1'..=b'9').contains(first))
                    && s.bytes().all(|b| b.is_ascii_digit())
                    && s.parse::<i32>().is_ok()
            })
}

async fn page(
    db: &mut PgConnection,
    workspace: Uuid,
    after: Option<i64>,
    limit: Option<u32>,
    history: bool,
) -> Result<Value, Failure> {
    let after = after.unwrap_or_default();
    let limit = limit.unwrap_or(25);
    if after < 0 || !(1..=100).contains(&limit) {
        return Err(Failure::Invalid);
    }
    let statement = if history {
        "SELECT ordinal, jsonb_build_object('cursor', ordinal, 'event', event, 'ask', body)
           FROM work_identity.work_identity_change WHERE workspace = $1 AND ordinal > $2 ORDER BY ordinal LIMIT $3"
    } else {
        "SELECT ordinal, body FROM work_identity.work_identity_ask
           WHERE workspace = $1 AND ordinal > $2 ORDER BY ordinal LIMIT $3"
    };
    let mut rows = sqlx::query_as::<_, (i64, Json<Value>)>(statement)
        .bind(workspace)
        .bind(after)
        .bind(i64::from(limit) + 1)
        .fetch_all(db)
        .await?;
    let has_more = rows.len() > limit as usize;
    rows.truncate(limit as usize);
    let next_after = rows.last().map(|(ordinal, _)| *ordinal);
    let items: Vec<_> = rows.into_iter().map(|(_, body)| body.0).collect();
    Ok(json!({"items": items, "next_after": next_after, "has_more": has_more}))
}

async fn configure(db: &mut PgConnection) -> Result<(), Failure> {
    sqlx::query(
        "SELECT set_config('lock_timeout', '1s', true),
                set_config('statement_timeout', '2s', true),
                set_config('idle_in_transaction_session_timeout', '5s', true)",
    )
    .execute(db)
    .await?;
    Ok(())
}

pub(super) async fn write(pool: &PgPool, workspace: Uuid, write: Write) -> Result<Value, Failure> {
    let mut tx = pool.begin().await?;
    // A waiter sees the previous writer's commit after taking the row lock,
    // even if the provider sets a different default transaction isolation.
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
        .execute(&mut *tx)
        .await?;
    configure(&mut tx).await?;
    // The lock serializes address reservations, idempotency checks, all ask
    // writes and history cursor allocation within this workspace.
    let (label, ordinal, cursor) = sqlx::query_as::<_, (String, i64, i64)>(
        "SELECT label, next_ordinal, next_change FROM work_identity.work_identity_workspace WHERE id = $1 FOR UPDATE",
    )
    .bind(workspace)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(Failure::NotFound)?;
    let mut session = WorkspaceWrite {
        db: &mut tx,
        workspace,
        label,
        ordinal,
        cursor,
    };
    let ask = session.apply(write).await?;
    sqlx::query(
        "UPDATE work_identity.work_identity_workspace SET next_ordinal = $2, next_change = $3 WHERE id = $1",
    )
    .bind(workspace)
    .bind(session.ordinal)
    .bind(session.cursor)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    to_json(ask)
}

struct WorkspaceWrite<'a> {
    db: &'a mut PgConnection,
    workspace: Uuid,
    label: String,
    ordinal: i64,
    cursor: i64,
}

impl WorkspaceWrite<'_> {
    async fn apply(&mut self, write: Write) -> Result<Ask, Failure> {
        let (id, revision) = match &write {
            Write::Create {
                idempotency_key,
                parent_id,
                request_ref,
                scope_ref,
            } => {
                let bytes = serde_json::to_vec(&write).map_err(|_| Failure::Invalid)?;
                return self
                    .create(
                        *idempotency_key,
                        &Sha256::digest(bytes),
                        *parent_id,
                        *request_ref,
                        *scope_ref,
                    )
                    .await;
            }
            Write::Move {
                id,
                expected_revision,
                ..
            }
            | Write::Revise {
                id,
                expected_revision,
                ..
            }
            | Write::Link {
                id,
                expected_revision,
                ..
            }
            | Write::Accept {
                id,
                expected_revision,
                ..
            } => (*id, *expected_revision),
        };
        let mut ask = get(self.db, self.workspace, id).await?;
        if ask.revision != revision {
            return Err(Failure::Conflict);
        }
        let event = match write {
            Write::Move {
                parent_id, segment, ..
            } => return self.move_subtree(ask, parent_id, segment).await,
            Write::Revise { scope_ref, .. } => {
                ask.scope_ref = scope_ref;
                ask.scope_revision += 1;
                ask.acceptance = None;
                "revised"
            }
            Write::Link {
                kind, reference, ..
            } => {
                if ask
                    .links
                    .iter()
                    .any(|link| link.kind == kind && link.reference == reference)
                {
                    return Ok(ask);
                }
                if ask.links.len() >= MAX_LINKS {
                    return Err(Failure::Capacity);
                }
                ask.links.push(Link { kind, reference });
                "linked"
            }
            Write::Accept {
                scope_revision,
                evidence_ref,
                ..
            } => {
                if ask.scope_revision != scope_revision
                    || !ask.links.iter().any(|link| {
                        link.kind == LinkKind::Artifact && link.reference == evidence_ref
                    })
                {
                    return Err(Failure::Conflict);
                }
                ask.acceptance = Some(Acceptance {
                    scope_revision,
                    evidence_ref,
                });
                "accepted"
            }
            Write::Create { .. } => unreachable!(),
        };
        ask.revision += 1;
        self.update(&ask, event).await?;
        Ok(ask)
    }

    async fn create(
        &mut self,
        key: Uuid,
        payload: &[u8],
        parent_id: Option<Uuid>,
        request_ref: Uuid,
        scope_ref: Uuid,
    ) -> Result<Ask, Failure> {
        if let Some((stored, id)) = sqlx::query_as::<_, (Vec<u8>, Uuid)>(
            "SELECT payload, id FROM work_identity.work_identity_create WHERE workspace = $1 AND key = $2",
        )
        .bind(self.workspace)
        .bind(key)
        .fetch_optional(&mut *self.db)
        .await?
        {
            return if stored == payload {
                get(self.db, self.workspace, id).await
            } else {
                Err(Failure::Conflict)
            };
        }
        if self.ordinal > MAX_ASKS {
            return Err(Failure::Capacity);
        }
        let mut segments = self.parent_segments(parent_id).await?;
        if segments.len() >= MAX_DEPTH {
            return Err(Failure::Capacity);
        }
        let prefix = if segments.is_empty() {
            format!("{}-", self.label)
        } else {
            format!("{}.", address(&self.label, &segments))
        };
        let segment = sqlx::query_scalar::<_, i32>(
            "SELECT n FROM generate_series(1, $3::integer) n WHERE NOT EXISTS
               (SELECT 1 FROM work_identity.work_identity_address WHERE workspace = $1 AND address = $2 || n::text)
               ORDER BY n LIMIT 1",
        )
        .bind(self.workspace)
        .bind(prefix)
        .bind(ALLOCATION_CANDIDATES)
        .fetch_optional(&mut *self.db)
        .await?
        .ok_or(Failure::Capacity)?;
        segments.push(segment);
        let address = address(&self.label, &segments);
        let ask = Ask {
            id: Uuid::new_v4(),
            workspace_id: self.workspace,
            origin_address: address.clone(),
            address,
            segments,
            origin_parent_id: parent_id,
            parent_id,
            request_ref,
            scope_ref,
            revision: 1,
            scope_revision: 1,
            links: Vec::new(),
            acceptance: None,
        };
        sqlx::query(
            "INSERT INTO work_identity.work_identity_ask (workspace, id, ordinal, body) VALUES ($1, $2, $3, $4)",
        )
        .bind(self.workspace)
        .bind(ask.id)
        .bind(self.ordinal)
        .bind(Json(&ask))
        .execute(&mut *self.db)
        .await?;
        self.ordinal += 1;
        self.reserve(&ask).await?;
        self.record(&ask, "created").await?;
        sqlx::query("INSERT INTO work_identity.work_identity_create (workspace, key, payload, id) VALUES ($1, $2, $3, $4)")
            .bind(self.workspace)
            .bind(key)
            .bind(payload)
            .bind(ask.id)
            .execute(&mut *self.db)
            .await?;
        Ok(ask)
    }

    async fn parent_segments(&mut self, parent: Option<Uuid>) -> Result<Vec<i32>, Failure> {
        match parent {
            Some(id) => Ok(get(self.db, self.workspace, id).await?.segments),
            None => Ok(Vec::new()),
        }
    }

    async fn move_subtree(
        &mut self,
        mut ask: Ask,
        parent_id: Option<Uuid>,
        segment: i32,
    ) -> Result<Ask, Failure> {
        if segment <= 0 {
            return Err(Failure::Invalid);
        }
        let mut destination = self.parent_segments(parent_id).await?;
        if destination.starts_with(&ask.segments) {
            return Err(Failure::Conflict);
        }
        destination.push(segment);
        if destination == ask.segments {
            return Ok(ask);
        }

        let descendants = sqlx::query_scalar::<_, Json<Ask>>(
            "SELECT body FROM work_identity.work_identity_ask WHERE workspace = $1 AND starts_with(body->>'address', $2)
               ORDER BY ordinal",
        )
        .bind(self.workspace)
        .bind(format!("{}.", ask.address))
        .fetch_all(&mut *self.db)
        .await?;
        let mut changes = Vec::with_capacity(descendants.len());
        for Json(mut descendant) in descendants {
            let suffix = descendant
                .segments
                .strip_prefix(ask.segments.as_slice())
                .ok_or(Failure::Unavailable)?;
            descendant.segments = [destination.as_slice(), suffix].concat();
            if descendant.segments.len() > MAX_DEPTH {
                return Err(Failure::Capacity);
            }
            descendant.address = address(&self.label, &descendant.segments);
            descendant.revision += 1;
            changes.push(descendant);
        }
        if destination.len() > MAX_DEPTH {
            return Err(Failure::Capacity);
        }
        ask.parent_id = parent_id;
        ask.segments = destination;
        ask.address = address(&self.label, &ask.segments);
        ask.revision += 1;
        self.reserve(&ask).await?;
        self.update(&ask, "moved").await?;
        for descendant in changes {
            self.reserve(&descendant).await?;
            self.update(&descendant, "moved").await?;
        }
        Ok(ask)
    }

    async fn reserve(&mut self, ask: &Ask) -> Result<(), Failure> {
        let owner = sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO work_identity.work_identity_address (workspace, address, id) VALUES ($1, $2, $3)
             ON CONFLICT (workspace, address) DO UPDATE SET id = EXCLUDED.id
               WHERE work_identity.work_identity_address.id = EXCLUDED.id RETURNING id",
        )
        .bind(self.workspace)
        .bind(&ask.address)
        .bind(ask.id)
        .fetch_optional(&mut *self.db)
        .await?;
        owner.ok_or(Failure::Conflict)?;
        Ok(())
    }

    async fn update(&mut self, ask: &Ask, event: &str) -> Result<(), Failure> {
        sqlx::query(
            "UPDATE work_identity.work_identity_ask SET body = $3 WHERE workspace = $1 AND id = $2",
        )
        .bind(self.workspace)
        .bind(ask.id)
        .bind(Json(ask))
        .execute(&mut *self.db)
        .await?;
        self.record(ask, event).await
    }

    async fn record(&mut self, ask: &Ask, event: &str) -> Result<(), Failure> {
        if self.cursor > i64::from(MAX_CHANGES) {
            return Err(Failure::Capacity);
        }
        sqlx::query("INSERT INTO work_identity.work_identity_change (workspace, ordinal, id, event, body) VALUES ($1, $2, $3, $4, $5)")
            .bind(self.workspace)
            .bind(self.cursor)
            .bind(ask.id)
            .bind(event)
            .bind(Json(ask))
            .execute(&mut *self.db)
            .await?;
        self.cursor += 1;
        Ok(())
    }
}

fn address(label: &str, segments: &[i32]) -> String {
    let segments: Vec<_> = segments.iter().map(i32::to_string).collect();
    format!("{label}-{}", segments.join("."))
}
