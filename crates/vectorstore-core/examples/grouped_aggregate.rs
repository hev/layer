//! Execute a selected-row fixture, without reading any backend.
//! cargo run -p vectorstore-core --example grouped_aggregate < fixture.json
//! Input: {"rows":[...],"request":{...},"relation":{...}}
use std::io::{self, Read};

use serde::Deserialize;
use serde_json::Value;
use vectorstore_core::grouped_aggregate::{execute, GroupRequest, Limits, RelationBinding};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input {
    rows: Vec<Value>,
    request: GroupRequest,
    relation: RelationBinding,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input)?;
    let input: Input = serde_json::from_str(&input)?;
    let page = execute(
        &input.rows,
        &input.request,
        &input.relation,
        Limits::default(),
    )?;
    println!("{}", serde_json::to_string(&page)?);
    Ok(())
}
