use mysql_async::{Transaction, Value, params, prelude::Queryable};
use serde_json::Value as JsonValue;
use uuid::Uuid;

use crate::moderation_client::Verdict;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Post,
    Invite,
    Profile,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Kind::Post => "post",
            Kind::Invite => "invite",
            Kind::Profile => "profile",
        }
    }
}

pub struct NewVerdict<'a> {
    pub kind: Kind,
    pub target_id: Uuid,
    pub user_id: Uuid,
    pub content_updated_at: Option<Value>,
    pub contents: JsonValue,
    pub model: &'a str,
    pub verdict: &'a Verdict,
}

pub async fn insert(tx: &mut Transaction<'_>, new: &NewVerdict<'_>) -> mysql_async::Result<()> {
    tx.exec_drop(
        r"-- sql
    INSERT INTO verdict
    (kind, target_id, user_id, content_updated_at, contents, model, violation, category, reason)
    VALUES (:kind, :target_id, :user_id, :content_updated_at, :contents, :model, :violation, :category, :reason)",
        params! {
            "kind" => new.kind.as_str(),
            "target_id" => new.target_id.as_bytes().to_vec(),
            "user_id" => new.user_id.as_bytes().to_vec(),
            "content_updated_at" => new.content_updated_at.clone(),
            "contents" => new.contents.to_string(),
            "model" => new.model,
            "violation" => new.verdict.violation,
            "category" => &new.verdict.category,
            "reason" => &new.verdict.reason,
        },
    )
    .await
}
