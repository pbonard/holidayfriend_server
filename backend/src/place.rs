use serde::{Deserialize, Serialize};

/// One of the cities of the web app's list: where a post goes, and where a profile is based in.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Place {
    /// ISO 3166-1 alpha-2
    pub country: String,
    pub city: String,
}
