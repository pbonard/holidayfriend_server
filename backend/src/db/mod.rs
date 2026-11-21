//! The sql of every table. a function takes the connection, or the transaction when the service does several
//! writes together, and returns what it read: the service decides what's one transaction
pub mod invite;
pub mod post;
pub mod post_not_interested;
pub mod profile;
