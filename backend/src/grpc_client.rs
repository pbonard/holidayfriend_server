//! The grpc client of my-server's chat: what chat knows about two people, which an invite between them depends on.

use std::time::Duration;

use tonic::transport::Channel;
use uuid::Uuid;

mod proto {
    tonic::include_proto!("inviteservice");
}

use proto::{CheckInviteRequest, invite_service_client::InviteServiceClient};

/// What chat knows about a sender and the person they invite
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InviteCheck {
    /// the invitee blocked the sender
    pub blocked: bool,
    /// the sender's inbox still has their chat. only the sender's: whether the invitee left isn't the sender's to know
    pub sender_has_chat: bool,
}

/// Where the servers are and how long a call may take
pub struct GrpcClientConfig {
    /// chat's grpc address, http://host:port
    pub chat_url: String,
    /// for connecting, and for each call
    pub timeout: Duration,
}

#[derive(Clone)]
pub struct GrpcClient {
    client: InviteServiceClient<Channel>,
}

impl GrpcClient {
    /// A client of the servers in config. it connects on the first call, and again after the connection fails
    pub fn connect_lazy(config: GrpcClientConfig) -> Self {
        let channel = Channel::from_shared(config.chat_url)
            .expect("chat_url must be a url")
            .connect_timeout(config.timeout)
            .timeout(config.timeout)
            .connect_lazy();
        Self {
            client: InviteServiceClient::new(channel),
        }
    }

    pub async fn check_invite(&self, from: Uuid, to: Uuid) -> Result<InviteCheck, tonic::Status> {
        // a clone shares the channel, the call needs its own handle
        let res = self
            .client
            .clone()
            .check_invite(CheckInviteRequest {
                from_id: from.as_bytes().to_vec(),
                to_id: to.as_bytes().to_vec(),
            })
            .await?
            .into_inner();
        Ok(InviteCheck {
            blocked: res.blocked,
            sender_has_chat: res.sender_has_chat,
        })
    }
}
