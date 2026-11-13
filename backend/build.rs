// the client of chat's InviteService, from the copy of its proto. needs protoc
fn main() {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(&["proto/invite.proto"], &["proto"])
        .expect("fail to compile proto/invite.proto");
}
