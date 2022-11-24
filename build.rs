use std::process::Command;

fn main() {
    let mut config = prost_build::Config::new();
    config.type_attribute(
        "Entry",
        "#[allow(missing_docs, clippy::missing_docs_in_private_items)]",
    );
    config.type_attribute(
        "Entry.data",
        "#![allow(missing_docs, clippy::missing_docs_in_private_items)]",
    );
    config.type_attribute(
        "Message",
        "#[allow(missing_docs, clippy::missing_docs_in_private_items)]",
    );
    config.type_attribute(
        "Message.data",
        "#![allow(missing_docs,clippy::missing_docs_in_private_items)]",
    );
    config.bytes(["."]);
    for item in [
        "HardState",
        "MsgHup",
        "MsgBeat",
        "MsgAppendResponse",
        "MsgRequestVote",
        "MsgRequestVoteResponse",
        "MsgHeartbeat",
        "MsgHeartbeatResponse",
        "MsgSnapshot",
        "MsgTimeoutNow",
        "MsgTransferLeader",
    ] {
        config.type_attribute(item, "#[non_exhaustive] #[derive(Copy, Eq)]");
    }
    for item in [
        "ConfState",
        "Entry",
        "Entry.data",
        "EntryNormal",
        "MsgPropose",
        "MsgAppend",
        "Message.data",
        "Message",
    ] {
        config.type_attribute(item, "#[non_exhaustive]");
    }

    for item in [
        "ConfState",
        "Entry",
        "Entry.data",
        "EntryNormal",
        "MsgPropose",
        "MsgAppend",
        "Message.data",
        "Message",
    ] {
        config.type_attribute(item, "#[derive(Eq)]");
    }

    config
        .out_dir("src/pb")
        .compile_protos(&["proto/eraftpb.proto"], &["."])
        .unwrap();

    Command::new("cargo")
        .args(["fmt", "--", "src/*.rs"])
        .status()
        .expect("cargo fmt failed");
}
