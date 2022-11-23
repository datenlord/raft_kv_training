use std::process::Command;

fn main() {
    let mut config = prost_build::Config::new();
    config.bytes(["."]);
    config
        .out_dir("src/pb")
        .compile_protos(&["proto/eraftpb.proto"], &["."])
        .unwrap();

    Command::new("cargo")
        .args(["fmt", "--", "src/*.rs"])
        .status()
        .expect("cargo fmt failed");
}
