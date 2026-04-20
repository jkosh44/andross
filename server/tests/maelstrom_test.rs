use bzip2::read::BzDecoder;
use std::env;
use std::fs::create_dir_all;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::process::Command;
use tar::Archive;

const MAELSTROM_URL: &str =
    "https://github.com/jepsen-io/maelstrom/releases/download/v0.2.4/maelstrom.tar.bz2";

#[test]
fn test_maelstrom_echo() {
    run_maelstrom_workload("echo");
}

#[test]
fn test_maelstrom_broadcast() {
    run_maelstrom_workload("broadcast");
}

/// Runs a maelstrom workload.
fn run_maelstrom_workload(workload: &str) {
    let project_root_str = env!("CARGO_MANIFEST_DIR");
    let project_root = PathBuf::from(project_root_str);
    // Download Maelstrom and build Andross server concurrently.
    let (maelstrom_bin, andross_bin) = std::thread::scope(|s| {
        let maelstrom_handle = s.spawn(|| download_maelstrom(&project_root));
        let andross_handle = s.spawn(|| build_andross(&project_root));
        (
            maelstrom_handle.join().unwrap(),
            andross_handle.join().unwrap(),
        )
    });
    let log_path = project_root.join("tests").join("maelstrom-logs");
    create_dir_all(log_path.as_path()).unwrap();

    println!("Running Maelstrom workload: {workload}");
    let args = [
        "test",
        "--workload",
        workload,
        "--bin",
        andross_bin.to_str().unwrap(),
        "--concurrency",
        "3",
        "--time-limit",
        "10",
        "--log-stderr",
    ];
    let exit_status = Command::new(maelstrom_bin)
        .args(args)
        .status()
        .expect("Failed to run Maelstrom workload");
    assert!(
        exit_status.success(),
        "Maelstrom workload failed: {exit_status}"
    );
}

/// Download the maelstrom binary if it doesn't already exist at `target_dir`.
fn download_maelstrom(crate_root: &Path) -> PathBuf {
    let maelstrom_dir = crate_root.join("tests");
    let maelstrom_bin = maelstrom_dir.join("maelstrom").join("maelstrom");

    if maelstrom_bin.exists() {
        return maelstrom_bin;
    }

    println!("Downloading Maelstrom from {MAELSTROM_URL}");
    let response = reqwest::blocking::get(MAELSTROM_URL).expect("Failed to download Maelstrom");
    let bytes = response.bytes().expect("Failed to read Maelstrom response");

    println!("Extracting Maelstrom");
    let bz_decoder = BzDecoder::new(Cursor::new(bytes));
    let mut archive = Archive::new(bz_decoder);
    archive
        .unpack(maelstrom_dir)
        .expect("Failed to unpack Maelstrom");

    maelstrom_bin
}

/// Builds the maelstrom compatible andross server binary and returns the path to it.
fn build_andross(project_root: &Path) -> PathBuf {
    println!("Building andross-server");
    let status = Command::new("cargo")
        .args(["build", "--bin", "maelstrom"])
        .status()
        .expect("Failed to run cargo build");
    assert!(status.success(), "Failed to build andross-server: {status}");
    project_root
        .parent()
        .unwrap()
        .join("target/debug/maelstrom")
}
