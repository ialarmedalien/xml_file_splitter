//! Integration tests for the XML splitter running inside the `kbase/cdm-data-loaders`
//! Docker image with `xml_file_splitter` built from source.
//!
//! These tests are marked `#[ignore]` by default so standard `cargo test` runs do
//! not require a Docker daemon. Run them with:
//!
//! ```sh
//! cargo test --test integration_docker -- --ignored
//! ```
//!
//! If `DOCKER_TEST_IMAGE` is set in the environment, that image is used directly;
//! otherwise the test harness builds the `cdm-integration-test` stage from the
//! repository's `Dockerfile` once before running the test suite.

use std::path::Path;
use std::process::Command;
use std::sync::OnceLock;

mod common;

const DEFAULT_TEST_IMAGE: &str = "xml-file-splitter-cdm-test:latest";

static DOCKER_IMAGE: OnceLock<Result<String, String>> = OnceLock::new();

/// Return the Docker image tag to test against, building the `cdm-integration-test`
/// stage from `Dockerfile` once if `DOCKER_TEST_IMAGE` is not set.
fn docker_image() -> &'static str {
    DOCKER_IMAGE
        .get_or_init(|| {
            if let Ok(img) = std::env::var("DOCKER_TEST_IMAGE")
                && !img.trim().is_empty()
            {
                return Ok(img);
            }

            let status = Command::new("docker")
                .arg("build")
                .arg("--target")
                .arg("cdm-integration-test")
                .arg("-t")
                .arg(DEFAULT_TEST_IMAGE)
                .arg(env!("CARGO_MANIFEST_DIR"))
                .status()
                .map_err(|e| format!("failed to invoke `docker build`: {e}"))?;

            if !status.success() {
                return Err("`docker build --target cdm-integration-test` failed".to_string());
            }

            Ok(DEFAULT_TEST_IMAGE.to_string())
        })
        .as_ref()
        .unwrap_or_else(|err| panic!("{err}"))
}

/// Ensure the temporary output directory is writable by the container's `nonroot`
/// user (UID 999 in `kbase/cdm-data-loaders`) when running on Linux hosts where
/// the host test runner user has a different UID.
fn make_world_writable(dir: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o777))
            .expect("failed to set permissions on temp output dir");
    }
}

/// Invoke `xml_split` inside the `cdm-data-loaders` container with `/input`
/// mounted from `tests/fixtures` and `/output` mounted from `output_dir`.
fn run_container_splitter(
    output_dir: &Path,
    input_filename: &str,
    chunk_size: usize,
    gzip: bool,
    validate: bool,
) {
    make_world_writable(output_dir);

    let fixtures = common::fixtures_dir();
    let image = docker_image();

    let mut cmd = Command::new("docker");
    cmd.arg("run")
        .arg("--rm")
        .arg("-v")
        .arg(format!("{}:/input:ro", fixtures.display()))
        .arg("-v")
        .arg(format!("{}:/output", output_dir.display()))
        .arg(image)
        .arg("xml_split")
        .arg("--input")
        .arg(format!("/input/{input_filename}"))
        .arg("--chunk-size")
        .arg(chunk_size.to_string())
        .arg("--element")
        .arg("entry")
        .arg("--output-prefix")
        .arg("/output/chunk");

    if gzip {
        cmd.arg("--gzip");
    }
    if validate {
        cmd.arg("--validate");
    }

    let status = cmd
        .status()
        .expect("failed to launch docker container for xml_split");
    assert!(
        status.success(),
        "container exited with non-zero status for input={input_filename} chunk_size={chunk_size} gzip={gzip} validate={validate}"
    );
}

/// Core helper: run the splitter inside the container against `input.xml.gz`
/// and compare every output chunk against the golden files.
fn run_and_compare(chunk_size: usize, gzip: bool, validate: bool) {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "input.xml.gz", chunk_size, gzip, validate);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    common::compare_chunks_against_golden(&prefix, chunk_size, gzip);
}

// ---------------------------------------------------------------------------
// Plain-output tests
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_docker_split_chunk_size_20() {
    run_and_compare(20, false, true);
}

#[test]
#[ignore]
fn test_docker_split_chunk_size_5() {
    run_and_compare(5, false, true);
}

#[test]
#[ignore]
fn test_docker_split_chunk_size_4() {
    run_and_compare(4, false, true);
}

#[test]
#[ignore]
fn test_docker_split_chunk_size_1() {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "input.xml.gz", 1, false, true);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    for chunk_index in 1..=15 {
        let path = xml_file_splitter::writer::chunk_path(&prefix, chunk_index, false);
        assert!(path.exists(), "expected chunk {chunk_index} to exist");
    }
    let extra = xml_file_splitter::writer::chunk_path(&prefix, 16, false);
    assert!(!extra.exists(), "unexpected extra chunk 16");
}

#[test]
#[ignore]
fn test_docker_split_no_entries() {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "no_entries.xml.gz", 20, false, true);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    let chunk_1 = xml_file_splitter::writer::chunk_path(&prefix, 1, false);
    let chunk_2 = xml_file_splitter::writer::chunk_path(&prefix, 2, false);
    assert!(chunk_1.exists(), "expected chunk 1 to exist for no_entries");
    assert!(!chunk_2.exists(), "unexpected chunk 2 for no_entries");
}

#[test]
#[ignore]
fn test_docker_split_nested_entries() {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "nested_entries.xml.gz", 20, false, true);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    let chunk_1 = xml_file_splitter::writer::chunk_path(&prefix, 1, false);
    let chunk_2 = xml_file_splitter::writer::chunk_path(&prefix, 2, false);
    assert!(chunk_1.exists(), "expected chunk 1 to exist for nested_entries");
    assert!(!chunk_2.exists(), "unexpected chunk 2 for nested_entries");
}

// ---------------------------------------------------------------------------
// Gzip-output tests
// ---------------------------------------------------------------------------

#[test]
#[ignore]
fn test_docker_split_gzip_chunk_size_20() {
    run_and_compare(20, true, true);
}

#[test]
#[ignore]
fn test_docker_split_gzip_chunk_size_5() {
    run_and_compare(5, true, true);
}

#[test]
#[ignore]
fn test_docker_split_gzip_chunk_size_4() {
    run_and_compare(4, true, true);
}

/// Output files carry the `.xml.gz` extension and are valid gzip streams.
#[test]
#[ignore]
fn test_docker_gzip_output_files_are_valid_gz() {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "input.xml.gz", 20, true, false);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    common::assert_gzip_chunks_valid(&prefix, 20);
}

/// Plain-mode output files must NOT be gzip streams.
#[test]
#[ignore]
fn test_docker_plain_output_files_are_not_gz() {
    let tmp = tempfile::tempdir().unwrap();
    run_container_splitter(tmp.path(), "input.xml.gz", 20, false, false);

    let prefix = tmp.path().join("chunk").to_str().unwrap().to_string();
    common::assert_plain_chunks_not_gz(&prefix, 20);
}
