//! M-A8, as a test: `moirai-protocol` and `moirai-network` name no model.
//!
//! The layering of the model plane is a grep, run here from the workspace root
//! exactly as CI runs it. The words `ModelId` and `MetamodelId` belong to the
//! application layer — the generated node binary and the editor — and to
//! nothing below it, and so does `eClass`, the instance encoding's class tag:
//! the schema parsed from a descriptor and the structural check that reads
//! it live in the generated crate, behind the guard hook, and never here.
//! The route literals `/api/models` and `/api/model/` are strings rather
//! than identifiers, so the match is on whole words.

use std::path::Path;
use std::process::Command;

#[test]
fn mp12_no_model_symbol_in_protocol_or_network() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let output = Command::new("grep")
        .args([
            "-rnw",
            "-e",
            "ModelId",
            "-e",
            "MetamodelId",
            "-e",
            "eClass",
            "moirai-protocol/src",
            "moirai-network/src",
        ])
        .current_dir(&workspace)
        .output()
        .expect("grep is available on the machine running the tests");
    let hits = String::from_utf8_lossy(&output.stdout);

    // grep exits 1 when nothing matched, 0 on a hit and 2 on an error such as
    // a missing directory; only the first is a pass.
    assert!(
        output.status.code() == Some(1) && hits.is_empty(),
        "a model symbol below the application layer (grep status {:?}):\n{hits}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
