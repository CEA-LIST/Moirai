//! M-A8 and I-A10, as a test: `moirai-protocol` and `moirai-network` name no
//! model, and no interpreter either.
//!
//! The layering of the model plane is a grep, run here from the workspace root
//! exactly as CI runs it. The words `ModelId` and `MetamodelId` belong to the
//! application layer — the generated node binary and the editor — and to
//! nothing below it, and so does `eClass`, the instance encoding's class tag:
//! the schema parsed from a descriptor and the structural check that reads
//! it live in the generated crate, behind the guard hook, and never here.
//! The route literals `/api/models` and `/api/model/` are strings rather
//! than identifiers, so the match is on whole words.
//!
//! Phase 5 adds two words to the same list. `MergeRule` and `SemanticsTable`
//! are the interpreted path's vocabulary, and the interpreted node is an
//! application exactly as the generated one is: `moirai-network` gained the
//! route that serves one more descriptor and the command behind it, and a
//! descriptor is opaque text to it — what one *means* is read in
//! `moirai-semantics` and `moirai-interp`, above this line.

use std::path::Path;
use std::process::Command;

#[test]
fn mp12_ip22_no_model_or_interpreter_symbol_in_protocol_or_network() {
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
            "-e",
            "MergeRule",
            "-e",
            "SemanticsTable",
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
        "a model or interpreter symbol below the application layer (grep status {:?}):\n{hits}{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
