//! EC.R1a (`cea-cdrt-knowledge/spec/items/EC.R1a.md`): every descriptor fixture in this repository
//! is the bytes `arachne describe` writes for its source `.ecore` ([`REGENERATED`]), or is listed in
//! [`KEPT`], unchanged since 684f817, with the reason it is not; and each regenerated one reads,
//! through `from_descriptor` (the reader `moirai-interp` installs a log with), to the table its
//! 684f817 bytes read to, the digest aside.
//!
//! `describe_writes_every_fixture` runs the binary `ARACHNE_BIN` names (Arachne
//! `akira/interpreter-modelstore` 8cf29f5 or later). Without it the test writes `ECR1A-SKIP` and
//! the reason to stderr and returns: a skip is not a pass.
//!
//! ```bash
//! ARACHNE_BIN=<arachne-model-plane's target>/debug/arachne \
//!     nice -n 10 cargo test -j 2 -p moirai-interp --test ecr1a_describe_fixtures
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use moirai_semantics::{from_descriptor, metamodel_digest};
use serde_json::Value;

/// `(fixture, its source, the fixture's digest at 684f817)`, paths from the repository root. A
/// fixture with a `<stem>.id` beside it also holds the identity line describe prints last.
const REGENERATED: [(&str, &str, &str); 24] = [
    (
        "moirai-interp/tests/fixtures/json.metamodel.json",
        "moirai-interp/tests/fixtures/json.ecore",
        "a9d83505175d8d6708b86c7eb64d356d64197f066f752c1a9ea842c76a45bc30",
    ),
    (
        "moirai-interp/tests/fixtures/ed16a/notation.metamodel.json",
        "moirai-interp/tests/fixtures/ed16a/notation.ecore",
        "14377158866c6dd249915cdadbc5579efb64a3e742b5bd7aff1ced4396f57b86",
    ),
    (
        "moirai-interp/tests/fixtures/si05/si05board.metamodel.json",
        "moirai-interp/tests/fixtures/si05/si05board.ecore",
        "0e7f449175bef61d4d42b375818d48887fd9964a8a7c0733e39927e2d5733928",
    ),
    (
        "moirai-interp/tests/fixtures/si05/si05keyed.metamodel.json",
        "moirai-interp/tests/fixtures/si05/si05keyed.ecore",
        "27d9ce713f73f35e14709d69aa1def907750ae8d24aa7dda698e57c7837d91f0",
    ),
    (
        "moirai-interp/tests/fixtures/si07/simpleuml.identity.metamodel.json",
        "moirai-interp/tests/fixtures/si12/simpleuml.d1.ecore",
        "5d34af887ace294f008c96403a4e6dd442f374d4e23639ec1620c04a4cf301f3",
    ),
    (
        "moirai-interp/tests/fixtures/si08/si08nest.metamodel.json",
        "moirai-interp/tests/fixtures/si08/si08nest.ecore",
        "c44b334a5b55f74dd8153a1dfe790e7bcf57603a99c47bdd2e811520dd0687c4",
    ),
    (
        "moirai-interp/tests/fixtures/si12/simpleuml.d1.metamodel.json",
        "moirai-interp/tests/fixtures/si12/simpleuml.d1.ecore",
        "5d34af887ace294f008c96403a4e6dd442f374d4e23639ec1620c04a4cf301f3",
    ),
    (
        "moirai-interp/tests/fixtures/si12/simpleuml.d2.metamodel.json",
        "moirai-interp/tests/fixtures/si12/simpleuml.d2.ecore",
        "f02b1b1fc95a3b9139298ec70618786f4e5373e296c6ea6a8d6f1a44285afb4c",
    ),
    (
        "moirai-interp/tests/fixtures/si12/simpleuml.d2-remove.metamodel.json",
        "moirai-interp/tests/fixtures/si12/simpleuml.d2-remove.ecore",
        "ea1b9b94692c960367c8696820745793dc349598402954a9b57b17f141951341",
    ),
    (
        "moirai-interp/tests/fixtures/si12/simpleuml.d2-rename.metamodel.json",
        "moirai-interp/tests/fixtures/si12/simpleuml.d2-rename.ecore",
        "2f84c0d089ff1b6454b5ece0fb1def4516eb8630a561f50c1bb7b05f5983c1e5",
    ),
    (
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-drop.metamodel.json",
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-drop.ecore",
        "ff3a1997150881b864404bc35d0bdd17cc7efd7cc3e4b4a35f39f2ccc5a1b46a",
    ),
    (
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-flag.metamodel.json",
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-flag.ecore",
        "f921f2e2f8e3469653f9ca8673857c270fb2ac01ae12695989fd917fab31688e",
    ),
    (
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-kind.metamodel.json",
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-kind.ecore",
        "e07e294bda1675ef49464b63653969bb08ccc879d4ef75f567e0ebef82adfd78",
    ),
    (
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-lww.metamodel.json",
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-lww.ecore",
        "0d31fc9c9ceaf5dfb3f72cefdca47599a5977fdbc5ad6469dd7733f8940d6385",
    ),
    (
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-noassoc.metamodel.json",
        "moirai-interp/tests/fixtures/si14/simpleuml.d2-noassoc.ecore",
        "55bf5ee542cdc8f6fa26e9854cab6510de9a89a750ba13505c04cb65cf35d36a",
    ),
    (
        "moirai-interp/tests/fixtures/st01.metamodel.json",
        "moirai-interp/tests/fixtures/st01.ecore",
        "7840af5dac657b02e69b3acdf2e301f3627c6f43c90f3a6c5299a54a7a646b4c",
    ),
    (
        "moirai-interp/tests/fixtures/st02.metamodel.json",
        "moirai-interp/tests/fixtures/st02.ecore",
        "632d63c7e4fc3640c83b43c14defc566cc84bb79a1b70fe59858b3da896138e8",
    ),
    (
        "moirai-interp/tests/fixtures/st04.metamodel.json",
        "moirai-interp/tests/fixtures/st04.ecore",
        "db63631a94b02cdea8249d6adb85ec0b68f627328526a3c2f5afe06baf765385",
    ),
    (
        "moirai-interp/tests/fixtures/st05.metamodel.json",
        "moirai-interp/tests/fixtures/st05.ecore",
        "d43cae9a05bd099529d24894d2d7c6c7aac76e7ea380df4c4c28209f47132ec3",
    ),
    (
        "moirai-semantics/tests/fixtures/ecm1j_m/tree.json",
        "moirai-semantics/tests/fixtures/ecm1j_m/tree.ecore",
        "aa77c957c35a7b8b647c98bc1a20d86554cda597323d38a9469406912aec7bfa",
    ),
    (
        "moirai-semantics/tests/fixtures/ecm1j_m/classless_root.json",
        "moirai-semantics/tests/fixtures/ecm1j_m/classless_root.ecore",
        "3cfabec549980b9b838581f93a1731e3c099610d2e556891cb57b63acc52a0d7",
    ),
    (
        "moirai-semantics/tests/fixtures/ecm1j_m/deptree.json",
        "moirai-semantics/tests/fixtures/ecm1j_m/deptree.ecore",
        "e5ff14739412a7ca63c660003e855cbd5e31e64216f5ac61bfdc5d2bca666205",
    ),
    (
        "moirai-semantics/tests/fixtures/ecm1j_m/corpus_c53.json",
        "moirai-semantics/tests/fixtures/ecm1j_m/corpus_c53.ecore",
        "88862e14072aaa317bd76681c8163e0dc070a28878014a90bc7b6b34ede3ef80",
    ),
    (
        "moirai-semantics/tests/fixtures/ecm1j_m/corpus_c67.json",
        "moirai-semantics/tests/fixtures/ecm1j_m/corpus_c67.ecore",
        "16f5495e0b673966c40fda4867281759c6f91c39cbea8440fd95dc411825cbd3",
    ),
];

/// Descriptor fixtures not regenerated: each is an older describe's output, and the source's
/// descriptor today reads to another table, so regenerating it would change what its tests read.
/// `(fixture, its digest at 684f817, the reason)`.
const KEPT: [(&str, &str, &str); 7] = [
    (
        "moirai-interp/tests/fixtures/bt.metamodel.json",
        "b899ec335e7dc46303d5a7871a1370a12db8858718bea57dc6f58f01c474feb5",
        "behavior_tree.ecore before SI.05: DataFlowPort.entry is a plain reference, an identityReference today",
    ),
    (
        "moirai-interp/tests/fixtures/notation.metamodel.json",
        "9d551713bd2d1b1cfd8c38e591ed33a454706305278f0d1c1dc7d3589167506c",
        "the notation descriptor Arachne shipped at dc16103 (9d551713), ED.16a's old descriptor",
    ),
    (
        "moirai-interp/tests/fixtures/si05/si05board.today.metamodel.json",
        "53a9cd162cbde85cf6eb695b9e722baf9fb140504f03b2f2e2fbb0b285e93deb",
        "si05board.ecore before SI.05, plain references: the side SI.05's tests compare against",
    ),
    (
        "moirai-interp/tests/fixtures/si10/legacy/umlclass.a44920ad.metamodel.json",
        "a44920adc661d308b7bbb9a8e4ccf1549a093546a241c0e33b28596de8000f3b",
        "SI.10's legacy register L2: the umlclass descriptor ED.10 was written for, before SI.05",
    ),
    (
        "moirai-interp/tests/fixtures/sysml.ar02.metamodel.json",
        "ebd580696f97d9339c148f5b29db5d0ff7b0928d78fb05efc7c206811405e627",
        "SysML at AR.02, before SI.05: 34 plain references, identityReference today",
    ),
    (
        "moirai-interp/tests/fixtures/uml.ar02.metamodel.json",
        "21c50dea71938b861233b72116f5ead094fe1bbb3d67bf623864f52c3ead1c3f",
        "SimpleUML at AR.02, before SI.05: 4 plain references, identityReference today",
    ),
    (
        "moirai-interp/tests/fixtures/uml.metamodel.json",
        "12d1a371fd995bce2c44e696e3714d59dae58fe04ff6450382a1e02b0a8a27dd",
        "SimpleUML before AR.02 and SI.05: no eOpposite, plain references",
    ),
];

/// The keys EC.05 and EC.06 add to a descriptor: top level, per class, per feature.
const ADDED_TOP: [&str; 3] = ["nsPrefix", "enumLiterals", "datatypes"];
const ADDED_CLASS: [&str; 2] = ["interface", "superTypeOrder"];
const ADDED_FEATURE: [&str; 6] = [
    "lowerBound",
    "upperBound",
    "default",
    "changeable",
    "unsettable",
    "eKeys",
];

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("..")
}

fn read(path: &str) -> Value {
    serde_json::from_slice(&fs::read(root().join(path)).expect(path)).expect(path)
}

/// Every `.json` file under `dir` that is a descriptor (a top-level `formatVersion`).
fn descriptors(dir: &Path, found: &mut BTreeSet<String>) {
    for entry in fs::read_dir(dir).unwrap().map(Result::unwrap) {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() && name != "target" && !name.starts_with('.') {
            descriptors(&path, found);
        } else if name.ends_with(".json")
            && serde_json::from_slice::<Value>(&fs::read(&path).unwrap())
                .is_ok_and(|v| v.get("formatVersion").is_some())
        {
            let rel = path.strip_prefix(root()).unwrap();
            found.insert(rel.to_string_lossy().into_owned());
        }
    }
}

#[test]
fn every_descriptor_fixture_is_listed_once() {
    let mut found = BTreeSet::new();
    descriptors(&root(), &mut found);
    let listed: Vec<&str> = REGENERATED
        .iter()
        .map(|r| r.0)
        .chain(KEPT.map(|k| k.0))
        .collect();
    let set: BTreeSet<String> = listed.iter().map(|p| p.to_string()).collect();
    assert_eq!(set.len(), listed.len(), "ECR1A: a fixture is listed twice");
    assert_eq!(
        found, set,
        "ECR1A: the descriptor fixtures are not the ones listed"
    );
}

#[test]
fn kept_fixtures_are_their_684f817_descriptors() {
    for (fixture, before, _) in KEPT {
        assert_eq!(
            metamodel_digest(&read(fixture)),
            before,
            "ECR1A: {fixture} is kept, yet it is not its 684f817 descriptor"
        );
    }
}

#[test]
fn describe_writes_every_fixture() {
    let Some(bin) = std::env::var_os("ARACHNE_BIN") else {
        // Straight to stderr, past the harness's capture, so the skip shows in a plain run.
        let _ = writeln!(
            std::io::stderr(),
            "ECR1A-SKIP describe_writes_every_fixture: ARACHNE_BIN is not set, so there is no \
             `arachne` binary to run `arachne describe` with; a skip is not a pass"
        );
        return;
    };
    let mut failures = Vec::new();
    for (fixture, source, _) in REGENERATED {
        let run = Command::new(&bin)
            .env_remove("RUST_LOG")
            .arg("describe")
            .arg(root().join(source))
            .output()
            .expect("run ARACHNE_BIN describe");
        if !run.status.success() {
            failures.push(format!(
                "{fixture}: describe {source} exit {:?}",
                run.status.code()
            ));
            continue;
        }
        if run.stdout != fs::read(root().join(fixture)).unwrap() {
            failures.push(format!(
                "{fixture}: not the bytes describe writes for {source}"
            ));
        }
        let id = Path::new(fixture).with_extension("id");
        if root().join(&id).exists() {
            let stderr = String::from_utf8_lossy(&run.stderr);
            let line = stderr.lines().rfind(|l| !l.is_empty()).unwrap_or("");
            if fs::read_to_string(root().join(&id)).unwrap() != format!("{line}\n") {
                failures.push(format!(
                    "{}: not the identity line describe prints",
                    id.display()
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "ECR1A: {} differences from describe over {} fixtures:\n  {}",
        failures.len(),
        REGENERATED.len(),
        failures.join("\n  ")
    );
}

/// `d` without the keys EC.05 and EC.06 add.
fn without_added(d: &Value) -> Value {
    let mut d = d.clone();
    let top = d.as_object_mut().unwrap();
    for k in ADDED_TOP {
        top.remove(k);
    }
    for class in top["classes"].as_object_mut().unwrap().values_mut() {
        let class = class.as_object_mut().unwrap();
        for k in ADDED_CLASS {
            class.remove(k);
        }
        for kind in ["attributes", "containments", "references"] {
            for feature in class
                .get_mut(kind)
                .and_then(Value::as_array_mut)
                .into_iter()
                .flatten()
            {
                for k in ADDED_FEATURE {
                    feature.as_object_mut().unwrap().remove(k);
                }
            }
        }
    }
    d
}

#[test]
fn regenerated_fixtures_read_to_their_684f817_tables() {
    let mut failures = Vec::new();
    for (fixture, _, before) in REGENERATED {
        let now = read(fixture);
        let old = without_added(&now);
        if metamodel_digest(&old) != before {
            failures.push(format!(
                "{fixture}: without EC.05's and EC.06's keys it is not its 684f817 descriptor"
            ));
            continue;
        }
        match (from_descriptor(&now), from_descriptor(&old)) {
            (Ok(mut now), Ok(old)) => {
                now.digest = old.digest.clone();
                if now != old {
                    failures.push(format!("{fixture}: reads to another table than at 684f817"));
                }
            }
            (now, old) => failures.push(format!(
                "{fixture}: read {:?}, at 684f817 {:?}",
                now.map(|_| "a table").map_err(|e| e.to_string()),
                old.map(|_| "a table").map_err(|e| e.to_string())
            )),
        }
    }
    assert!(failures.is_empty(), "ECR1A: {}", failures.join("\n  "));
}
