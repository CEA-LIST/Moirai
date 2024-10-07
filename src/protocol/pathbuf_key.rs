use camino::{Utf8Path, Utf8PathBuf};
use radix_trie::TrieKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PathBufKey(Utf8PathBuf);

impl TrieKey for PathBufKey {
    fn encode_bytes(&self) -> Vec<u8> {
        self.0.as_os_str().as_encoded_bytes().to_vec()
    }
}

impl PathBufKey {
    pub fn new(path: &Utf8Path) -> Self {
        Self(path.to_path_buf())
    }
}
