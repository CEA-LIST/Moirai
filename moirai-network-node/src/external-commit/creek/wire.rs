//! ID-only control frames for the dedicated consensus channel.
use super::paxos::Message;
use std::{io, sync::Arc};

const MAX_MESSAGE: usize = 8192;
const VERSION: u8 = 0xC1;

pub(super) fn encode(message: &Message) -> io::Result<Arc<[u8]>> {
    let mut bytes = vec![VERSION];
    bincode::serde::encode_into_std_write(message, &mut bytes, bincode::config::standard())
        .map_err(io::Error::other)?;
    if bytes.len() > MAX_MESSAGE {
        return Err(io::Error::other("oversized Creek message"));
    }
    Ok(bytes.into())
}
pub(super) fn decode(bytes: &[u8]) -> io::Result<Message> {
    if bytes.first() != Some(&VERSION) || bytes.len() > MAX_MESSAGE {
        return Err(io::Error::other("invalid Creek frame"));
    }
    let (message, used) = bincode::serde::decode_from_slice(
        &bytes[1..],
        bincode::config::standard().with_limit::<MAX_MESSAGE>(),
    )
    .map_err(io::Error::other)?;
    if used + 1 != bytes.len() {
        return Err(io::Error::other("trailing Creek bytes"));
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::external_commit::creek::{
        VertexId,
        paxos::{Accepted, Ballot},
    };
    #[test]
    fn accept_wire_contains_only_ballot_and_vertex_metadata() {
        let message = Message::Accept {
            slot: 1,
            ballot: Ballot {
                round: 1,
                leader: 2,
            },
            vertex: VertexId { origin: 3, seq: 4 },
        };
        let bytes = encode(&message).unwrap();
        assert_eq!(bytes.len(), 7);
        assert!(matches!(
            decode(&bytes).unwrap(),
            Message::Accept {
                vertex: VertexId { origin: 3, seq: 4 },
                ..
            }
        ));
        let mut trailing = bytes.to_vec();
        trailing.push(0);
        assert!(decode(&trailing).is_err());
        assert!(decode(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn an_empty_promise_does_not_encode_an_accepted_value_placeholder() {
        let ballot = Ballot {
            round: 1,
            leader: 1,
        };
        let empty = encode(&Message::Promise {
            slot: 1,
            ballot,
            accepted: None,
        })
        .unwrap();
        let filled = encode(&Message::Promise {
            slot: 1,
            ballot,
            accepted: Some(Accepted {
                ballot,
                vertex: VertexId { origin: 1, seq: 1 },
            }),
        })
        .unwrap();
        assert_eq!(empty.len(), 6); // version + kind + slot + ballot round/leader + option tag
        assert_eq!(filled.len(), 10); // only Some adds the accepted ballot and ID
    }
}
