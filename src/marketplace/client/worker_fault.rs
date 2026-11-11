//! Observe only the worker's first exact diagnostic code. Never use it for
//! lifecycle decisions or forward its raw stderr to a log or client.
use tokio::io::{AsyncRead, AsyncReadExt};

const PREFIX: &[u8] = b"marketplace worker first failure: ";
const MAX_LINE: usize = 80;
const MAX_BYTES: usize = 64 * 1024;
const CODES: &[&str] = &[
    "worker_startup",
    "worker_retirement",
    "network_control",
    "transport_failed",
    "transport_shutdown_failed",
    "tor_state_socket",
    "tor_state_protocol",
    "tor_state_deadline",
    "private_input_deadline",
    "private_dispatch_deadline",
    "private_start_deadline",
    "private_output",
    "private_channel",
];

struct Parser {
    line: [u8; MAX_LINE],
    len: usize,
    discard: bool,
    recorded: bool,
}

impl Parser {
    fn new() -> Self {
        Self {
            line: [0; MAX_LINE],
            len: 0,
            discard: false,
            recorded: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) -> Option<&'static str> {
        if self.recorded {
            return None;
        }
        for &byte in bytes {
            if byte == b'\n' {
                if !self.discard
                    && let Some(suffix) = self.line[..self.len].strip_prefix(PREFIX)
                    && let Some(&code) = CODES.iter().find(|code| code.as_bytes() == suffix)
                {
                    self.recorded = true;
                    return Some(code);
                }
                self.len = 0;
                self.discard = false;
            } else if !self.discard {
                if self.len == MAX_LINE || !(32..=126).contains(&byte) {
                    self.discard = true;
                } else {
                    self.line[self.len] = byte;
                    self.len += 1;
                }
            }
        }
        None
    }
}

/// Retain the original diagnostic overflow contract: only the byte after
/// 64 KiB is an overflow. That byte never enters the parser.
pub(super) async fn drain(
    mut stream: impl AsyncRead + Unpin,
    mut on_fault: impl FnMut(&'static str),
) -> bool {
    let mut parser = Parser::new();
    let mut buffer = [0u8; 1024];
    let mut remaining = MAX_BYTES;
    loop {
        let limit = (remaining + 1).min(buffer.len());
        match stream.read(&mut buffer[..limit]).await {
            Ok(0) | Err(_) => return false,
            Ok(read) => {
                if let Some(code) = parser.push(&buffer[..read.min(remaining)]) {
                    on_fault(code);
                }
                if read > remaining {
                    return true;
                }
                remaining -= read;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MARKER: &[u8] = b"marketplace worker first failure: tor_state_socket\n";

    #[test]
    fn parser_matches_the_worker_closed_code_contract() {
        let worker = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../crates/elixir_marketplace/worker/src/worker-fault.mjs"
        ));
        let values = worker
            .split_once("const codes=new Set([")
            .unwrap()
            .1
            .split_once("]);")
            .unwrap()
            .0;
        assert_eq!(
            values.split('\'').skip(1).step_by(2).collect::<Vec<_>>(),
            CODES
        );
        let prefix = format!("const prefix='{}';", std::str::from_utf8(PREFIX).unwrap());
        assert!(worker.contains(&prefix));
    }

    #[test]
    fn accepts_one_complete_marker_across_every_split() {
        for split in 0..MARKER.len() {
            let mut parser = Parser::new();
            assert_eq!(parser.push(b"untrusted warning\n"), None);
            assert_eq!(parser.push(&MARKER[..split]), None);
            assert_eq!(parser.push(&MARKER[split..]), Some("tor_state_socket"));
            assert_eq!(
                parser.push(b"marketplace worker first failure: private_output\n"),
                None
            );
        }
    }

    #[test]
    fn rejects_injected_suffixes_unknown_codes_invalid_bytes_and_unfinished_lines() {
        let mut parser = Parser::new();
        for junk in [
            b"marketplace worker first failure: other\n".as_slice(),
            b"marketplace worker first failure: tor_state_socket bad\n",
            b"marketplace worker first failure: tor_state_socket\r\n",
            b"marketplace worker first failure: tor_state_socket\xff\n",
            b"prefix marketplace worker first failure: tor_state_socket\n",
        ] {
            assert_eq!(parser.push(junk), None);
        }
        assert_eq!(parser.push(&[b'x'; MAX_LINE + 1]), None);
        assert_eq!(
            parser.push(MARKER),
            None,
            "no marker suffix from a discarded line"
        );
        assert_eq!(parser.push(&MARKER[..MARKER.len() - 1]), None);
        assert_eq!(parser.push(b"\n"), Some("tor_state_socket"));
    }

    #[tokio::test]
    async fn overflow_is_unchanged_and_extra_bytes_cannot_emit_a_marker() {
        let mut bytes = vec![b'\n'; MAX_BYTES - MARKER.len()];
        bytes.extend(MARKER);
        let mut faults = Vec::new();
        assert!(!drain(bytes.as_slice(), |code| faults.push(code)).await);
        assert_eq!(faults, ["tor_state_socket"]);
        bytes.insert(0, b'\n'); // The marker's closing LF is now outside budget.
        assert!(
            drain(bytes.as_slice(), |_| panic!(
                "overflow byte reached the parser"
            ))
            .await
        );
    }
}
