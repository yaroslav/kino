//! The WebSocket wire format (RFC 6455): the handshake digest, the frames Kino writes, and its
//! clients' frames, which tungstenite reads into messages.

use std::io::{self, Cursor, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, ReadBuf};
use tungstenite::error::{Error, ProtocolError};
use tungstenite::protocol::frame::coding::{CloseCode, OpCode};
use tungstenite::protocol::frame::FrameHeader;
use tungstenite::protocol::{CloseFrame, Role, WebSocketConfig, WebSocketContext};
use tungstenite::Message;

use super::stats::Cause;

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// What a socket's reader buffers (tungstenite's default is 128 KiB).
const READ_BUFFER: usize = 4 * 1024;
const MAX_CONTROL: usize = 125;

/// Sec-WebSocket-Accept for a client's Sec-WebSocket-Key.
pub fn accept_key(key: &[u8]) -> String {
    let mut ctx = ring::digest::Context::new(&ring::digest::SHA1_FOR_LEGACY_USE_ONLY);
    ctx.update(key);
    ctx.update(GUID);
    base64(ctx.finish().as_ref())
}

fn base64(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(input.len().div_ceil(3) * 4);
    for chunk in input.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for (i, shift) in [18, 12, 6, 0].into_iter().enumerate() {
            if i <= chunk.len() {
                out.push(ALPHABET[(n >> shift) as usize & 63] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}

/// What a frame Kino sends carries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
}

/// One server frame: FIN set, unmasked.
pub fn encode(opcode: Opcode, payload: &[u8]) -> Bytes {
    encode_parts(opcode, &[payload])
}

/// One server frame whose payload is `parts` joined, each copied once.
pub(super) fn encode_parts(opcode: Opcode, parts: &[&[u8]]) -> Bytes {
    let len = parts.iter().map(|part| part.len()).sum::<usize>();
    let mut buf = BytesMut::with_capacity(len + 10);
    buf.put_u8(0x80 | opcode as u8);
    if len < 126 {
        buf.put_u8(len as u8);
    } else if len < 65_536 {
        buf.put_u8(126);
        buf.put_u16(len as u16);
    } else {
        buf.put_u8(127);
        buf.put_u64(len as u64);
    }
    for part in parts {
        buf.put_slice(part);
    }
    buf.freeze()
}

pub fn close_frame(code: u16) -> Bytes {
    encode(Opcode::Close, &code.to_be_bytes())
}

/// Codes an endpoint may send (RFC 6455 7.4); 1005, 1006 and 1015 never appear on the wire.
pub fn valid_close_code(code: u16) -> bool {
    matches!(code, 1000..=1003 | 1007..=1011 | 3000..=4999)
}

/// The close code Kino answers with (none to a client that is gone), and the cause it ends for.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Ended {
    pub(super) code: Option<u16>,
    pub(super) cause: Cause,
}

/// A socket's client frames, read into whole messages by tungstenite: masked, each within
/// `max_message` however fragmented. Kino answers pings and closes itself.
pub(super) struct Reader<R> {
    rd: R,
    ws: WebSocketContext,
    fed: Fed,
}

impl<R: AsyncRead + Unpin> Reader<R> {
    pub(super) fn new(rd: R, max_message: usize) -> Self {
        let config = WebSocketConfig::default()
            .read_buffer_size(READ_BUFFER)
            .max_message_size(Some(max_message))
            // A declared length past this is refused before its payload
            // is read; a control frame may carry MAX_CONTROL whatever the cap.
            .max_frame_size(Some(max_message.max(MAX_CONTROL)))
            .accept_unmasked_frames(false);
        Reader {
            rd,
            ws: WebSocketContext::new(Role::Server, Some(config)),
            fed: Fed::default(),
        }
    }

    /// The next message, ping or pong; a close or a failure ends reading.
    pub(super) async fn next(&mut self) -> Result<Message, Ended> {
        let read = std::future::poll_fn(|cx| {
            let mut rd = Polled {
                rd: &mut self.rd,
                cx,
                fed: &mut self.fed,
            };
            match self.ws.read(&mut rd) {
                Err(Error::Io(error)) if error.kind() == io::ErrorKind::WouldBlock => Poll::Pending,
                read => Poll::Ready(read),
            }
        })
        .await;
        match read {
            Ok(Message::Close(frame)) => Err(closed(frame)),
            Ok(message) => {
                if self.fed.handed_back() {
                    self.ws = WebSocketContext::new(Role::Server, Some(*self.ws.get_config()));
                }
                Ok(message)
            }
            Err(error) => Err(failed(&error)),
        }
    }
}

/// What tungstenite has been fed, to tell when it holds none of it: it keeps its buffer as
/// large as the largest frame, so after one past READ_BUFFER the reader starts afresh once empty.
#[derive(Default)]
struct Fed {
    /// A frame head cut short by the end of a read; the longest is
    /// 2 + 8 (length) + 4 (mask) bytes.
    head: [u8; 14],
    have: usize,
    /// The payload still to come of a frame whose head is in.
    rest: u64,
    /// Message-ending frames (FIN set, or control) that tungstenite has yet to hand back.
    unread: usize,
    open: bool,
    /// A data frame past READ_BUFFER came since the last fresh start.
    grown: bool,
}

impl Fed {
    fn feed(&mut self, mut bytes: &[u8]) {
        while !bytes.is_empty() {
            if self.rest == 0 {
                let Some(len) = self.head(&mut bytes) else {
                    return;
                };
                self.rest = len;
            }
            let taken = self.rest.min(bytes.len() as u64);
            self.rest -= taken;
            bytes = &bytes[taken as usize..];
        }
    }

    /// Once the head is whole, its payload length, with `bytes` moved past it. None while the
    /// head is cut short; FrameHeader::parse never refuses a whole head.
    fn head(&mut self, bytes: &mut &[u8]) -> Option<u64> {
        let taken = bytes.len().min(self.head.len() - self.have);
        self.head[self.have..][..taken].copy_from_slice(&bytes[..taken]);
        let mut cursor = Cursor::new(&self.head[..self.have + taken]);
        let Ok(Some((head, len))) = FrameHeader::parse(&mut cursor) else {
            self.have += taken;
            return None;
        };
        *bytes = &bytes[cursor.position() as usize - self.have..];
        self.have = 0;
        let control = matches!(head.opcode, OpCode::Control(_));
        self.unread += usize::from(head.is_final || control);
        if !control {
            self.open = !head.is_final;
            self.grown |= len > READ_BUFFER as u64;
        }
        Some(len)
    }

    /// True once per large frame, when the message handed back was the last thing tungstenite
    /// holds: no message-ending frame unread, no fragmented message open, no head cut short.
    fn handed_back(&mut self) -> bool {
        self.unread -= 1;
        let empty = self.unread == 0 && self.have == 0 && !self.open;
        empty && std::mem::take(&mut self.grown)
    }
}

/// Its own code back (1000 for none), or 1002 for one no endpoint may send: tungstenite hands
/// those over as its 1002 "Protocol violation", and lets 1012 and 1013 through.
fn closed(frame: Option<CloseFrame>) -> Ended {
    let code = frame.as_ref().map_or(1000, |frame| u16::from(frame.code));
    let replaced = frame.is_some_and(|frame| {
        frame.code == CloseCode::Protocol && frame.reason == "Protocol violation"
    });
    let (code, cause) = if replaced || !valid_close_code(code) {
        (1002, Cause::Protocol)
    } else {
        (code, Cause::Normal)
    };
    Ended {
        code: Some(code),
        cause,
    }
}

/// 1002 for a broken rule, 1007 for text not UTF-8, 1009 past the cap; a gone client hears nothing.
fn failed(error: &Error) -> Ended {
    let (code, cause) = match error {
        Error::Protocol(ProtocolError::ResetWithoutClosingHandshake) => (None, Cause::ClientGone),
        Error::Protocol(_) => (Some(1002), Cause::Protocol),
        Error::Utf8(_) => (Some(1007), Cause::Protocol),
        Error::Capacity(_) => (Some(1009), Cause::TooBig),
        _ => (None, Cause::ClientGone),
    };
    Ended { code, cause }
}

/// A socket's read half as std::io for tungstenite, within one poll:
/// Pending reads as WouldBlock, and each read is shown to `fed`. What
/// tungstenite writes (its own pongs and close echoes) goes nowhere.
struct Polled<'a, 'b, R> {
    rd: &'a mut R,
    cx: &'a mut Context<'b>,
    fed: &'a mut Fed,
}

impl<R: AsyncRead + Unpin> Read for Polled<'_, '_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut buf = ReadBuf::new(buf);
        match Pin::new(&mut *self.rd).poll_read(self.cx, &mut buf) {
            Poll::Ready(read) => read.map(|()| {
                self.fed.feed(buf.filled());
                buf.filled().len()
            }),
            Poll::Pending => Err(io::ErrorKind::WouldBlock.into()),
        }
    }
}

impl<R> Write for Polled<'_, '_, R> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncWriteExt;

    use super::*;
    use crate::ws::testing::client_frame;

    /// What a Reader capped at `max_message` yields for `wire`, sent by a
    /// client that then hangs up, up to how reading ended. One byte gets
    /// through at a time, so every read waits, and resumes, at each byte.
    async fn read(wire: Vec<u8>, max_message: usize) -> Vec<Result<Message, Ended>> {
        let (mut client, conn) = tokio::io::duplex(1);
        tokio::spawn(async move {
            let _ = client.write_all(&wire).await;
        });
        let mut reader = Reader::new(conn, max_message);
        let mut seen = Vec::new();
        loop {
            let next = reader.next().await;
            let ended = next.is_err();
            seen.push(next);
            if ended {
                return seen;
            }
        }
    }

    fn ended(code: Option<u16>, cause: Cause) -> Result<Message, Ended> {
        Err(Ended { code, cause })
    }

    /// A connection its client reset.
    struct Reset;

    impl AsyncRead for Reset {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context,
            _: &mut ReadBuf,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }
    }

    #[test]
    fn server_frames_are_unmasked_with_fin_set_and_the_shortest_length_form() {
        // RFC 6455 5.2: up to 125 bytes in the 7 bits, up to 65,535 as 126
        // and 16 bits, past that as 127 and 64 bits.
        let frames: [(Bytes, &[u8]); 5] = [
            (encode(Opcode::Text, b"Hello"), b"\x81\x05Hello"), // RFC 6455 5.7
            (encode(Opcode::Text, &[0; 125]), &[0x81, 125]),
            (encode(Opcode::Text, &[0; 126]), &[0x81, 126, 0, 126]),
            (
                encode(Opcode::Binary, &[0; 65_535]),
                &[0x82, 126, 0xFF, 0xFF],
            ),
            (
                encode(Opcode::Binary, &[0; 65_536]),
                &[0x82, 127, 0, 0, 0, 0, 0, 1, 0, 0],
            ),
        ];
        for (frame, head) in frames {
            assert_eq!(
                &frame[..head.len()],
                head,
                "a frame of {} bytes",
                frame.len()
            );
        }
    }

    #[tokio::test]
    async fn how_reading_ends_sets_the_close_code_and_the_cause() {
        use Cause::*;
        let close = |code: u16, reason: &[u8]| {
            client_frame(0x88, &[&code.to_be_bytes()[..], reason].concat())
        };
        let mut cut = client_frame(0x81, b"hello");
        cut.truncate(cut.len() - 2);
        // One row per case; rustfmt would split each row field by field.
        #[rustfmt::skip]
        let cases = [
            (cut, None, ClientGone, "a connection lost mid-frame"),
            // RFC 6455 5.1: a client must mask every frame.
            (b"\x81\x02hi".to_vec(), Some(1002), Protocol, "an unmasked frame"),
            (client_frame(0x81, &[0xC3, 0x28]), Some(1007), Protocol, "text not UTF-8"),
            (client_frame(0x82, &[0; 17]), Some(1009), TooBig, "past the cap"),
            (client_frame(0x88, b""), Some(1000), Normal, "a close with no code"),
            (close(4000, b"bye"), Some(4000), Normal, "a close with a code"),
            (close(1005, b""), Some(1002), Protocol, "a code for local use only"),
            (close(1012, b""), Some(1002), Protocol, "a code RFC 6455 does not define"),
        ];
        for (wire, code, cause, why) in cases {
            let ended = read(wire, 16).await.pop();
            assert_eq!(ended, Some(Err(Ended { code, cause })), "{why}");
        }
        let reset = Reader::new(Reset, 16).next().await;
        assert_eq!(reset, ended(None, ClientGone), "a connection reset");
    }

    #[tokio::test]
    async fn the_cap_counts_every_fragment_of_a_message() {
        // A control frame may carry 125 bytes whatever the cap, and counts
        // toward no message.
        let mut wire = client_frame(0x02, &[7; 10]);
        wire.extend(client_frame(0x89, &[9; 125]));
        wire.extend(client_frame(0x80, &[7; 6]));
        wire.extend(client_frame(0x02, &[7; 10]));
        wire.extend(client_frame(0x80, &[7; 7]));
        assert_eq!(
            read(wire, 16).await,
            [
                Ok(Message::Ping(Bytes::from(vec![9; 125]))),
                Ok(Message::Binary(Bytes::from(vec![7; 16]))),
                ended(Some(1009), Cause::TooBig)
            ]
        );
    }

    #[tokio::test]
    async fn a_large_message_is_let_go_of_once_nothing_is_read_past_it() {
        // tungstenite reads a frame whole into its buffer and hands the
        // payload over in place, so a message shares the reader's buffer.
        let large = client_frame(0x82, &vec![7; 512 * 1024]);
        // 200 bytes: a head of 8, its length in 16 bits.
        let small = client_frame(0x82, &[5; 200]);
        let (mut client, conn) = tokio::io::duplex(1 << 20);
        let mut reader = Reader::new(conn, 1 << 20);
        let mut next = async || reader.next().await.expect("a message");
        let binary = |message| match message {
            Message::Binary(data) => data,
            other => panic!("expected a binary message, got {other:?}"),
        };

        client.write_all(&small).await.unwrap();
        let alone = binary(next().await);
        assert!(
            !alone.is_unique(),
            "with nothing large read, nothing is let go of"
        );
        client.write_all(&large).await.unwrap();
        let alone = binary(next().await);
        assert!(alone.is_unique(), "the reader still holds a large message");

        // Read past it: a message, a head cut short, a payload cut short.
        for cut in [small.len(), 3, 9] {
            let wire = [&large[..], &small[..cut]].concat();
            client.write_all(&wire).await.unwrap();
            let ahead = binary(next().await);
            assert!(!ahead.is_unique(), "let go of with {cut} bytes past it");
            client.write_all(&small[cut..]).await.unwrap();
            let behind = binary(next().await);
            assert_eq!(behind, &[5; 200][..]);
            assert!(!behind.is_unique(), "a small message is read in place");
            drop(behind);
            assert!(ahead.is_unique(), "the reader still holds a large message");
        }

        // A pong is a message tungstenite hands back too.
        let wire = [large, client_frame(0x8A, b"pong")].concat();
        client.write_all(&wire).await.unwrap();
        let ahead = binary(next().await);
        assert!(!ahead.is_unique(), "let go of with a pong past it");
        assert_eq!(next().await, Message::Pong(Bytes::from_static(b"pong")));
        assert!(ahead.is_unique(), "the reader still holds a large message");
    }

    #[tokio::test]
    async fn a_ping_between_the_fragments_of_a_large_message_leaves_it_whole() {
        let mut wire = client_frame(0x02, &[7; 8 * 1024]);
        wire.extend(client_frame(0x89, b"ping"));
        wire.extend(client_frame(0x80, &[7; 10]));
        assert_eq!(
            read(wire, 1 << 20).await,
            [
                Ok(Message::Ping(Bytes::from_static(b"ping"))),
                Ok(Message::Binary(Bytes::from(vec![7; 8 * 1024 + 10]))),
                ended(None, Cause::ClientGone)
            ]
        );
    }

    #[tokio::test]
    async fn a_huge_declared_length_is_refused_before_any_payload_is_read() {
        // No payload follows: reading it would find the client gone (or
        // allocate 2^63 bytes).
        let mut wire = vec![0x82, 0x80 | 127];
        wire.extend_from_slice(&(1u64 << 63).to_be_bytes());
        wire.extend_from_slice(&[0; 4]);
        assert_eq!(read(wire, 1024).await, [ended(Some(1009), Cause::TooBig)]);
    }
}
