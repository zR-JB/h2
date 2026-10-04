use bytes::Bytes;
use http::Response;
use std::convert::TryInto;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn frame(socket: &mut TcpStream, kind: u8, flags: u8, stream: u32, payload: &[u8]) {
    let length = payload.len() as u32;
    let mut header = [0; 9];
    header[..3].copy_from_slice(&length.to_be_bytes()[1..]);
    header[3] = kind;
    header[4] = flags;
    header[5..].copy_from_slice(&stream.to_be_bytes());
    socket.write_all(&header).await.unwrap();
    socket.write_all(payload).await.unwrap();
}

async fn next(socket: &mut TcpStream) -> (u8, u32, Vec<u8>) {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await.unwrap();
    let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
    let mut payload = vec![0; length];
    socket.read_exact(&mut payload).await.unwrap();
    (
        header[3],
        u32::from_be_bytes(header[5..].try_into().unwrap()),
        payload,
    )
}

async fn server(
    hold_closed: bool,
) -> (
    TcpStream,
    tokio::task::JoinHandle<()>,
    tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (clear, mut clears) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<()>>(1);
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        socket.set_nodelay(true).unwrap();
        let mut builder = h2::server::Builder::new();
        builder
            .initial_window_size(8 * 1024 * 1024)
            .initial_connection_window_size(16 * 1024 * 1024)
            .max_concurrent_streams(250)
            .max_header_list_size(32 * 1024)
            .max_send_buffer_size(16 * 1024)
            .data_frame_budget(1024);
        let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
        let mut held = Vec::new();
        loop {
            let result = tokio::select! {
                result = connection.accept() => match result { Some(result) => result, None => break },
                Some(done) = clears.recv() => {
                    held.clear();
                    done.send(()).unwrap();
                    continue;
                }
            };
            let Ok((request, mut respond)) = result else {
                break;
            };
            let mut body = request.into_body();
            if hold_closed || body.stream_id().as_u32() == 1 {
                if hold_closed {
                    respond.send_response(Response::new(()), true).unwrap();
                }
                held.push((body, respond));
            } else {
                let mut send = respond.send_response(Response::new(()), false).unwrap();
                tokio::spawn(async move {
                    while let Some(Ok(data)) = body.data().await {
                        body.flow_control().release_capacity(data.len()).unwrap();
                        send.send_data(Bytes::from_static(b"ack"), false).unwrap();
                    }
                });
            }
        }
    });
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket.set_nodelay(true).unwrap();
    socket
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
        .await
        .unwrap();
    frame(&mut socket, 4, 0, 0, &[]).await;
    for stream in if hold_closed { vec![] } else { vec![1, 3] } {
        frame(&mut socket, 1, 4, stream, b"\x83\x86\x84\x01\x09localhost").await;
    }
    (socket, task, clear)
}

async fn acknowledgment(socket: &mut TcpStream) -> Option<u32> {
    loop {
        let (kind, stream, payload) = next(socket).await;
        if kind == 7 {
            return Some(u32::from_be_bytes(payload[4..8].try_into().unwrap()));
        }
        if kind == 0 && stream == 3 {
            return None;
        }
    }
}

#[tokio::test]
async fn draining_large_frames_cannot_refund_another_streams_unread_events() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (mut socket, task, _clear) = server(false).await;
        let mut refused = false;
        for _ in 0..10 {
            frame(&mut socket, 0, 0, 1, b"x").await;
            frame(&mut socket, 0, 0, 3, &[0; 16 * 1024]).await;
            if let Some(reason) = acknowledgment(&mut socket).await {
                assert_eq!(reason, u32::from(h2::Reason::ENHANCE_YOUR_CALM));
                refused = true;
                break;
            }
        }
        assert!(
            refused,
            "drained payloads must not replenish unread event charges"
        );
        task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn consumed_full_frames_reuse_event_budget_across_wire_window_updates() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let (mut socket, task, _clear) = server(false).await;
        for _ in 0..2048 {
            frame(&mut socket, 0, 0, 3, &[0; 16 * 1024]).await;
            assert_eq!(acknowledgment(&mut socket).await, None);
        }
        task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn unread_trailers_retain_the_configured_stream_slots() {
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        let (mut socket, task, clear) = server(true).await;
        for stream in (1..=499).step_by(2) {
            frame(&mut socket, 1, 4, stream, b"\x83\x86\x84\x01\x09localhost").await;
            frame(&mut socket, 1, 5, stream, &[]).await;
            loop {
                let (kind, id, _) = next(&mut socket).await;
                assert_ne!(kind, 7);
                if kind == 1 && id == stream {
                    break;
                }
            }
        }
        frame(&mut socket, 1, 4, 501, b"\x83\x86\x84\x01\x09localhost").await;
        loop {
            let (kind, stream, payload) = next(&mut socket).await;
            assert_ne!(kind, 7);
            if kind == 3 && stream == 501 {
                assert_eq!(
                    u32::from_be_bytes(payload.try_into().unwrap()),
                    u32::from(h2::Reason::REFUSED_STREAM)
                );
                break;
            }
            assert!(
                !(kind == 1 && stream == 501),
                "closed unread trailers must retain their stream slot"
            );
        }
        let (done, completed) = tokio::sync::oneshot::channel();
        clear.send(done).await.unwrap();
        completed.await.unwrap();
        frame(&mut socket, 1, 5, 503, b"\x83\x86\x84\x01\x09localhost").await;
        loop {
            let (kind, stream, _) = next(&mut socket).await;
            assert!(
                !(kind == 3 && stream == 503),
                "dropping unread trailers must release their stream slot"
            );
            if kind == 1 && stream == 503 {
                break;
            }
        }
        task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn empty_final_data_events_consume_budget() {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let (mut socket, task, _clear) = server(true).await;
        for stream in (1..=9).step_by(2) {
            frame(&mut socket, 1, 4, stream, b"\x83\x86\x84\x01\x09localhost").await;
            frame(&mut socket, 0, 1, stream, &[]).await;
        }
        loop {
            let (kind, _, payload) = next(&mut socket).await;
            if kind == 7 {
                assert_eq!(
                    u32::from_be_bytes(payload[4..8].try_into().unwrap()),
                    u32::from(h2::Reason::ENHANCE_YOUR_CALM)
                );
                break;
            }
        }
        task.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn tiny_send_events_backpressure_and_refund_with_one_byte_window_updates() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (fill, mut filled) = tokio::sync::oneshot::channel();
        let (complete, completed) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            socket.set_nodelay(true).unwrap();
            let mut builder = h2::server::Builder::new();
            builder
                .initial_window_size(8 * 1024 * 1024)
                .initial_connection_window_size(16 * 1024 * 1024)
                .max_concurrent_streams(250)
                .max_header_list_size(32 * 1024)
                .max_send_buffer_size(16 * 1024);
            let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
            let (_, mut reply) = connection.accept().await.unwrap().unwrap();
            let mut send = reply.send_response(Response::new(()), false).unwrap();
            send.reserve_capacity(16 * 1024);
            loop {
                tokio::select! {
                    result = connection.accept() => { assert!(result.is_none()); },
                    _ = &mut filled => break,
                }
            }
            let mut queued = 0;
            while send.capacity() > 0 && queued < 100 {
                send.send_data(Bytes::from_static(b"x"), false).unwrap();
                queued += 1;
            }
            assert!(
                queued > 0 && queued < 100,
                "metadata must backpressure below the payload watermark"
            );
            assert_eq!(send.capacity(), 0);
            assert!(send.send_data(Bytes::from_static(b"x"), false).is_err());
            let producer = tokio::spawn(async move {
                for _ in queued..1024 {
                    send.reserve_capacity(1);
                    if send.capacity() == 0 {
                        std::future::poll_fn(|cx| send.poll_capacity(cx))
                            .await
                            .unwrap()
                            .unwrap();
                    }
                    send.send_data(Bytes::from_static(b"x"), false).unwrap();
                }
                send.send_data(Bytes::new(), true).unwrap();
                complete.send(()).unwrap();
            });
            while connection.accept().await.is_some() {}
            producer.abort();
        });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.set_nodelay(true).unwrap();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        frame(&mut socket, 4, 0, 0, &[0, 4, 0, 0, 0, 1]).await;
        frame(&mut socket, 1, 5, 1, b"\x83\x86\x84\x01\x09localhost").await;
        for _ in 0..100 {
            frame(&mut socket, 8, 0, 1, &[0, 0, 0, 1]).await;
        }
        frame(&mut socket, 6, 0, 0, b"ready!!!").await;
        loop {
            let (kind, _, payload) = next(&mut socket).await;
            if kind == 6 && payload == b"ready!!!" {
                break;
            }
        }
        fill.send(()).unwrap();
        let mut received = 0;
        while received < 1024 {
            let (kind, stream, payload) = next(&mut socket).await;
            assert_ne!(kind, 7);
            if kind == 0 && stream == 1 && !payload.is_empty() {
                assert_eq!(payload, b"x");
                received += 1;
                frame(&mut socket, 8, 0, 1, &[0, 0, 0, 1]).await;
            }
        }
        completed.await.unwrap();
        server.abort();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn header_metadata_refusal_preserves_ordinary_fields_and_hpack_siblings() {
    use http::{header::HeaderName, HeaderMap};
    use std::hash::{Hash, Hasher};

    struct Fnv(u64);
    impl Hasher for Fnv {
        fn finish(&self) -> u64 {
            self.0
        }
        fn write(&mut self, bytes: &[u8]) {
            for byte in bytes {
                self.0 = (self.0 ^ u64::from(*byte)).wrapping_mul(0x100000001b3);
            }
        }
    }
    let mut hostile = HeaderMap::new();
    let mut names: Vec<String> = (0..205).map(|i| format!("h{i:03x}")).collect();
    for name in &names {
        hostile.append(
            name.parse::<HeaderName>().unwrap(),
            http::HeaderValue::from_static(""),
        );
    }
    for i in 0u32.. {
        let name: HeaderName = format!("x{i:x}").parse().unwrap();
        let mut hash = Fnv(0xcbf29ce484222325);
        name.hash(&mut hash);
        if hash.finish() & 4095 == 0 {
            names.push(name.as_str().to_owned());
            hostile.append(name, http::HeaderValue::from_static(""));
            if hostile.capacity() > 1536 {
                break;
            }
        }
        assert!(i < 10_000_000, "collision fixture failed to grow");
    }
    assert!(names.iter().map(|name| name.len() + 32).sum::<usize>() < 32 * 1024 - 200);
    let literal = |block: &mut Vec<u8>, name: &str| {
        assert!(name.len() < 127);
        block.extend_from_slice(&[0, name.len() as u8]);
        block.extend_from_slice(name.as_bytes());
        block.push(0);
    };
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, mut observed) = tokio::sync::mpsc::channel(8);
        let (trailer_done, trailer_end) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut builder = h2::server::Builder::new();
            builder
                .initial_window_size(8 * 1024 * 1024)
                .initial_connection_window_size(16 * 1024 * 1024)
                .max_concurrent_streams(250)
                .max_header_list_size(32 * 1024);
            let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
            let mut trailer_done = Some(trailer_done);
            while let Some(result) = connection.accept().await {
                let (request, mut reply) = result.unwrap();
                let id = request.body().stream_id().as_u32();
                assert!(request.headers().capacity() <= 1536);
                accepted
                    .send((
                        id,
                        request.headers().len(),
                        request.headers().get("x-sync").cloned(),
                    ))
                    .await
                    .unwrap();
                if id == 9 {
                    let mut body = request.into_body();
                    let done = trailer_done.take().unwrap();
                    tokio::spawn(async move {
                        done.send(body.trailers().await.unwrap_err().reason())
                            .unwrap();
                    });
                } else {
                    reply.send_response(Response::new(()), true).unwrap();
                }
            }
        });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        frame(&mut socket, 4, 0, 0, &[]).await;
        for (id, duplicate) in [(1, false), (3, true)] {
            let mut block = b"\x82\x87\x84\x01\x09localhost".to_vec();
            for i in 0..if duplicate { 980 } else { 880 } {
                literal(
                    &mut block,
                    &if duplicate {
                        "a".into()
                    } else {
                        format!("h{i:03x}")
                    },
                );
            }
            frame(&mut socket, 1, 5, id, &block).await;
            let (seen, count, _) = observed.recv().await.unwrap();
            assert_eq!((seen, count), (id, if duplicate { 980 } else { 880 }));
        }
        let mut block = b"\x82\x87\x84\x01\x09localhost".to_vec();
        for name in &names {
            literal(&mut block, name);
        }
        block.extend_from_slice(b"\x40\x06x-sync\x01v");
        frame(&mut socket, 1, 4, 5, &block).await;
        frame(&mut socket, 1, 5, 7, b"\x82\x87\x84\x01\x09localhost\xbe").await;
        let (seen, count, sync) = observed.recv().await.unwrap();
        assert_eq!((seen, count), (7, 1));
        assert_eq!(sync.unwrap(), "v");
        let mut refused = false;
        while !refused {
            let (kind, id, payload) = next(&mut socket).await;
            assert_ne!(kind, 7, "metadata refusal must remain stream-local");
            if kind == 3 && id == 5 {
                assert_eq!(payload, u32::from(h2::Reason::PROTOCOL_ERROR).to_be_bytes());
                refused = true;
            }
        }
        frame(&mut socket, 1, 4, 9, b"\x82\x87\x84\x01\x09localhost").await;
        assert_eq!(observed.recv().await.unwrap().0, 9);
        let mut trailers = Vec::new();
        for name in &names {
            literal(&mut trailers, name);
        }
        trailers.extend_from_slice(b"\x40\x06x-sync\x01w");
        frame(&mut socket, 1, 5, 9, &trailers).await;
        frame(&mut socket, 1, 5, 11, b"\x82\x87\x84\x01\x09localhost\xbe").await;
        let (seen, _, sync) = observed.recv().await.unwrap();
        assert_eq!(seen, 11);
        assert_eq!(sync.unwrap(), "w");
        assert_eq!(trailer_end.await.unwrap(), Some(h2::Reason::PROTOCOL_ERROR));
        task.abort();
    })
    .await
    .unwrap();
}

#[derive(Debug)]
struct Meter {
    limit: usize,
    used: std::sync::atomic::AtomicUsize,
    peak: std::sync::atomic::AtomicUsize,
}

impl Meter {
    fn new(limit: usize) -> std::sync::Arc<Self> {
        std::sync::Arc::new(Meter {
            limit,
            used: Default::default(),
            peak: Default::default(),
        })
    }

    fn used(&self) -> usize {
        self.used.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl h2::SharedBudget for Meter {
    #[allow(deprecated)] // try_update needs Rust 1.99; the crate supports 1.63
    fn try_charge(&self, bytes: usize) -> bool {
        use std::sync::atomic::Ordering::SeqCst;
        let limit = self.limit;
        match self.used.fetch_update(SeqCst, SeqCst, |used| {
            Some(used + bytes).filter(|used| *used <= limit)
        }) {
            Ok(used) => {
                self.peak.fetch_max(used + bytes, SeqCst);
                true
            }
            Err(_) => false,
        }
    }

    fn refund(&self, bytes: usize) {
        self.used
            .fetch_sub(bytes, std::sync::atomic::Ordering::SeqCst);
    }
}

async fn until_reset(socket: &mut TcpStream, id: u32) -> h2::Reason {
    loop {
        let (kind, stream, payload) = next(socket).await;
        assert_ne!(kind, 7, "a refused charge must remain stream-local");
        assert!(!(kind == 1 && stream == id), "a refusal must not answer");
        if kind == 3 && stream == id {
            return u32::from_be_bytes(payload.try_into().unwrap()).into();
        }
    }
}

#[tokio::test]
async fn header_and_data_floods_stay_within_the_connection_allowance() {
    const CAP: usize = 64 * 1024;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let meter = Meter::new(usize::MAX);
        let budget = meter.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, mut observed) = tokio::sync::mpsc::channel(64);
        let (clear, mut clears) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<bool>>(1);
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut builder = h2::server::Builder::new();
            builder
                .max_concurrent_streams(250)
                .max_header_list_size(32 * 1024)
                .data_frame_budget(1 << 20)
                .shared_budget(budget, CAP);
            let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
            let mut held = Vec::new();
            let mut response = None;
            loop {
                tokio::select! {
                    result = connection.accept() => {
                        let (request, mut reply) = match result { Some(Ok(accepted)) => accepted, _ => break };
                        let id = request.body().stream_id().as_u32();
                        accepted.send((id, request.headers().get("x-sync").cloned())).await.unwrap();
                        if id == 3 {
                            response = Some(reply.send_response(Response::new(()), false).unwrap());
                        }
                        held.push((request, reply));
                    }
                    Some(done) = clears.recv() => {
                        let finished = response.take().unwrap().send_data(Bytes::new(), true);
                        held.clear();
                        done.send(finished.is_ok()).unwrap();
                    }
                }
            }
        });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        frame(&mut socket, 4, 0, 0, &[]).await;
        let head = |sync: bool| {
            let mut block = b"\x82\x87\x84\x01\x09localhost".to_vec();
            for i in 0..20u8 {
                block.extend_from_slice(&[0, 2, b'x', b'a' + i, 16]);
                block.extend_from_slice(&[b'v'; 16]);
            }
            if sync {
                block.extend_from_slice(b"\x40\x06x-sync\x01v");
            }
            block
        };
        let mut id = 1;
        let refused = loop {
            frame(&mut socket, 1, 4, id, &head(false)).await;
            frame(&mut socket, 6, 0, 0, b"accepted").await;
            let mut replies = Vec::new();
            loop {
                let (kind, stream, payload) = next(&mut socket).await;
                assert_ne!(kind, 7);
                if kind == 6 && payload == b"accepted" {
                    break;
                }
                if stream == id {
                    replies.push((kind, payload));
                }
            }
            match observed.try_recv() {
                Ok((stream, _)) => assert_eq!(stream, id),
                Err(_) => {
                    let refusal = u32::from(h2::Reason::REFUSED_STREAM).to_be_bytes();
                    assert_eq!(replies, [(3, refusal.to_vec())], "pressure is retryable");
                    break id;
                }
            }
            id += 2;
        };
        assert!(refused > 5, "ordinary requests must fit the cap");
        for _ in 0..CAP {
            frame(&mut socket, 0, 0, 1, b"x").await;
        }
        assert_eq!(until_reset(&mut socket, 1).await, h2::Reason::ENHANCE_YOUR_CALM);
        frame(&mut socket, 1, 4, refused + 2, &head(true)).await;
        assert_eq!(until_reset(&mut socket, refused + 2).await, h2::Reason::REFUSED_STREAM);
        frame(&mut socket, 1, 5, 5, b"\x00\x02xa\x01v").await;
        assert_eq!(until_reset(&mut socket, 5).await, h2::Reason::ENHANCE_YOUR_CALM);
        let mut oversize = b"\x82\x87\x84\x01\x09localhost".to_vec();
        for _ in 0..40 {
            oversize.extend_from_slice(b"\x00\x02xa\x7f\xe9\x06");
            oversize.extend_from_slice(&[b'v'; 1000]);
        }
        for (i, block) in oversize.chunks(16 * 1024).enumerate() {
            let last = (i + 1) * 16 * 1024 >= oversize.len();
            let kind = if i == 0 { 1 } else { 9 };
            frame(&mut socket, kind, if last { 4 } else { 0 }, refused + 4, block).await;
        }
        let mut answered = false;
        let reset = loop {
            let (kind, stream, payload) = next(&mut socket).await;
            assert_ne!(kind, 7);
            answered |= kind == 1 && stream == refused + 4;
            if kind == 3 && stream == refused + 4 {
                break payload;
            }
        };
        assert!(answered, "oversize headers keep their 431 under pressure");
        assert_eq!(reset, u32::from(h2::Reason::PROTOCOL_ERROR).to_be_bytes());
        assert_eq!(meter.peak.load(std::sync::atomic::Ordering::SeqCst), 0);

        let (done, cleared) = tokio::sync::oneshot::channel();
        clear.send(done).await.unwrap();
        assert!(cleared.await.unwrap(), "an empty final frame needs no allowance");
        while next(&mut socket).await != (0, 3, Vec::new()) {}
        frame(&mut socket, 1, 5, refused + 6, b"\x82\x87\x84\x01\x09localhost\xbe").await;
        let (stream, sync) = observed.recv().await.unwrap();
        assert_eq!(stream, refused + 6);
        assert_eq!(sync.unwrap(), "v");
        drop(socket);
        task.await.unwrap();
        assert_eq!(meter.used(), 0);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ended_connections_refund_window_credit_that_live_streams_hold() {
    const GROWTH: usize = 128 * 1024;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for fault in [false, true] {
            let meter = Meter::new(1 << 20);
            let budget = meter.clone();
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let task = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut builder = h2::server::Builder::new();
                builder.shared_budget(budget.clone(), 64 * 1024);
                let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
                let (request, _reply) = connection.accept().await.unwrap().unwrap();
                let mut body = request.into_body();
                let flow = body.flow_control();
                let target = (65_535 + GROWTH) as u32;
                assert!(flow.set_target_connection_window_size(target));
                assert_eq!(budget.used(), GROWTH);
                while let Some(Ok(_)) = connection.accept().await {}
                flow.set_target_connection_window_size(target);
                assert_eq!(budget.used(), 0, "held streams kept ended credit");
            });
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket
                .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
                .await
                .unwrap();
            frame(&mut socket, 4, 0, 0, &[]).await;
            frame(&mut socket, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost").await;
            while next(&mut socket).await.0 != 8 {}
            if fault {
                frame(&mut socket, 0, 0, 0, b"x").await;
            } else {
                socket.shutdown().await.unwrap();
            }
            task.await.unwrap();
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn receive_window_growth_is_funded_and_refunded_as_peer_credit_drains() {
    const GROWTH: usize = 128 * 1024;
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let meter = Meter::new(1 << 20);
        let budget = meter.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (report, mut reports) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut builder = h2::server::Builder::new();
            builder
                .initial_window_size(1 << 20)
                .shared_budget(budget.clone(), 64 * 1024);
            let mut connection = builder.handshake::<_, Bytes>(socket).await.unwrap();
            let (request, _reply) = connection.accept().await.unwrap().unwrap();
            let reader = tokio::spawn(async move {
                let mut body = request.into_body();
                let base = budget.used();
                let flow = body.flow_control();
                assert!(!flow.set_target_connection_window_size(65_536 + (1 << 20)));
                assert!(flow.set_target_connection_window_size((65_535 + GROWTH) as u32));
                assert_eq!(budget.used(), base + GROWTH);
                let first = body.data().await.unwrap().unwrap();
                assert!(body
                    .flow_control()
                    .set_target_connection_window_size(65_535));
                body.flow_control().release_capacity(first.len()).unwrap();
                report.send(budget.used() - base).unwrap();
                while let Some(data) = body.data().await {
                    body.flow_control()
                        .release_capacity(data.unwrap().len())
                        .unwrap();
                }
                report.send(budget.used() - base).unwrap();
            });
            while connection.accept().await.is_some() {}
            reader.await.unwrap();
        });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket
            .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")
            .await
            .unwrap();
        frame(&mut socket, 4, 0, 0, &[]).await;
        frame(&mut socket, 4, 1, 0, &[]).await;
        frame(&mut socket, 1, 4, 1, b"\x83\x86\x84\x01\x09localhost").await;
        loop {
            let (kind, stream, payload) = next(&mut socket).await;
            if kind == 8 && stream == 0 {
                assert_eq!(
                    u32::from_be_bytes(payload.try_into().unwrap()) as usize,
                    GROWTH
                );
                break;
            }
        }
        let chunk = [0; 16 * 1024];
        frame(&mut socket, 0, 0, 1, &chunk).await;
        let held = reports.recv().await.unwrap();
        assert!(
            held >= GROWTH - chunk.len(),
            "lowering must not refund unused peer credit"
        );
        let mut sent = chunk.len();
        while sent + chunk.len() <= 65_535 + GROWTH {
            frame(&mut socket, 0, 0, 1, &chunk).await;
            sent += chunk.len();
        }
        frame(&mut socket, 0, 1, 1, &chunk[..65_535 + GROWTH - sent]).await;
        assert_eq!(reports.recv().await.unwrap(), 0);
        drop(socket);
        task.await.unwrap();
        assert_eq!(meter.used(), 0);
    })
    .await
    .unwrap();
}
