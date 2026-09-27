use bytes::Bytes;
use http::Response;
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
