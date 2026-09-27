use super::*;

pub(super) async fn enqueue_response(
    sender: &mpsc::Sender<QueuedNativeResponse>,
    response: NativeFrame,
    connection_budget: Arc<Semaphore>,
    global_budget: Arc<Semaphore>,
    reserved_global: Option<OwnedSemaphorePermit>,
    maximum_connection_bytes: usize,
) -> io::Result<()> {
    let response = bounded_response(response, maximum_connection_bytes);
    let bytes = response
        .payload
        .len()
        .saturating_add(NATIVE_FRAME_HEADER_BYTES);
    let bytes = u32::try_from(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "native response exceeds outbound byte budget",
        )
    })?;
    let connection_bytes = connection_budget
        .acquire_many_owned(bytes)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
    let global_bytes = match reserved_global {
        Some(permit) => permit,
        None => global_budget
            .acquire_many_owned(bytes)
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?,
    };
    sender
        .send(QueuedNativeResponse {
            frame: response,
            _connection_bytes: connection_bytes,
            _global_bytes: global_bytes,
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native writer stopped"))
}

pub(super) fn bounded_response(
    response: NativeFrame,
    maximum_connection_bytes: usize,
) -> NativeFrame {
    if response
        .payload
        .len()
        .saturating_add(NATIVE_FRAME_HEADER_BYTES)
        <= maximum_connection_bytes
    {
        return response;
    }
    error_frame(
        response.header,
        NativeStatus::TooManyRequests,
        "native response exceeds the configured outbound byte budget",
    )
}

/// Serves multiplexed native connections until `shutdown` resolves.
///
/// Responses may complete out of order and are correlated by the request ID
/// copied from each request frame.
pub async fn serve_native<F>(
    listener: TcpListener,
    store: Arc<DurableTelemetryStore>,
    config: NativeServerConfig,
    shutdown: F,
) -> io::Result<()>
where
    F: Future<Output = ()>,
{
    let config = config.validate()?;
    let outbound_budget = Arc::new(Semaphore::new(config.max_outbound_bytes_total));
    let inbound_budget = Arc::new(Semaphore::new(config.max_inbound_bytes_total));
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (socket, peer) = accepted?;
                if config.production.is_some()
                    && !config.allow_insecure_remote
                    && !peer.ip().is_loopback()
                {
                    continue;
                }
                socket.set_nodelay(true)?;
                let store = Arc::clone(&store);
                let config = config.clone();
                let outbound_budget = Arc::clone(&outbound_budget);
                let inbound_budget = Arc::clone(&inbound_budget);
                let connection_permit = match &config.production {
                    Some(runtime) => match runtime.try_native_connection() {
                        Some(permit) => Some(permit),
                        None => continue,
                    },
                    None => None,
                };
                tokio::spawn(async move {
                    let _connection_permit = connection_permit;
                    if let Err(error) = serve_connection(
                        socket,
                        store,
                        config,
                        outbound_budget,
                        inbound_budget,
                    ).await
                        && error.kind() != io::ErrorKind::UnexpectedEof
                        && error.kind() != io::ErrorKind::ConnectionReset
                    {
                        eprintln!("native ShardTelemetry connection failed: {error}");
                    }
                });
            }
        }
    }
}

pub(super) async fn serve_connection(
    socket: TcpStream,
    store: Arc<DurableTelemetryStore>,
    config: NativeServerConfig,
    outbound_budget: Arc<Semaphore>,
    inbound_budget: Arc<Semaphore>,
) -> io::Result<()> {
    let (mut reader, mut writer) = socket.into_split();
    let (responses, mut response_receiver) =
        mpsc::channel::<QueuedNativeResponse>(config.max_in_flight_per_connection);
    let write_timeout = config.write_timeout;
    let writer_task = tokio::spawn(async move {
        while let Some(response) = response_receiver.recv().await {
            let write = async {
                writer.write_all(&response.frame.header.encode()).await?;
                writer.write_all(&response.frame.payload).await
            };
            tokio::time::timeout(write_timeout, write)
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "native response write timed out")
                })??;
        }
        writer.shutdown().await
    });
    let permits = Arc::new(Semaphore::new(config.max_in_flight_per_connection));
    let connection_outbound = Arc::new(Semaphore::new(config.max_outbound_bytes_per_connection));
    let mut authenticated = config.production.is_none();
    let mut shutdown = config
        .production
        .as_ref()
        .map(|runtime| runtime.lifecycle().subscribe_shutdown());
    let authentication_deadline = config
        .production
        .as_ref()
        .map(|runtime| tokio::time::Instant::now() + runtime.native_auth_timeout());

    loop {
        let mut encoded_header = [0; NATIVE_FRAME_HEADER_BYTES];
        if shutdown.as_ref().is_some_and(|shutdown| *shutdown.borrow()) {
            break;
        }
        let read = reader.read_exact(&mut encoded_header);
        let read_result = match (shutdown.as_mut(), authenticated) {
            (Some(shutdown), false) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                result = read => result,
            },
            (Some(shutdown), true) => tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                result = read => result,
            },
            (None, _) => read.await,
        };
        match read_result {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        }
        let header = NativeFrameHeader::decode(&encoded_header).map_err(invalid_data)?;
        if header.payload_len as usize > config.max_frame_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "native frame exceeds the configured connection limit",
            ));
        }
        let inbound_permit = if header.payload_len == 0 {
            None
        } else {
            Some(
                Arc::clone(&inbound_budget)
                    .try_acquire_many_owned(header.payload_len)
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "native inbound byte budget is exhausted",
                        )
                    })?,
            )
        };
        let mut payload = vec![0; header.payload_len as usize];
        if !authenticated {
            match shutdown.as_mut() {
                Some(shutdown) => tokio::select! {
                    biased;
                    _ = shutdown.changed() => break,
                    _ = tokio::time::sleep_until(authentication_deadline.expect("production deadline")) => break,
                    result = reader.read_exact(&mut payload) => { result?; }
                },
                None => reader.read_exact(&mut payload).await.map(|_| ())?,
            }
        } else {
            reader.read_exact(&mut payload).await?;
        }
        let payload_hash = header
            .verify_payload_and_hash(&payload)
            .map_err(invalid_data)?;
        if !authenticated {
            let response = authenticate_frame(header, &payload, &config);
            authenticated = response.header.status == NativeStatus::Ok;
            enqueue_response(
                &responses,
                response,
                Arc::clone(&connection_outbound),
                Arc::clone(&outbound_budget),
                None,
                config.max_outbound_bytes_per_connection,
            )
            .await?;
            if !authenticated {
                break;
            }
            continue;
        }
        let permit = Arc::clone(&permits)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "native server stopped"))?;
        let responses = responses.clone();
        let store = Arc::clone(&store);
        let request_config = config.clone();
        let connection_outbound = Arc::clone(&connection_outbound);
        let outbound_budget = Arc::clone(&outbound_budget);
        tokio::spawn(async move {
            let _inbound_permit = inbound_permit;
            // Reserve a whole per-connection response window before executing
            // a query. This bounds work whose exact encoded result size is not
            // known until after stripe execution completes.
            let query_outbound = if matches!(
                header.opcode,
                NativeOpcode::Query | NativeOpcode::QueryMetrics | NativeOpcode::QueryTraces
            ) {
                match outbound_budget
                    .clone()
                    .acquire_many_owned(request_config.max_outbound_bytes_per_connection as u32)
                    .await
                {
                    Ok(permit) => Some(permit),
                    Err(_) => return,
                }
            } else {
                None
            };
            let response = if header.is_response || header.status != NativeStatus::Ok {
                error_frame(
                    header,
                    NativeStatus::BadRequest,
                    "clients must send request frames with status OK",
                )
            } else {
                dispatch(header, payload, payload_hash, store, &request_config).await
            };
            let _ = enqueue_response(
                &responses,
                response,
                connection_outbound,
                outbound_budget,
                query_outbound,
                request_config.max_outbound_bytes_per_connection,
            )
            .await;
            drop(permit);
        });
    }

    drop(responses);
    writer_task
        .await
        .map_err(|error| io::Error::other(format!("native writer task failed: {error}")))?
}

pub(super) fn authenticate_frame(
    header: NativeFrameHeader,
    payload: &[u8],
    config: &NativeServerConfig,
) -> NativeFrame {
    if header.is_response || header.status != NativeStatus::Ok {
        return error_frame(
            header,
            NativeStatus::BadRequest,
            "authentication must be a request frame with status OK",
        );
    }
    if header.opcode != NativeOpcode::Authenticate {
        if let Some(runtime) = &config.production {
            runtime.record_authentication_failure();
        }
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "authenticate before sending native operations",
        );
    }
    let Some(runtime) = &config.production else {
        return ok_frame(header, Vec::new());
    };
    let Ok(token) = std::str::from_utf8(payload) else {
        runtime.record_authentication_failure();
        return error_frame(
            header,
            NativeStatus::Unauthorized,
            "native credential is not valid UTF-8",
        );
    };
    if runtime.authenticates(token) {
        ok_frame(header, Vec::new())
    } else {
        error_frame(
            header,
            NativeStatus::Unauthorized,
            "invalid native production credential",
        )
    }
}
