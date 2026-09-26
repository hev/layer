//! Propagate transport closure to queued/running request cancellation.
use axum::{
    extract::connect_info::Connected,
    serve::{IncomingStream, Listener},
};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    sync::watch,
};

pub struct TrackingListener(pub TcpListener);
pub struct TrackedStream {
    stream: TcpStream,
    closed: watch::Sender<bool>,
}
#[derive(Clone, Debug)]
pub struct Disconnect(pub watch::Receiver<bool>);
impl Connected<IncomingStream<'_, TrackingListener>> for Disconnect {
    fn connect_info(stream: IncomingStream<'_, TrackingListener>) -> Self {
        Self(stream.io().closed.subscribe())
    }
}
impl Listener for TrackingListener {
    type Io = TrackedStream;
    type Addr = SocketAddr;
    async fn accept(&mut self) -> (TrackedStream, SocketAddr) {
        let (stream, address) = Listener::accept(&mut self.0).await;
        let (closed, _) = watch::channel(false);
        (TrackedStream { stream, closed }, address)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.0.local_addr()
    }
}
impl Drop for TrackedStream {
    fn drop(&mut self) {
        let _ = self.closed.send(true);
    }
}
impl AsyncRead for TrackedStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buffer.filled().len();
        let remaining = buffer.remaining();
        let result = Pin::new(&mut self.stream).poll_read(cx, buffer);
        if matches!(result, Poll::Ready(Err(_)))
            || (matches!(result, Poll::Ready(Ok(())))
                && remaining > 0
                && buffer.filled().len() == before)
        {
            let _ = self.closed.send(true);
        }
        result
    }
}
impl AsyncWrite for TrackedStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stream).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
