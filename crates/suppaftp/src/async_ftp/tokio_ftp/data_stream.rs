//! # Data Stream
//!
//! This module exposes the async data stream implementation where bytes must be written to/read from

use std::pin::Pin;

use pin_project::pin_project;
#[cfg(feature = "tokio")]
use tokio::io::Result;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
#[cfg(feature = "tokio")]
use tokio::net::TcpStream;
use tokio::sync::OwnedSemaphorePermit;

use super::TokioTlsStream;

#[pin_project(project = DataStreamInnerProj)]
enum DataStreamInner<T>
where
    T: TokioTlsStream + Send,
{
    Tcp(#[pin] TcpStream),
    Ssl(#[pin] Box<T>),
}

/// Data Stream used for communications. It can be both of type Tcp in case of plain communication or Ssl in case of FTPS
#[pin_project]
pub struct DataStream<T>
where
    T: TokioTlsStream + Send,
{
    #[pin]
    inner: DataStreamInner<T>,
    permit: Option<OwnedSemaphorePermit>,
}

impl<T> DataStream<T>
where
    T: TokioTlsStream + Send,
{
    pub(super) fn from_tcp(stream: TcpStream) -> Self {
        DataStream {
            inner: DataStreamInner::Tcp(stream),
            permit: None,
        }
    }

    pub(super) fn from_ssl(stream: Box<T>) -> Self {
        DataStream {
            inner: DataStreamInner::Ssl(stream),
            permit: None,
        }
    }

    pub(super) fn with_permit(mut self, permit: OwnedSemaphorePermit) -> Self {
        self.permit = Some(permit);
        self
    }
}

#[cfg(feature = "async-secure")]
impl<T> DataStream<T>
where
    T: TokioTlsStream + Send,
{
    /// Unwrap the stream into TcpStream. This method is only used in secure connection.
    pub fn into_tcp_stream(self) -> TcpStream {
        match self.inner {
            DataStreamInner::Tcp(stream) => stream,
            DataStreamInner::Ssl(stream) => stream.tcp_stream(),
        }
    }
}

impl<T> DataStream<T>
where
    T: TokioTlsStream + Send,
{
    /// Returns a reference to the underlying TcpStream.
    pub fn get_ref(&self) -> &TcpStream {
        match &self.inner {
            DataStreamInner::Tcp(stream) => stream,
            DataStreamInner::Ssl(stream) => stream.get_ref(),
        }
    }
}

// -- async

impl<T> AsyncRead for DataStream<T>
where
    T: TokioTlsStream + Send,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<Result<()>> {
        match self.project().inner.project() {
            DataStreamInnerProj::Tcp(stream) => stream.poll_read(cx, buf),
            DataStreamInnerProj::Ssl(stream) => stream.poll_read(cx, buf),
        }
    }
}

impl<T> AsyncWrite for DataStream<T>
where
    T: TokioTlsStream + Send,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize>> {
        match self.project().inner.project() {
            DataStreamInnerProj::Tcp(stream) => stream.poll_write(cx, buf),
            DataStreamInnerProj::Ssl(stream) => stream.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        match self.project().inner.project() {
            DataStreamInnerProj::Tcp(stream) => stream.poll_flush(cx),
            DataStreamInnerProj::Ssl(stream) => stream.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<()>> {
        match self.project().inner.project() {
            DataStreamInnerProj::Tcp(stream) => stream.poll_shutdown(cx),
            DataStreamInnerProj::Ssl(stream) => stream.poll_shutdown(cx),
        }
    }
}
