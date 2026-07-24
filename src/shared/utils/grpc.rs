use bytes::Bytes;
use tokio_stream::{ StreamExt};
use tonic::Status;
use crate::core::ports::inbound::media_process::ByteStream;

pub fn tonic_stream_to_byte_stream<T, F>(
    stream: tonic::Streaming<T>,
    extractor: F,
) -> ByteStream
where
    T: Send + 'static,
    F: Fn(T) -> Vec<u8> + Send + Sync + 'static,
{
    let mapped = stream.map(move |item| {
        item.map(|chunk| Bytes::from(extractor(chunk)))
            .map_err(|e: Status| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
    });

    Box::pin(mapped)
}