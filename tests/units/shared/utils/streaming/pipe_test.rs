use sx_vault_media::shared::utils::operators::builder::FnOperator;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};
use sx_vault_media::shared::utils::streaming::traits::{NoOpEmitter, StreamOperator};

#[tokio::test]
async fn test_stream_pipe_as_stream_operator() {
    let op1 = FnOperator::new("op1").do_it(|_chunk, _state, _emitter| Ok(()));
    let op2 = FnOperator::new("op2").do_it(|_chunk, _state, _emitter| Ok(()));

    let mut pipe = StreamPipe::new("test_pipe", vec![Box::new(op1), Box::new(op2)]);

    assert_eq!(pipe.name(), "test_pipe");

    let emitter = NoOpEmitter;
    let res = pipe.process(Some(b"hello world"), &emitter).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), Some(b"hello world".to_vec()));

    let flush_res = pipe.process(None, &emitter).await;
    assert!(flush_res.is_ok());
    assert_eq!(flush_res.unwrap(), None);
}

#[tokio::test]
async fn test_reactive_stream_pipe_execution() {
    let dummy_data = b"0000ftypisom0000";
    let reader = &dummy_data[..];

    let op = FnOperator::new("passthrough").do_it(|_chunk, _state, _emitter| Ok(()));
    let pipe = StreamPipe::with_operators(vec![Box::new(op)]);

    let result = reactive_stream_pipe(reader, pipe, "test_media_123".to_string()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), dummy_data.len() as u64);
}
