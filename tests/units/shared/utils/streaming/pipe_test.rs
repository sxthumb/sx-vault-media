use sx_vault_media::shared::utils::operators::FnOperator;
use sx_vault_media::shared::utils::streaming::context::PipelineContext;
use sx_vault_media::shared::utils::streaming::pipe::{reactive_stream_pipe, StreamPipe};
use sx_vault_media::shared::utils::streaming::traits::{NoOpEmitter, StreamOperator};

#[tokio::test]
async fn test_stream_pipe_as_stream_operator() {
    let op1 = FnOperator::new("op1").do_it(|_chunk, _state, _ctx, _emitter| Ok(()));
    let op2 = FnOperator::new("op2").do_it(|_chunk, _state, _ctx, _emitter| Ok(()));

    let mut pipe = StreamPipe::new("test_pipe", vec![Box::new(op1), Box::new(op2)]);

    assert_eq!(pipe.name(), "test_pipe");

    let mut ctx = PipelineContext::new();
    let emitter = NoOpEmitter;
    let res = pipe.process(Some(b"hello world"), &mut ctx, &emitter).await;
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), Some(b"hello world".to_vec()));

    let flush_res = pipe.process(None, &mut ctx, &emitter).await;
    assert!(flush_res.is_ok());
    assert_eq!(flush_res.unwrap(), None);
}

#[tokio::test]
async fn test_reactive_stream_pipe_execution() {
    let dummy_data = b"0000ftypisom0000";
    let reader = &dummy_data[..];

    let op = FnOperator::new("passthrough").do_it(|_chunk, _state, _ctx, _emitter| Ok(()));
    let pipe = StreamPipe::with_operators(vec![Box::new(op)]);

    let result = reactive_stream_pipe(reader, pipe, "test_media_123".to_string()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), dummy_data.len() as u64);
}

#[tokio::test]
async fn test_fn_operator_validate_it_passes_on_valid_ctx() {
    #[derive(Clone)]
    struct Flag(bool);

    let op = FnOperator::new("validator")
        .validate_it(|_state, ctx, _emitter| {
            let flag = ctx.get::<Flag>().ok_or_else(|| {
                sx_vault_media::shared::utils::streaming::errors::PipelineError::OperatorFailed {
                    operator_name: "validator",
                    reason: "Flag ausente".to_string(),
                }
            })?;
            if flag.0 {
                Ok(())
            } else {
                Err(sx_vault_media::shared::utils::streaming::errors::PipelineError::OperatorFailed {
                    operator_name: "validator",
                    reason: "flag false".to_string(),
                })
            }
        });

    let mut pipe = StreamPipe::with_operators(vec![Box::new(op)]);
    let mut ctx = PipelineContext::new();
    ctx.insert(Flag(true));
    let emitter = NoOpEmitter;

    // on_next deve ser pass-through
    let chunk_res = pipe.process(Some(b"chunk"), &mut ctx, &emitter).await;
    assert_eq!(chunk_res.unwrap(), Some(b"chunk".to_vec()));

    // on_complete deve validar com sucesso
    let flush_res = pipe.process(None, &mut ctx, &emitter).await;
    assert!(flush_res.is_ok());
}

#[tokio::test]
async fn test_fn_operator_transform_it_modifies_bytes() {
    use sx_vault_media::shared::utils::operators::FnOperator;

    let op = FnOperator::new("to_uppercase").transform_it(|chunk, _state, _ctx, _emitter| {
        Ok(chunk.iter().map(|b| b.to_ascii_uppercase()).collect())
    });

    let mut pipe = StreamPipe::with_operators(vec![Box::new(op)]);
    let mut ctx = PipelineContext::new();
    let emitter = NoOpEmitter;

    let res = pipe.process(Some(b"hello"), &mut ctx, &emitter).await;
    assert_eq!(res.unwrap(), Some(b"HELLO".to_vec()));
}
