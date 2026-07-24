use std::net::SocketAddr;
use std::sync::Arc;
use colored::*;

use sx_vault_media::core::use_cases::upload_video::UploadVideoUseCase;
use sx_vault_media::infra::adapters::inbound::grpc::{
    MediaGrpcController, MediaServiceServer,
};

const MAX_MESSAGE_SIZE: usize = 2 * 1024 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:50051".parse()?;

    println!("{}", "Initializing Video Vault Service...".bold().green());

    let upload_use_case = Arc::new(UploadVideoUseCase::new());

    let video_controller = MediaGrpcController::new(upload_use_case);

    println!(
        "{}",
        format!("gRPC server listening on h2://{}", addr)
            .bold()
            .green()
    );

    tonic::transport::Server::builder()
        .add_service(
            MediaServiceServer::new(video_controller)
            .max_decoding_message_size(MAX_MESSAGE_SIZE)
        )
        .serve(addr)
        .await?;

    Ok(())
}