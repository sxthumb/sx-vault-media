use std::sync::OnceLock;
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub enum MediaEvent {
    Progress {
        media_id: String,
        state: String,
        message: String,
        percentage: f32,
    },
    Completed {
        media_id: String,
        total_bytes: u64,
    },
    Failed {
        media_id: String,
        error: String,
    },
}

static BUS: OnceLock<broadcast::Sender<MediaEvent>> = OnceLock::new();

fn get_bus() -> &'static broadcast::Sender<MediaEvent> {
    BUS.get_or_init(|| {
        let (tx, _) = broadcast::channel(1024);
        tx
    })
}

/// Dispara um evento globalmente de qualquer lugar do código (operadores, pipe, etc)
pub fn publish(event: MediaEvent) {
    let bus = get_bus();
    let _ = bus.send(event); // Se ninguém estiver ouvindo, a mensagem é descartada sem erro
}

/// Cria um receiver para assinar os eventos do barramento
pub fn subscribe() -> broadcast::Receiver<MediaEvent> {
    get_bus().subscribe()
}