use crossterm::event::{KeyEvent, MouseEvent};

use crate::api::types::AgentEvent;

pub enum AppEvent {
    Key(KeyEvent),
    Mouse(MouseEvent),
    Resize(u16, u16),
    SseEvent(AgentEvent),
    ApiError(String),
}
