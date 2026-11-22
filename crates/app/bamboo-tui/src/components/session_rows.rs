use std::cell::RefCell;

use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};

/// Session identity is stable even when a live update reorders visible rows.
#[derive(Debug, Default)]
pub(crate) struct SessionRows {
    hitboxes: RefCell<Vec<(Rect, String)>>,
    pressed: Option<String>,
}

impl SessionRows {
    pub(crate) fn render(&self, area: Rect, rows: Vec<(usize, String)>) {
        let mut hitboxes = self.hitboxes.borrow_mut();
        hitboxes.clear();
        hitboxes.extend(rows.into_iter().filter_map(|(offset, id)| {
            let offset = u16::try_from(offset).ok()?;
            (offset < area.height).then(|| (Rect::new(area.x, area.y + offset, area.width, 1), id))
        }));
    }

    pub(crate) fn handle_mouse(&mut self, mouse: MouseEvent) -> Option<String> {
        let at_pointer = || {
            let position = Position::new(mouse.column, mouse.row);
            self.hitboxes
                .borrow()
                .iter()
                .find(|(area, _)| area.contains(position))
                .map(|(_, id)| id.clone())
        };
        match mouse.kind {
            MouseEventKind::Down(MouseButton::Left) => self.pressed = at_pointer(),
            MouseEventKind::Up(MouseButton::Left) => {
                let released = at_pointer();
                return self
                    .pressed
                    .take()
                    .filter(|id| released.as_ref() == Some(id));
            }
            _ => self.pressed = None,
        }
        None
    }
}
