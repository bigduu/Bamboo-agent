//! Prompt construction for the auto-title generator.

use bamboo_agent_core::Message;

/// Build the system+user message pair sent to the fast model for title generation.
///
/// The user content is truncated to 2000 characters to keep prompt costs bounded.
pub fn build_title_messages(first_user_text: &str) -> Vec<Message> {
    let truncated: String = first_user_text.chars().take(2000).collect();
    let system = "Generate a 3-7 word title summarising this conversation. \
                  Output the title text only - no quotes, no trailing punctuation, no preamble.";
    vec![Message::system(system), Message::user(truncated)]
}
