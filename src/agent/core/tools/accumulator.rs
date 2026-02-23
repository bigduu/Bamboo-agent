use uuid::Uuid;

use crate::agent::core::tools::{FunctionCall, ToolCall};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartialToolCall {
    pub id: String,
    pub tool_type: String,
    pub name: String,
    pub arguments: String,
}

#[derive(Debug, Default, Clone)]
pub struct ToolCallAccumulator {
    parts: Vec<PartialToolCall>,
}

impl ToolCallAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update(&mut self, call: ToolCall) {
        update_partial_tool_call(&mut self.parts, call);
    }

    pub fn extend<I>(&mut self, calls: I)
    where
        I: IntoIterator<Item = ToolCall>,
    {
        for call in calls {
            self.update(call);
        }
    }

    pub fn finalize(self) -> Vec<ToolCall> {
        finalize_tool_calls(self.parts)
    }

    pub fn parts(&self) -> &[PartialToolCall] {
        &self.parts
    }

    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }
}

pub fn update_partial_tool_call(parts: &mut Vec<PartialToolCall>, call: ToolCall) {
    if call.id.is_empty() && call.function.name.is_empty() && call.function.arguments.is_empty() {
        return;
    }

    if call.id.is_empty() && call.function.name.is_empty() {
        if let Some(last) = parts.last_mut() {
            last.arguments.push_str(&call.function.arguments);
        } else {
            parts.push(PartialToolCall {
                id: String::new(),
                tool_type: call.tool_type.clone(),
                name: String::new(),
                arguments: call.function.arguments.clone(),
            });
        }
        return;
    }

    let existing = if !call.id.is_empty() {
        parts.iter_mut().find(|part| part.id == call.id)
    } else if !call.function.name.is_empty() {
        parts.iter_mut().find(|part| {
            (part.id.is_empty() && part.name == call.function.name)
                || (part.id.is_empty() && part.name.is_empty())
        })
    } else {
        None
    };

    if let Some(existing) = existing {
        existing.arguments.push_str(&call.function.arguments);

        if !call.function.name.is_empty() {
            existing.name = call.function.name.clone();
        }

        if !call.tool_type.is_empty() {
            existing.tool_type = call.tool_type.clone();
        }
    } else {
        parts.push(PartialToolCall {
            id: call.id.clone(),
            tool_type: call.tool_type.clone(),
            name: call.function.name.clone(),
            arguments: call.function.arguments.clone(),
        });
    }
}

pub fn finalize_tool_calls(parts: Vec<PartialToolCall>) -> Vec<ToolCall> {
    parts
        .into_iter()
        .filter(|part| !part.name.trim().is_empty())
        .map(|part| ToolCall {
            id: if part.id.is_empty() {
                format!("call_{}", Uuid::new_v4())
            } else {
                part.id
            },
            tool_type: if part.tool_type.is_empty() {
                "function".to_string()
            } else {
                part.tool_type
            },
            function: FunctionCall {
                name: part.name,
                arguments: part.arguments,
            },
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_tool_call(id: &str, name: &str, arguments: &str) -> ToolCall {
        ToolCall {
            id: id.to_string(),
            tool_type: "function".to_string(),
            function: FunctionCall {
                name: name.to_string(),
                arguments: arguments.to_string(),
            },
        }
    }

    #[test]
    fn accumulator_merges_partial_arguments() {
        let mut accumulator = ToolCallAccumulator::new();

        accumulator.update(make_tool_call(
            "call_1",
            "execute_command",
            "{\"command\": \"",
        ));
        accumulator.update(make_tool_call("call_1", "", "echo hello"));
        accumulator.update(make_tool_call("call_1", "", "\"}"));

        let calls = accumulator.finalize();

        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.name, "execute_command");
        assert_eq!(calls[0].function.arguments, "{\"command\": \"echo hello\"}");
    }

    #[test]
    fn finalize_skips_calls_without_tool_name() {
        let mut parts = Vec::new();
        update_partial_tool_call(
            &mut parts,
            ToolCall {
                id: "call_1".to_string(),
                tool_type: "function".to_string(),
                function: FunctionCall {
                    name: String::new(),
                    arguments: "{}".to_string(),
                },
            },
        );

        let calls = finalize_tool_calls(parts);
        assert!(calls.is_empty());
    }

    #[test]
    fn argument_only_chunk_extends_last_partial() {
        let mut parts = Vec::new();
        update_partial_tool_call(
            &mut parts,
            make_tool_call("call_1", "execute_command", "{\"a\":"),
        );
        update_partial_tool_call(&mut parts, make_tool_call("", "", "1}"));

        let calls = finalize_tool_calls(parts);
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function.arguments, "{\"a\":1}");
    }
}
