use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Default)]
pub struct ContextualInfo {
    context: HashMap<String, Value>,
}

impl ContextualInfo {
    pub fn new() -> Self {
        Self {
            context: HashMap::new(),
        }
    }

    pub fn add_context(&mut self, key: &str, value: Value) {
        self.context.insert(key.to_string(), value);
    }

    pub fn merge_with_event(&self, event: &mut Value) {
        if let Value::Object(ref mut map) = event {
            for (key, value) in &self.context {
                map.insert(key.clone(), value.clone());
            }
        }
    }
}
