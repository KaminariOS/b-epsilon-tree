#[derive(Debug)]
pub struct Args {
    /// ε
    pub eps: f32,

    /// buffer size
    pub buffer_size: usize,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            eps: 0.5,
            buffer_size: 34,
        }
    }
}
