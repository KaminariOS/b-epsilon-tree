use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// ε
    #[arg(short, long, default_value_t = 0.5)]
    pub eps: f32,

    /// buffer size
    #[arg(short, long, default_value_t = 34)]
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
