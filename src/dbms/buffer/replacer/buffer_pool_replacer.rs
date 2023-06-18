#[derive(Debug, PartialEq, Eq)]
pub enum BufferPoolReplacerError {
    /// Frame is out of range
    FrameOutOfRange(String),
}

pub trait IBufferPoolReplacer {
    /// Select a frame to remove from the buffer pool, returning the ID of the
    /// removed frame. If no frame can be freed, e.g. all frame slots are free,
    /// return `None`.
    fn victim(&mut self) -> Result<Option<usize>, BufferPoolReplacerError>;
    /// Remove a frame from the replacer, after it's been addd to the buffer
    /// pool.
    fn pin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError>;
    /// Add a frame to the replacer, after its pin count becomes zero.
    fn unpin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError>;
    /// Return the number of frames currently in the replacer.
    fn size(&self) -> Result<usize, BufferPoolReplacerError>;
}
