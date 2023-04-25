use super::buffer_pool_replacer::{BufferPoolReplacerError, IBufferPoolReplacer};

#[derive(Debug, PartialEq, Clone)]
enum ClockReplacerPageStatus {
    Empty,
    Untouched,
    Accessed,
}

pub struct ClockReplacer {
    clock_hand: usize,
    page_status: Vec<ClockReplacerPageStatus>,
}

impl ClockReplacer {
    /// Creates a new [`ClockReplacer`].
    #[allow(dead_code)]
    pub fn new(size: usize) -> Self {
        ClockReplacer {
            clock_hand: 0,
            page_status: vec![ClockReplacerPageStatus::Empty; size],
        }
    }
}

impl ClockReplacer {
    fn max_size(&self) -> usize {
        self.page_status.len()
    }
}

impl IBufferPoolReplacer for ClockReplacer {
    fn victim(&mut self) -> Result<Option<usize>, BufferPoolReplacerError> {
        let mut victim = None;

        if let Ok(0) = self.size() {
            return Ok(None);
        }

        while victim.is_none() {
            match self.page_status[self.clock_hand] {
                ClockReplacerPageStatus::Empty => {}
                ClockReplacerPageStatus::Untouched => {
                    victim = Some(self.clock_hand);
                }
                ClockReplacerPageStatus::Accessed => {
                    self.page_status[self.clock_hand] = ClockReplacerPageStatus::Untouched;
                }
            }
            self.clock_hand = (self.clock_hand + 1) % self.max_size();
        }

        if let Some(idx) = victim {
            self.page_status[idx] = ClockReplacerPageStatus::Empty;
        }

        Ok(victim)
    }

    fn pin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError> {
        if frame_id >= self.max_size() {
            return Err(BufferPoolReplacerError::FrameOutOfRange(format!(
                "frame_id {} is out of range",
                frame_id
            )));
        }

        self.page_status[frame_id] = ClockReplacerPageStatus::Empty;
        Ok(())
    }

    fn unpin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError> {
        if frame_id >= self.max_size() {
            return Err(BufferPoolReplacerError::FrameOutOfRange(format!(
                "frame_id {} is out of range",
                frame_id
            )));
        }

        self.page_status[frame_id] = ClockReplacerPageStatus::Accessed;
        Ok(())
    }

    fn size(&self) -> Result<usize, BufferPoolReplacerError> {
        Ok(self
            .page_status
            .iter()
            .map(|status| match status {
                ClockReplacerPageStatus::Empty => 0,
                _ => 1,
            })
            .sum::<usize>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
        2,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
        2,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Accessed,
        ],
        3,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
        0,
    )]
    fn test_size(
        #[case] starting_state: Vec<ClockReplacerPageStatus>,
        #[case] expected_size: usize,
    ) {
        let mut clock_replacer = ClockReplacer::new(3);
        clock_replacer.page_status = starting_state;

        assert_eq!(clock_replacer.size(), Ok(expected_size));
    }

    #[rstest]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
        3,
        Err(BufferPoolReplacerError::FrameOutOfRange(format!(
            "frame_id 3 is out of range"
        ))),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    fn test_unpin(
        #[case] starting_state: Vec<ClockReplacerPageStatus>,
        #[case] to_unpin: usize,
        #[case] expected_result: Result<(), BufferPoolReplacerError>,
        #[case] expected_final_state: Vec<ClockReplacerPageStatus>,
    ) {
        let mut clock_replacer = ClockReplacer::new(3);
        clock_replacer.page_status = starting_state;

        let pin_result = clock_replacer.unpin(to_unpin);
        assert_eq!(pin_result, expected_result);
        assert_eq!(clock_replacer.page_status, expected_final_state);
    }

    #[rstest]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
        3,
        Err(BufferPoolReplacerError::FrameOutOfRange(format!(
            "frame_id 3 is out of range"
        ))),
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    fn test_pin(
        #[case] starting_state: Vec<ClockReplacerPageStatus>,
        #[case] to_pin: usize,
        #[case] expected_result: Result<(), BufferPoolReplacerError>,
        #[case] expected_final_state: Vec<ClockReplacerPageStatus>,
    ) {
        let mut clock_replacer = ClockReplacer::new(3);
        clock_replacer.page_status = starting_state;

        let pin_result = clock_replacer.pin(to_pin);
        assert_eq!(pin_result, expected_result);
        assert_eq!(clock_replacer.page_status, expected_final_state);
    }

    #[rstest]
    #[case(
        vec![
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Empty,
        ],
        Some(0),
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
        None,
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty
        ],
        Some(1),
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Accessed,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Accessed
        ],
        Some(2),
        vec![
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Untouched,
            ClockReplacerPageStatus::Empty,
            ClockReplacerPageStatus::Accessed
        ],
    )]
    fn test_victim(
        #[case] starting_state: Vec<ClockReplacerPageStatus>,
        #[case] expected_victim: Option<usize>,
        #[case] expected_final_state: Vec<ClockReplacerPageStatus>,
    ) {
        let mut clock_replacer = ClockReplacer::new(4);
        clock_replacer.page_status = starting_state;

        let victim_result = clock_replacer.victim();
        assert!(victim_result.is_ok());

        let victim = victim_result.unwrap();
        assert_eq!(victim, expected_victim);
        assert_eq!(clock_replacer.page_status, expected_final_state);
    }
}
