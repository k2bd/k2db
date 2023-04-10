use super::replacer::{BufferPoolReplacer, BufferPoolReplacerError};

#[derive(Debug, PartialEq, Clone)]
enum ClockReplacerPageStatus {
    EMPTY,
    UNTOUCHED,
    ACCESSED,
}

struct ClockReplacer {
    size: usize,
    clock_hand: usize,
    page_status: Vec<ClockReplacerPageStatus>,
}

impl ClockReplacer {
    /// Creates a new [`ClockReplacer`].
    fn new(size: usize) -> Self {
        ClockReplacer {
            size,
            clock_hand: 0,
            page_status: vec![ClockReplacerPageStatus::EMPTY; size],
        }
    }
}

impl BufferPoolReplacer for ClockReplacer {
    fn victim(&mut self) -> Result<Option<usize>, BufferPoolReplacerError> {
        let mut victim = None;

        if let Ok(0) = self.size() {
            return Ok(None);
        }

        while victim.is_none() {
            match self.page_status[self.clock_hand] {
                ClockReplacerPageStatus::EMPTY => {}
                ClockReplacerPageStatus::UNTOUCHED => {
                    victim = Some(self.clock_hand);
                }
                ClockReplacerPageStatus::ACCESSED => {
                    self.page_status[self.clock_hand] = ClockReplacerPageStatus::UNTOUCHED;
                }
            }
            self.clock_hand = (self.clock_hand + 1) % self.size;
        }

        if let Some(idx) = victim {
            self.page_status[idx] = ClockReplacerPageStatus::EMPTY;
        }

        Ok(victim)
    }

    fn pin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError> {
        if frame_id >= self.size {
            return Err(BufferPoolReplacerError::FrameOutOfRange(format!(
                "frame_id {} is out of range",
                frame_id
            )));
        }

        self.page_status[frame_id] = ClockReplacerPageStatus::EMPTY;
        Ok(())
    }

    fn unpin(&mut self, frame_id: usize) -> Result<(), BufferPoolReplacerError> {
        if frame_id >= self.size {
            return Err(BufferPoolReplacerError::FrameOutOfRange(format!(
                "frame_id {} is out of range",
                frame_id
            )));
        }

        self.page_status[frame_id] = ClockReplacerPageStatus::ACCESSED;
        Ok(())
    }

    fn size(&self) -> Result<usize, BufferPoolReplacerError> {
        Ok(self
            .page_status
            .iter()
            .map(|status| match status {
                ClockReplacerPageStatus::EMPTY => 0,
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
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
        2,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
        ],
        2,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::ACCESSED,
        ],
        3,
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
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
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
        ],
        3,
        Err(BufferPoolReplacerError::FrameOutOfRange(format!(
            "frame_id 3 is out of range"
        ))),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
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
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
        ],
        1,
        Ok(()),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
        ],
        3,
        Err(BufferPoolReplacerError::FrameOutOfRange(format!(
            "frame_id 3 is out of range"
        ))),
        vec![
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
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
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::EMPTY,
        ],
        Some(0),
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
        None,
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY
        ],
        Some(1),
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY
        ],
    )]
    #[case(
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::ACCESSED,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::ACCESSED
        ],
        Some(2),
        vec![
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::UNTOUCHED,
            ClockReplacerPageStatus::EMPTY,
            ClockReplacerPageStatus::ACCESSED
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
