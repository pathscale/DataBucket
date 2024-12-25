use crate::Link;

use super::Interval;

pub struct PageIterator {
    intervals: Vec<Interval>,
    current_intervals_index: usize,
    current_position_in_interval: usize,
}

impl PageIterator {
    pub fn new(intervals: Vec<Interval>) -> PageIterator {
        PageIterator {
            current_intervals_index: 0,
            current_position_in_interval: if intervals.len() > 0 { intervals[0].0 } else { 0 },
            intervals,
        }
    }
}

impl Iterator for PageIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let mut result: Option<Self::Item> = None;

        if self.current_intervals_index >= self.intervals.len() {
            result = None
        } else if self.current_position_in_interval
            >= self.intervals[self.current_intervals_index].0
            && self.current_position_in_interval <= self.intervals[self.current_intervals_index].1
        {
            result = Some(self.current_position_in_interval as u32);
            self.current_position_in_interval += 1;
        } else if self.current_position_in_interval > self.intervals[self.current_intervals_index].1
        {
            self.current_intervals_index += 1;
            if self.current_intervals_index >= self.intervals.len() {
                result = None;
            } else {
                self.current_position_in_interval = self.intervals[self.current_intervals_index].0;
                result = Some(self.current_position_in_interval as u32);
                self.current_position_in_interval += 1;
            }
        }

        result
    }
}

#[cfg(test)]
mod test {
    use crate::Interval;

    use super::PageIterator;

    #[test]
    fn test_page_iterator() {
        let interval1 = Interval(1, 2);
        let interval2 = Interval(5, 7);
        let page_iterator = PageIterator::new(vec![interval1, interval2]);
        let collected = page_iterator.collect::<Vec<_>>();
        assert_eq!(collected, vec![1, 2, 5, 6, 7]);
    }

    #[test]
    fn test_page_iterator_empty() {
        let page_iterator = PageIterator::new(vec![]);
        let collected = page_iterator.collect::<Vec<_>>();
        assert_eq!(collected, Vec::<u32>::new());
    }
}
