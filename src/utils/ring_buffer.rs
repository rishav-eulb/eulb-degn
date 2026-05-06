/// Fixed-size ring buffer for maintaining a rolling window of values.
/// When full, new values overwrite the oldest entry.
#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    buf: Vec<Option<T>>,
    head: usize,
    len: usize,
    capacity: usize,
}

impl<T: Clone> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![None; capacity],
            head: 0,
            len: 0,
            capacity,
        }
    }

    pub fn push(&mut self, value: T) {
        self.buf[self.head] = Some(value);
        self.head = (self.head + 1) % self.capacity;
        if self.len < self.capacity {
            self.len += 1;
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    /// Returns the most recently pushed item.
    pub fn latest(&self) -> Option<&T> {
        if self.len == 0 {
            return None;
        }
        let idx = if self.head == 0 {
            self.capacity - 1
        } else {
            self.head - 1
        };
        self.buf[idx].as_ref()
    }

    /// Returns the oldest item in the buffer.
    pub fn oldest(&self) -> Option<&T> {
        if self.len == 0 {
            return None;
        }
        if self.len < self.capacity {
            self.buf[0].as_ref()
        } else {
            self.buf[self.head].as_ref()
        }
    }

    /// Iterates from oldest to newest.
    pub fn iter(&self) -> RingBufferIter<'_, T> {
        RingBufferIter {
            buf: &self.buf,
            start: if self.len < self.capacity {
                0
            } else {
                self.head
            },
            count: 0,
            len: self.len,
            capacity: self.capacity,
        }
    }

    pub fn clear(&mut self) {
        self.buf.iter_mut().for_each(|v| *v = None);
        self.head = 0;
        self.len = 0;
    }
}

pub struct RingBufferIter<'a, T> {
    buf: &'a [Option<T>],
    start: usize,
    count: usize,
    len: usize,
    capacity: usize,
}

impl<'a, T> Iterator for RingBufferIter<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.len {
            return None;
        }
        let idx = (self.start + self.count) % self.capacity;
        self.count += 1;
        self.buf[idx].as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_latest() {
        let mut rb = RingBuffer::new(3);
        rb.push(1.0);
        rb.push(2.0);
        rb.push(3.0);
        assert_eq!(rb.latest(), Some(&3.0));
        assert_eq!(rb.oldest(), Some(&1.0));
        assert_eq!(rb.len(), 3);
        assert!(rb.is_full());
    }

    #[test]
    fn test_overflow() {
        let mut rb = RingBuffer::new(3);
        rb.push(1.0);
        rb.push(2.0);
        rb.push(3.0);
        rb.push(4.0);
        assert_eq!(rb.latest(), Some(&4.0));
        assert_eq!(rb.oldest(), Some(&2.0));
        assert_eq!(rb.len(), 3);
    }

    #[test]
    fn test_iter() {
        let mut rb = RingBuffer::new(3);
        rb.push(10);
        rb.push(20);
        rb.push(30);
        rb.push(40);
        let items: Vec<&i32> = rb.iter().collect();
        assert_eq!(items, vec![&20, &30, &40]);
    }
}
