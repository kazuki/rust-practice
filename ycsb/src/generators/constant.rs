use super::{Generator, NumberGenerator};

pub struct Constant {
    value: u64,
}

impl Constant {
    pub fn new(value: u64) -> Self {
        Constant { value }
    }
}

impl NumberGenerator for Constant {}
impl Generator<u64> for Constant {
    // fn last(&self) -> u64 { self.value }
    fn next(&self) -> u64 {
        self.value
    }
}
