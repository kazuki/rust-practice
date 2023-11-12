mod constant;
mod counter;
mod zipfian;

pub use constant::Constant;
pub use counter::Counter;
pub use zipfian::Zipfian;

pub trait Generator<T>: 'static + std::marker::Send + std::marker::Sync {
    fn next(&self) -> T;
    // fn last(&self) -> T;
}

pub trait NumberGenerator: Generator<u64> {}
