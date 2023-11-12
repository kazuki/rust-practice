use super::{Generator, NumberGenerator};

use rand::{thread_rng, Rng};

pub struct Zipfian {
    items: u64,
    base: u64,
    // constant: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    theta: f64,
    // zeta2theta: f64,
    // count_for_zeta: i64,
    // last_value: i64,
}

const ZIPFIAN_CONSTANT: f64 = 0.99;

impl Zipfian {
    pub fn new_from_count(items: u64) -> Zipfian {
        Self::new_from_range(0, items - 1)
    }
    pub fn new_from_range(min: u64, max: u64) -> Zipfian {
        Self::new(
            min,
            max,
            ZIPFIAN_CONSTANT,
            Self::zetastatic(0, max - min + 1, ZIPFIAN_CONSTANT, 0.0),
        )
    }
    pub fn new(min: u64, max: u64, constant: f64, zetan: f64) -> Zipfian {
        let zetan = Self::zetastatic(0, max - min + 1, constant, 0.0);
        let items = max - min + 1;
        let theta = constant;
        let zeta2theta = Self::zeta(2, theta);
        Zipfian {
            items: items,
            base: min,
            // constant: constant,
            theta: theta,
            // zeta2theta: zeta2theta,
            alpha: 1.0 / (1.0 - theta),
            zetan: zetan,
            // count_for_zeta: items,
            eta: (1.0 - (2.0 / items as f64)).powf(1.0 - theta) / (1.0 - zeta2theta / zetan),
            // last_value: 0,
        }
    }

    fn zeta(n: u64, theta_value: f64) -> f64 {
        Self::zetastatic(0, n, theta_value, 0.0)
    }

    fn zetastatic(st: u64, n: u64, theta: f64, initialsum: f64) -> f64 {
        let mut sum = initialsum;
        for i in st..n {
            sum += 1.0 / ((i + 1) as f64).powf(theta);
        }
        return sum;
    }
}

impl NumberGenerator for Zipfian {}

impl Generator<u64> for Zipfian {
    // fn last(&self) -> u64 { self.last_value }

    fn next(&self) -> u64 {
        let u = thread_rng().gen::<f64>();
        let uz = u * self.zetan;
        if uz < 1.0 {
            return self.base;
        }
        if uz < 1.0 + 0.5f64.powf(self.theta) {
            return self.base + 1;
        }
        let ret = self.base
            + ((self.items as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha)) as u64;
        // self.last_value = ret;
        return ret;
    }
}
