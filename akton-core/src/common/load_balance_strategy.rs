use crate::{common::Context, prelude::LoadBalancerStrategy};

use rand::{thread_rng, Rng};

#[derive(Debug, Default)]
pub struct RoundRobinStrategy {
    current_index: usize,
}

impl RoundRobinStrategy {
    pub fn new() -> Self {
        RoundRobinStrategy { current_index: 0 }
    }
}

impl LoadBalancerStrategy for RoundRobinStrategy {
    fn select_item(&mut self, items: &[Context]) -> Option<usize> {
        if items.is_empty() {
            None
        } else {
            self.current_index = (self.current_index + 1) % items.len();
            Some(self.current_index)
        }
    }
}

#[derive(Debug, Default)]
pub struct RandomStrategy;

impl LoadBalancerStrategy for RandomStrategy {
    fn select_item(&mut self, items: &[Context]) -> Option<usize> {
        if items.is_empty() {
            None
        } else {
            let index = thread_rng().gen_range(0..items.len());
            Some(index)
        }
    }
}

#[derive(Debug)]
pub enum LBStrategy {
    RoundRobin,
    Random,
}
