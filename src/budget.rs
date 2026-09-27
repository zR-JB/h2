use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// A byte budget shared across connections, set with [`shared_budget`].
///
/// [`shared_budget`]: crate::server::Builder::shared_budget
pub trait SharedBudget: Send + Sync + fmt::Debug {
    /// Charges `bytes`, returning `false` without charging when unavailable.
    fn try_charge(&self, bytes: usize) -> bool;

    /// Returns `bytes` previously charged with [`try_charge`](Self::try_charge).
    fn refund(&self, bytes: usize);
}

pub(crate) type Budget = Option<Arc<dyn SharedBudget>>;

/// A connection's allowance for state whose bytes the caller reserved up front.
#[derive(Debug)]
pub(crate) struct Allowance {
    used: AtomicUsize,
    max: usize,
}

impl Allowance {
    pub(crate) fn new(max: usize) -> Self {
        Allowance {
            used: AtomicUsize::new(0),
            max,
        }
    }
}

impl SharedBudget for Allowance {
    fn try_charge(&self, bytes: usize) -> bool {
        let max = self.max;
        self.used
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|used| *used <= max)
            })
            .is_ok()
    }

    fn refund(&self, bytes: usize) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
    }
}

#[derive(Debug, Default)]
pub(crate) struct Charge {
    budget: Budget,
    bytes: usize,
}

impl Charge {
    pub(crate) fn try_add(&mut self, budget: &Budget, bytes: usize) -> bool {
        let budget = match budget {
            Some(budget) if bytes > 0 => budget,
            _ => return true,
        };
        if !budget.try_charge(bytes) {
            return false;
        }
        self.budget.get_or_insert_with(|| budget.clone());
        self.bytes += bytes;
        true
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn shrink_to(&mut self, bytes: usize) {
        if let Some(budget) = &self.budget {
            if bytes < self.bytes {
                budget.refund(self.bytes - bytes);
                self.bytes = bytes;
            }
        }
    }

    pub(crate) fn absorb(&mut self, mut other: Charge) {
        if other.budget.is_some() {
            self.budget = other.budget.take();
            self.bytes += std::mem::take(&mut other.bytes);
        }
    }
}

impl Drop for Charge {
    fn drop(&mut self) {
        self.shrink_to(0);
    }
}

// Accounting never distinguishes otherwise equal frames.
impl PartialEq for Charge {
    fn eq(&self, _: &Charge) -> bool {
        true
    }
}

impl Eq for Charge {}
