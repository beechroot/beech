//! ProbShaper — probabilistic completion helper
//!
//! Drives content-addressed node-splitting decisions from a stream of bytes.
//! Each `is_complete` call advances an internal SHA-256 state and a byte
//! counter, then stochastically answers "should the current span end here?"
//! according to a log-normal schedule that favors spans of length
//! approximately `target_length` (linear-space mean).
//!
//! ```rust
//! use beech_shaper::ProbShaper;
//!
//! // Mean ≈ 1000 bytes, linear-space stddev = 500 bytes.
//! let mut shaper = ProbShaper::new(1_000, 500);
//! let chunk = [0, 1, 2, 3];
//! if shaper.is_complete(&chunk[..]) {
//!     let id = shaper.id();
//! }
//! ```
//!
//! # Distribution
//!
//! Under the hood this is a log-normal on positive reals, parameterized so
//! that the linear-space mean and standard deviation match the constructor
//! arguments. The log-space parameters are derived by moment-matching:
//!
//! ```text
//! σ² = ln(1 + stddev² / mean²)
//! μ   = ln(mean) − σ² / 2
//! ```
//!
//! Note that for a log-normal the **median** is `mean · exp(−σ²/2)`, which
//! is strictly less than the mean. So `CDF(target_length)` is slightly above
//! 0.5 (the exact value depends on the chosen stddev), not exactly 0.5.

use sha2::{Digest, Sha256};
use statrs::distribution::{ContinuousCDF, LogNormal};

/// A helper that answers "is it complete yet?" according to a log-normal
/// schedule over a running byte count.
///
/// Parameters in the constructor are **linear-space**: `target_length` is
/// the mean of the distribution and `stddev` is its standard deviation,
/// both in the same units as `bytes.len()` in [`is_complete`].
#[derive(Debug, Clone)]
pub struct ProbShaper {
    dist: LogNormal,
    hasher: Sha256,
    elapsed: usize,
}

impl ProbShaper {
    /// Construct a `ProbShaper` whose underlying log-normal has linear-space
    /// mean `target_length` and standard deviation `stddev`.
    ///
    /// Panics if either parameter is zero.
    pub fn new(target_length: usize, stddev: usize) -> Self {
        assert!(
            target_length > 0 && stddev > 0,
            "target_length and stddev must be > 0"
        );

        let mean = target_length as f64;
        let sd = stddev as f64;

        // Moment-match linear-space (mean, stddev) to log-space (μ, σ).
        let sigma = (1.0 + (sd * sd) / (mean * mean)).ln().sqrt();
        let mu = mean.ln() - 0.5 * sigma * sigma;
        let dist = LogNormal::new(mu, sigma).expect("valid log-normal");

        Self {
            dist,
            hasher: Sha256::new(),
            elapsed: 0,
        }
    }

    /// Consume a chunk of bytes and stochastically decide whether the span
    /// ends here.
    ///
    /// On each call:
    /// 1. The byte counter advances by `bytes.len()`.
    /// 2. The bytes are folded into the running SHA-256 state.
    /// 3. A step-hazard is computed:
    ///    `h = (F(t) − F(t₀)) / (1 − F(t₀))`, where `F` is the log-normal CDF,
    ///    `t₀` is the counter before this call, and `t` is the counter after.
    ///    This is the conditional probability of completing in this step given
    ///    we haven't completed yet.
    /// 4. A pseudo-uniform draw `p ∈ [0, 1)` is derived from the current hash
    ///    state (non-destructively snapshotted), and the method returns
    ///    `p < h`.
    ///
    /// Aggregate behavior: the probability of *not* having returned `true`
    /// after the counter reaches `T` is approximately `1 − F(T)`, so spans
    /// cluster near `target_length` with the spread controlled by `stddev`.
    pub fn is_complete(&mut self, bytes: &[u8]) -> bool {
        let t_prev = self.elapsed;
        let t_curr = t_prev + bytes.len();
        self.hasher.update(bytes);
        self.elapsed = t_curr;

        let f_prev = self.dist.cdf(t_prev as f64);
        let f_curr = self.dist.cdf(t_curr as f64);
        let denom = 1.0 - f_prev;
        if denom <= 0.0 {
            // Numerically saturated — treat as certainly complete.
            return true;
        }
        let hazard = (f_curr - f_prev) / denom;

        // Derive a uniform draw from the current hash state without consuming it.
        let snapshot = self.hasher.clone().finalize();
        let x = u64::from_be_bytes(snapshot[0..8].try_into().unwrap());
        let p_draw = ((x >> 11) as f64) * (1.0 / (1u64 << 53) as f64);

        p_draw < hazard
    }

    /// Finalize the running hash and return its 32-byte digest.
    ///
    /// Does not consume the shaper; the internal state is cloned so the
    /// shaper can continue to be used or dropped afterward.
    pub fn id(&self) -> [u8; 32] {
        self.hasher.clone().finalize().into()
    }
}

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
