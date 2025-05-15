//! ProbShaper — probabilistic completion helper
//!
//! ```rust
//! use beech_shaper::ProbShaper;
//!
//! let mut shaper = ProbShaper::new(1_000, 500);          // target = 1000 units
//! let bytes = [0,1,2,3];
//! if shaper.is_complete(&bytes[..]) {
//!     println!("Finished!");
//! }
//! ```

use sha2::{Digest, Sha256};
use statrs::distribution::{ContinuousCDF, LogNormal};

/// A helper that answers “is it complete yet?” according to a log‑normal schedule.
///
/// * `target_length` is the point where the distribution’s **mean** sits.  
/// * `sigma` controls the spread (≈ *standard deviation* of `ln(x)`).  
///   Typical values: 0.3 – 1.0.  Larger ⇒ broader, more forgiving tail.
///
/// The only public method, [`is_complete`], returns `true` stochastically:
/// `P(true) = F_LN(elapsed)`, where `F_LN` is the CDF of the underlying
/// log‑normal distribution.
#[derive(Debug, Clone)]
pub struct ProbShaper {
    dist: LogNormal,
    hasher: Sha256,
    elapsed: Option<usize>,
}

impl ProbShaper {
    /// Create a new `ProbShaper` where `target_length` is the mean and `stddev` is the standard deviation,
    /// both in linear space (elapsed-time units).
    pub fn new(target_length: usize, stddev: usize) -> Self {
        assert!(
            target_length > 0 && stddev > 0,
            "target and stddev must be > 0"
        );

        let m = target_length as f64;
        let s = stddev as f64;

        // Convert linear-space mean/stddev to log-space parameters
        let sigma = (1.0 + (s * s) / (m * m)).ln().sqrt();
        let mu = m.ln() - 0.5 * sigma * sigma;
        let dist = LogNormal::new(mu, sigma).expect("valid log-normal");

        Self {
            dist,
            hasher: Sha256::new(),
            elapsed: None,
        }
    }

    /// Return `true` with probability equal to the log‑normal CDF at `elapsed`.
    /// Deterministic completion: randomness comes from the evolving hash state
    /// based on provided bytes.
    ///
    /// * If `elapsed` is *far* below `target_length`, the chance is tiny.  
    /// * At `elapsed ≈ target_length`, chance ≈ 0.5 (slightly above 0.5 for
    ///   σ > 0).  
    /// * For very large `elapsed`, the probability approaches 1.0.
    /// Return `true` with probability equal to the CDF at `elapsed`.
    /// Internally this is now expressed via `remaining_probability`.
    pub fn is_complete(&mut self, bytes: &[u8]) -> bool {
        let elapsed_prev = self.elapsed.unwrap_or(1);
        let elapsed = elapsed_prev + bytes.len();

        self.hasher.update(bytes);

        let f_prev = self.dist.cdf(elapsed_prev as f64);
        let f_curr = self.dist.cdf(elapsed as f64);
        self.elapsed = Some(elapsed);

        let hazard = (f_curr - f_prev) / (1.0 - f_prev);

        // Snapshot the current hash without disturbing the ongoing state
        let hash_bytes = self.hasher.clone().finalize();
        let x = u64::from_be_bytes(hash_bytes[0..8].try_into().unwrap());
        let p_draw = ((x >> 11) as f64) * (1.0 / (1u64 << 53) as f64);

        p_draw < hazard
    }

    pub fn id(self) -> [u8; 32] {
        self.hasher.finalize().into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The CDF of the underlying LogNormal must increase with `elapsed`.
    #[test]
    fn probability_monotonic() {
        let shaper = ProbShaper::new(1_000, 500);
        assert!(shaper.dist.cdf(500.0) < shaper.dist.cdf(1_000.0));
        assert!(shaper.dist.cdf(1_000.0) < shaper.dist.cdf(2_000.0));
    }
}
