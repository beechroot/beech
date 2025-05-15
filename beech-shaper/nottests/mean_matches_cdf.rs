//! Verify that the observed mean of `is_complete` matches the
//! theoretical CDF value with a one‑in‑a‑million false‑failure rate.

use prob_shaper::ProbShaper;
use statrs::distribution::{ContinuousCDF, LogNormal};

#[test]
fn mean_converges_to_cdf_within_millionth_error() {
    // ----- design parameters ---------------------------------------------------
    const N: usize = 250_000; // trials
    const Z: f64 = 4.753_424_308; // Φ⁻¹(1‑α/2) with α = 1 e‑6 (two‑tailed)

    // ----- shaper under test ---------------------------------------------------
    let target = 1_000_usize;
    let sigma = 0.6_f64;
    let mut shaper = ProbShaper::new(target, sigma);

    // Point at which we evaluate the CDF (centre gives worst‑case variance)
    let elapsed = target;

    // ----- expected probability -----------------------------------------------
    // Re‑build the same LogNormal(μ, σ) as in `ProbShaper::new`
    let mu = (target as f64).ln() - 0.5 * sigma * sigma;
    let dist = LogNormal::new(mu, sigma).expect("valid lognormal");
    let p = dist.cdf(elapsed as f64); // theoretical completion prob.

    // ----- sampling ------------------------------------------------------------
    let successes = (0..N)
        .filter(|i| {
            // use the loop index as deterministic per‑call entropy
            let bytes = i.to_le_bytes(); // 8‑byte slice
            shaper.is_complete(elapsed, &bytes)
        })
        .count();
    let mean = successes as f64 / N as f64;

    // ----- statistical band ----------------------------------------------------
    // Standard error for Bernoulli mean
    let se = (p * (1.0 - p) / N as f64).sqrt();
    let margin = Z * se;

    assert!(
        (mean - p).abs() <= margin,
        "Empirical mean {:.6} differs from expected {:.6} by more than ±{:.6} \
         (N={}, σ≈{:.6})",
        mean,
        p,
        margin,
        N,
        se
    );
}
