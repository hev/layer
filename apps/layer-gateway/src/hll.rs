//! HyperLogLog sketch for field-stats distinct counts above the value cap.
//!
//! Precision 14 (16,384 one-byte registers, 16 KiB, about 22 KiB as JSON) has
//! a relative standard error of `1.04 / sqrt(16384)`, about 0.81%. Two standard
//! deviations (about 95% of estimates) are 1.6% and 2% is 2.5 standard errors
//! (about 98.7%): the "about 2%" the field-stats contract documents. The estimator is
//! Ertl's improved estimator, which has no bias correction range to tune, so
//! the error holds from the value cap into the millions.
//!
//! The hash is xxh64 with a fixed seed. Sketches are persisted to Aerospike
//! and S3 and merged across gateways, so the hash must never change.

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use xxhash_rust::xxh64::xxh64;

pub const PRECISION: u32 = 14;
const REGISTERS: usize = 1 << PRECISION;
const SEED: u64 = 0x6865_7632_6c61_7972;

/// Relative standard error of an estimate, as a fraction.
pub const STANDARD_ERROR: f64 = 0.0081;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hll {
    registers: Vec<u8>,
}

impl Default for Hll {
    fn default() -> Self {
        Self {
            registers: vec![0; REGISTERS],
        }
    }
}

impl Hll {
    pub fn insert(&mut self, value: &str) {
        let hash = xxh64(value.as_bytes(), SEED);
        let index = (hash >> (64 - PRECISION)) as usize;
        let rest = hash << PRECISION;
        // Leading zeros of the remaining 50 bits, plus one; all-zero is 51.
        let rho = (rest.leading_zeros().min(64 - PRECISION) + 1) as u8;
        if rho > self.registers[index] {
            self.registers[index] = rho;
        }
    }

    pub fn merge(&mut self, other: &Hll) {
        for (a, b) in self.registers.iter_mut().zip(&other.registers) {
            *a = (*a).max(*b);
        }
    }

    pub fn estimate(&self) -> u64 {
        let q = (64 - PRECISION) as usize;
        let m = REGISTERS as f64;
        let mut histogram = vec![0u32; q + 2];
        for r in &self.registers {
            histogram[*r as usize] += 1;
        }
        let mut z = m * tau(1.0 - f64::from(histogram[q + 1]) / m);
        for k in (1..=q).rev() {
            z = 0.5 * (z + f64::from(histogram[k]));
        }
        z += m * sigma(f64::from(histogram[0]) / m);
        if z.is_infinite() || z == 0.0 {
            return 0;
        }
        (m * m / (2.0 * std::f64::consts::LN_2 * z)).round() as u64
    }

    fn encode(&self) -> String {
        B64.encode(&self.registers)
    }

    fn decode(raw: &str) -> Option<Self> {
        let registers = B64.decode(raw).ok()?;
        (registers.len() == REGISTERS).then_some(Self { registers })
    }
}

fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    loop {
        x *= x;
        let previous = z;
        z += x * y;
        y += y;
        if z == previous {
            return z;
        }
    }
}

fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    loop {
        x = x.sqrt();
        let previous = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z == previous {
            return z / 3.0;
        }
    }
}

impl Serialize for Hll {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.encode())
    }
}

impl<'de> Deserialize<'de> for Hll {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Hll::decode(&raw).ok_or_else(|| serde::de::Error::custom("invalid hll sketch"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sketch_of(n: u64, salt: &str) -> Hll {
        let mut h = Hll::default();
        for i in 0..n {
            h.insert(&format!("{salt}-{i}"));
        }
        h
    }

    #[test]
    fn empty_sketch_counts_zero() {
        assert_eq!(Hll::default().estimate(), 0);
    }

    #[test]
    fn duplicates_do_not_move_the_estimate() {
        let mut h = sketch_of(20_000, "d");
        let before = h.estimate();
        for i in 0..20_000 {
            h.insert(&format!("d-{i}"));
        }
        assert_eq!(h.estimate(), before);
    }

    #[test]
    fn merge_equals_sketch_of_union() {
        let a = sketch_of(30_000, "m");
        let mut b = Hll::default();
        for i in 15_000..60_000 {
            b.insert(&format!("m-{i}"));
        }
        let mut merged = a.clone();
        merged.merge(&b);
        assert_eq!(merged, sketch_of(60_000, "m"));
    }

    #[test]
    fn serde_round_trip_and_rejects_wrong_length() {
        let h = sketch_of(12_345, "s");
        let back: Hll = serde_json::from_str(&serde_json::to_string(&h).unwrap()).unwrap();
        assert_eq!(h, back);
        assert!(serde_json::from_str::<Hll>("\"AAAA\"").is_err());
    }

    /// Quality evidence across representative cardinalities, from just past the
    /// default value cap to millions, over independent key sets. The documented
    /// contract is statistical: the standard error is about 0.8%, nearly every
    /// estimate lands within 2% (2.5 standard errors, about 98.7% of them), and
    /// none lands beyond 3%. Run with `--nocapture` for the table.
    #[test]
    fn relative_error_matches_the_documented_two_percent() {
        let cardinalities = [10_001u64, 25_000, 50_000, 100_000, 500_000, 2_000_000];
        let (mut within, mut all) = (0u32, 0u32);
        for n in cardinalities {
            let trials = if n >= 500_000 { 4 } else { 24 };
            let (mut worst, mut total) = (0f64, 0f64);
            for t in 0..trials {
                let estimate = sketch_of(n, &format!("q{t}")).estimate() as f64;
                let err = (estimate - n as f64).abs() / n as f64;
                worst = worst.max(err);
                total += err;
                all += 1;
                if err < 0.02 {
                    within += 1;
                }
            }
            let mean = total / f64::from(trials);
            println!("hll n={n} trials={trials} worst={worst:.4} mean={mean:.4}");
            assert!(worst < 0.03, "n={n} worst relative error {worst}");
            assert!(mean < 0.012, "n={n} mean relative error {mean}");
        }
        let share = f64::from(within) / f64::from(all);
        println!("hll within 2%: {within}/{all} = {share:.3}");
        assert!(share >= 0.95, "only {share} of estimates within 2%");
    }
}
