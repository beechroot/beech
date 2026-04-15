//! Access-path planning for the SQLite vtab boundary.
//!
//! `AccessPlan::select` chooses how the cursor will traverse the tree given
//! a set of candidate constraints offered by SQLite's `xBestIndex`. The
//! plan is then serialized into SQLite's `idx_str` so that `xFilter` can
//! reconstruct the search bound from the `args` slice without depending
//! on SQLite's iteration order.

use crate::Id;
use crate::Table;
use crate::query::ConstraintOp;
use apache_avro::AvroSchema;
use serde::{Deserialize, Serialize};

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;

/// What the cursor will do at scan time.
#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
pub struct AccessPlan {
    pub table_id: Id,
    /// Search constraints in key-part order. At most one entry per key
    /// part; all entries before the last are `Eq`; the last may be a range.
    pub search: Vec<SearchSlot>,
    /// True iff the natural left-to-right cursor traversal already
    /// satisfies the requested ORDER BY.
    pub preserves_order: bool,
    pub estimate: PlanEstimate,
}

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
pub struct SearchSlot {
    /// 0-based key-part index.
    pub key_part: i32, // Avro doesn't support unsigned
    /// Row-column index (for cursor compatibility).
    pub column: i32,
    pub op: ConstraintOp,
    /// 1-based; cursor pulls the value from `args[argv_index - 1]`.
    pub argv_index: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
pub struct PlanEstimate {
    pub estimated_cost: f64,
    pub estimated_rows: i64,
}

/// One candidate constraint as offered by SQLite to `xBestIndex`.
#[derive(Debug, Clone, Copy)]
pub struct CandidateConstraint {
    /// Row-column index from SQLite's `IndexInfoConstraint::column()`.
    pub column: i32,
    pub op: ConstraintOp,
}

impl AccessPlan {
    /// Choose an access path for `table` from the offered SQLite constraints.
    ///
    /// Algorithm: walk key parts from 0 upward. For each part, prefer an
    /// `Eq` constraint and continue; on the first part with no `Eq`, take a
    /// single range constraint if available and stop; on the first part with
    /// no usable constraint, stop. Constraints with op `Unknown` or on
    /// non-key columns are ignored.
    pub fn select(
        table: &Table,
        candidates: impl IntoIterator<Item = CandidateConstraint>,
        total_rows: i64,
    ) -> Self {
        let n_keys = table.key_columns.len();

        // Bucket candidates by key part. Each bucket holds (column, op) pairs.
        let mut by_part: Vec<Vec<(i32, ConstraintOp)>> = vec![vec![]; n_keys];
        for c in candidates {
            if c.op == ConstraintOp::Unknown {
                continue;
            }
            let Some(part) = table.column_key_index(c.column as usize) else {
                continue;
            };
            by_part[part].push((c.column, c.op.clone()));
        }

        let mut search = Vec::with_capacity(n_keys);
        let mut argv = 1;
        for (part_idx, part_candidates) in by_part.into_iter().enumerate() {
            // Prefer Eq for the prefix.
            let eq = part_candidates
                .iter()
                .find(|(_, op)| *op == ConstraintOp::Eq)
                .cloned();
            if let Some((column, op)) = eq {
                search.push(SearchSlot {
                    key_part: part_idx as i32,
                    column,
                    op,
                    argv_index: argv,
                });
                argv += 1;
                continue;
            }
            // No Eq on this part: take one range and stop.
            let range = part_candidates.into_iter().find(|(_, op)| {
                matches!(
                    op,
                    ConstraintOp::Lt | ConstraintOp::Le | ConstraintOp::Gt | ConstraintOp::Ge
                )
            });
            if let Some((column, op)) = range {
                search.push(SearchSlot {
                    key_part: part_idx as i32,
                    column,
                    op,
                    argv_index: argv,
                });
            }
            break;
        }

        let estimate = Self::estimate(n_keys, &search, total_rows);

        AccessPlan {
            table_id: table.id.clone(),
            search,
            preserves_order: false,
            estimate,
        }
    }

    /// First-cut cost/row heuristic. Deliberately simple — the goal is to
    /// give SQLite's planner a meaningful gradient between equality-pinned
    /// prefixes, range-bounded prefixes, and full scans, not to be precise.
    /// Tune later from real workloads.
    pub(crate) fn estimate(n_keys: usize, search: &[SearchSlot], total_rows: i64) -> PlanEstimate {
        let total = total_rows.max(1) as f64;
        let n_eq = search
            .iter()
            .take_while(|s| s.op == ConstraintOp::Eq)
            .count();
        let has_range = search
            .last()
            .map(|s| s.op != ConstraintOp::Eq)
            .unwrap_or(false);

        let (rows, cost) = if n_eq == n_keys && !has_range {
            // Pinpoint: full key, all equality.
            let cost = total.log2().max(1.0);
            (1.0, cost)
        } else if n_eq > 0 || has_range {
            // Prefix-bounded scan. Each Eq part divides rows by 10; a trailing
            // range halves the remainder.
            let mut rows = total / 10f64.powi(n_eq as i32);
            if has_range {
                rows *= 0.5;
            }
            let rows = rows.max(1.0);
            (rows, rows)
        } else {
            (total, total)
        };

        PlanEstimate {
            estimated_cost: cost,
            estimated_rows: rows.round() as i64,
        }
    }
}
