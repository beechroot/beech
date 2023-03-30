use crate::source::Store;
use crate::{BeechError, Id, Key, Page, Result, Table};
use avro_rs::types::Value;
use log::debug;
use ordered_float::OrderedFloat;
use std::cmp::Ordering;
use std::vec::Vec;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConstraintOp {
    Eq,
    Gt,
    Le,
    Lt,
    Ge,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Constraint {
    pub column: usize,
    pub op: ConstraintOp,
    pub has_index: bool,
}

#[derive(Debug)]
pub struct OrderBy {
    pub column: usize,
    pub desc: bool,
}

#[derive(Debug)]
pub struct Cursor {
    stack: Vec<(Id, usize)>,
    lower_bound: Option<Key>,
    pub table: Table,
    constraints: Vec<Vec<(Constraint, Value)>>, // one collection of constraints per key part
}

#[derive(Debug)]
pub struct OrdValue<'a>(pub &'a Value);

impl<'a> Eq for OrdValue<'a> {}
impl<'a> PartialEq for OrdValue<'a> {
    fn eq(&self, rhs: &Self) -> bool {
        matches!(self.partial_cmp(rhs), Some(Ordering::Equal))
    }
}
impl<'a> Ord for OrdValue<'a> {
    fn cmp(&self, rhs: &OrdValue) -> Ordering {
        match self.partial_cmp(rhs) {
            None => Ordering::Equal,
            Some(c) => c,
        }
    }
}
impl<'a> PartialOrd for OrdValue<'a> {
    fn partial_cmp(&self, rhs: &OrdValue) -> Option<Ordering> {
        let (&OrdValue(lhs), &OrdValue(rhs)) = (self, rhs);
        match lhs {
            Value::Null => {
                if *rhs == Value::Null {
                    Some(Ordering::Equal)
                } else {
                    Some(Ordering::Less)
                }
            }
            Value::Boolean(v0) => match rhs {
                Value::Null => Some(Ordering::Greater),
                Value::Boolean(v1) => Some(v0.cmp(v1)),
                Value::Int(v1) => Some(v0.cmp(&(*v1 != 0))),
                Value::Long(v1) => Some(v0.cmp(&(*v1 != 0))),
                Value::Float(v1) => Some(v0.cmp(&(*v1 != 0.0))),
                Value::Double(v1) => Some(v0.cmp(&(*v1 != 0.0))),
                _ => None,
            },
            Value::Int(v0) => match rhs {
                Value::Null => Some(Ordering::Greater),
                Value::Boolean(v1) => Some((*v0 != 0).cmp(v1)),
                Value::Int(v1) => Some(v0.cmp(v1)),
                Value::Long(v1) => Some((i64::from(*v0)).cmp(v1)),
                Value::Float(v1) => OrderedFloat(*v0 as f32).partial_cmp(&OrderedFloat(*v1)),
                Value::Double(v1) => OrderedFloat(f64::from(*v0)).partial_cmp(&OrderedFloat(*v1)),
                _ => None,
            },
            Value::Long(v0) => match rhs {
                Value::Null => Some(Ordering::Greater),
                Value::Boolean(v1) => Some((*v0 != 0).cmp(v1)),
                Value::Int(v1) => Some(v0.cmp(&(i64::from(*v1)))),
                Value::Long(v1) => Some(v0.cmp(v1)),
                Value::Float(v1) => OrderedFloat(*v0 as f32).partial_cmp(&OrderedFloat(*v1)),
                Value::Double(v1) => OrderedFloat(*v0 as f64).partial_cmp(&OrderedFloat(*v1)),
                _ => None,
            },
            Value::Float(v0) => match rhs {
                Value::Null => Some(Ordering::Greater),
                Value::Boolean(v1) => Some((*v0 != 0.0).cmp(v1)),
                Value::Int(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(*v1 as f32)),
                Value::Long(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(*v1 as f32)),
                Value::Float(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(*v1)),
                Value::Double(v1) => OrderedFloat(f64::from(*v0)).partial_cmp(&OrderedFloat(*v1)),
                _ => None,
            },
            Value::Double(v0) => match rhs {
                Value::Null => Some(Ordering::Greater),
                Value::Boolean(v1) => Some((*v0 != 0.0).cmp(v1)),
                Value::Int(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(f64::from(*v1))),
                Value::Long(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(*v1 as f64)),
                Value::Float(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(f64::from(*v1))),
                Value::Double(v1) => OrderedFloat(*v0).partial_cmp(&OrderedFloat(*v1)),
                _ => None,
            },
            x => match (x, rhs) {
                (Value::Bytes(v0), Value::Bytes(v1)) => Some(v0.cmp(v1)),
                (Value::String(v0), Value::String(v1)) => Some(v0.cmp(v1)),
                (Value::Fixed(sz0, v0), Value::Fixed(sz1, v1)) if sz0 == sz1 => Some(v0.cmp(v1)),
                (Value::Enum(pos0, _), Value::Enum(pos1, _)) => Some(pos0.cmp(pos1)),
                (Value::Union(bv0), Value::Union(bv1)) => OrdValue(&**bv0).partial_cmp(&OrdValue(&**bv1)),
                (_, _) => None,
            },
        }
    }
}

impl Constraint {
    pub fn new(column: usize, op: ConstraintOp) -> Self {
        Constraint {
            column,
            op,
            has_index: false,
        }
    }
    fn before_beginning(&self, reference_val: &Value, current_val: &Value) -> bool {
        if !self.has_index {
            return false;
        }
        let bb = match self.op {
            ConstraintOp::Gt | ConstraintOp::Eq | ConstraintOp::Ge => {
                let ov1 = OrdValue(current_val);
                let ov2 = OrdValue(reference_val);
                ov1 < ov2 || (self.op == ConstraintOp::Gt && ov1 == ov2)
            }
            _ => false,
        };
        bb
    }

    fn after_end(&self, reference_val: &Value, current_val: &Value) -> bool {
        if !self.has_index {
            return false;
        }
        match self.op {
            ConstraintOp::Lt | ConstraintOp::Eq | ConstraintOp::Le => {
                let ov1 = OrdValue(current_val);
                let ov2 = OrdValue(reference_val);
                ov1 > ov2 || (self.op == ConstraintOp::Lt && ov1 == ov2)
            }
            _ => false,
        }
    }
}
impl Cursor {
    pub fn new(t: &Table) -> Self {
        Cursor {
            stack: vec![],
            table: t.clone(),
            lower_bound: None,
            constraints: vec![],
        }
    }

    pub fn init(&mut self, usage: IndexUsage, values: Vec<Value>) {
        let key_size = self.table.metadata.key_columns.len();
        let mut constraints: Vec<Vec<(Constraint, Value)>> = vec![Default::default(); key_size];
        for (mut c, v) in usage.constraints.into_iter().zip(values).into_iter() {
            if let Some(col) = self.table.metadata.column_key_part(c.column) {
                c.has_index = true;
                if let Some(slot) = constraints.get_mut(col) {
                    slot.push((c, v));
                }
            }
        }
        self.constraints = constraints;
        self.lower_bound = None;
        self.stack = self.table.root.as_ref().map_or(Vec::new(), |r| vec![(r.clone(), 0)]);
    }

    pub fn advance_to_right(&mut self, source: &mut dyn Store) -> Result<()> {
        loop {
            match self.stack.pop() {
                Some((id, _)) => {
                    let p = source.get_page(&self.table, &id)?;
                    match p {
                        Page::Branch { children, .. } => {
                            let next_child_idx = children.len() - 1;
                            self.stack.push((id, next_child_idx));
                            self.stack.push((children[next_child_idx].clone(), 0))
                        }
                        Page::Leaf { values, .. } => {
                            self.stack.push((id, values.len() - 1));
                            return Ok(());
                        }
                    }
                }
                None => return Ok(()),
            }
        }
    }
    pub fn advance_to_left(&mut self, source: &mut dyn Store) -> Result<()> {
        while let Some((id, child)) = &self.stack.pop() {
            let p = source.get_page(&self.table, &id)?;
            if let Some(new_child) = self.find_next_child(p, *child) {
                self.stack.push((id.clone(), new_child));
                if let Page::Branch { children, .. } = p {
                    let new_child_id = children.get(new_child).ok_or(BeechError::Corrupt)?;
                    self.stack.push((new_child_id.clone(), 0));
                } else {
                    break;
                }
            } else {
                self.advance_stack(source)?;
            }
        }
        Ok(())
    }

    fn find_next_child(&mut self, p: &Page, starting_child: usize) -> Option<usize> {
        let s = &p.keys()[starting_child..];
        let location_result = s.binary_search_by(|e| {
            if self.should_skip(&self.lower_bound, e) {
                self.lower_bound = Some(e.clone());
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });
        // we're abusing the binary search function a bit, relying on this
        // in its documentation: "If the value is not found then
        // Result::Err is returned, containing the index where a matching
        // element could be inserted"
        // since our predicate above never returns "Equal", binary_search_by
        // should always return Err
        let loc = location_result.unwrap_err();
        let new_loc = loc + starting_child;
        if new_loc >= p.keys().len() && p.is_leaf() {
            None
        } else {
            Some(new_loc)
        }
    }

    fn advance_stack(&mut self, source: &mut dyn Store) -> Result<()> {
        while !self.stack.is_empty() {
            match &self.stack.pop() {
                Some((id, child)) => {
                    let (last_child, new_lower_bound) = {
                        let p = source.get_page(&self.table, &id)?;
                        (p.last_child(), p.keys().get(*child).cloned())
                    };
                    self.lower_bound = new_lower_bound;
                    if *child < last_child {
                        self.stack.push((id.clone(), child + 1));
                        break;
                    }
                }
                None => return Err(BeechError::Corrupt.into()),
            }
        }

        Ok(())
    }

    pub fn next(&mut self, source: &mut dyn Store) -> Result<()> {
        self.advance_stack(source)?;
        self.advance_to_left(source)?;
        if let Some((id, child)) = &self.stack.last() {
            let p = source.get_page(&self.table, &id)?;

            let maybe_key = p.keys().get(*child);
            if let Some(key) = maybe_key {
                if self.done_iterating(key) {
                    self.stack.clear();
                }
            }
        };
        Ok(())
    }

    pub fn current(&self) -> Option<&(Id, usize)> {
        self.stack.last()
    }

    // positive number counts up from the bottom of the stack
    pub fn stack_level(&self, lvl: isize) -> Option<&(Id, usize)> {
        if lvl <= 0 {
            let levels_down = (-1 * lvl) as usize;
            if levels_down >= self.stack.len() {
                None
            } else {
                self.stack.get(self.stack.len() - levels_down as usize - 1)
            }
        } else {
            self.stack.get(lvl as usize)
        }
    }

    pub fn eof(&self) -> bool {
        self.stack.is_empty()
    }

    fn should_skip(&self, maybe_k_start: &Option<Key>, k_end: &Key) -> bool {
        let maybe_k_start = maybe_k_start.as_ref();
        // first thing to determine is which keyparts are in-order
        // in the subtree bounded by k_start and k_end
        // if there is no k_start, the only one that can be assumed
        // to be in order is the first
        let in_order_depth = maybe_k_start
            .as_ref()
            .map(|k_start| {
                let klen = k_start.len();
                let mut kiter = k_start.iter().zip(k_end).enumerate();
                kiter
                    .find_map(|(i, (ka, kb))| {
                        if OrdValue(ka) != OrdValue(kb) {
                            Some(i + 1)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(klen)
            })
            .unwrap_or(1);

        for (i, (cs, end_part)) in self.constraints.iter().take(in_order_depth).zip(k_end).enumerate() {
            for (c, v) in cs {
                if let Some(start_part) = maybe_k_start.and_then(|k_start| k_start.get(i)) {
                    if c.after_end(v, start_part) {
                        return true;
                    }
                }
                if c.before_beginning(v, end_part) {
                    return true;
                }
            }
        }
        false
    }

    fn done_iterating(&self, k: &Key) -> bool {
        k.get(0)
            .map(|kv| {
                let first_constraints = &self.constraints[0];
                first_constraints.iter().any(|(c, v)| c.after_end(v, kv))
            })
            .unwrap_or(false)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct IndexUsage {
    pub table_id: Id,
    pub order_by_consumed: bool,
    pub estimated_cost: f64,
    constraints: Vec<Constraint>,
}

pub fn best_index(table: &Table, constraints: Vec<Constraint>, order_bys: Vec<OrderBy>) -> IndexUsage {
    let order_by_columns: Vec<usize> = order_bys.iter().map(|ob| ob.column).collect();
    let key_column_indexes: Vec<usize> = table.metadata.key_columns.iter().map(|(idx, _)| *idx).collect();
    let order_by_consumed = order_bys.iter().all(|ob| !ob.desc) && key_column_indexes.starts_with(&order_by_columns);
    let usable_constraints = constraints
        .iter()
        .any(|c| table.metadata.column_key_part(c.column).is_some());
    let estimated_cost = if usable_constraints {
        512.0
    } else {
        1024.0 * 1024.0 * 1024.0 * 1024.0
    };
    debug!("IndexUsage: {} {} {:?}", order_by_consumed, estimated_cost, constraints);
    IndexUsage {
        table_id: table.id.clone(),
        order_by_consumed,
        estimated_cost,
        constraints,
    }
}
