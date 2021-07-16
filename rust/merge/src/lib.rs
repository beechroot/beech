use std::collections::BinaryHeap;
use std::cmp::Reverse;

pub struct IterMerger<I, II>
where
    I: Iterator<Item = II>,
    II: Ord,
{
    inputs: Vec<I>,
    heap: BinaryHeap<(Option<Reverse<II>>, usize)>,
}

impl<I, II> IterMerger<I, II>
where
    I: Iterator<Item = II>,
    II: Ord,
{
    pub fn new(mut inputs: Vec<I>) -> Self {
        IterMerger {
            heap: inputs
                .iter_mut()
                .enumerate()
                .map(|(n, i)| (i.next().map(Reverse), n))
                .collect(),
            inputs,
        }
    }
}

impl<I, II> Iterator for IterMerger<I, II>
where
    I: Iterator<Item = II>,
    II: Ord,
{
    // we will be counting with usize
    type Item = II;

    // next() is the only required method
    fn next(&mut self) -> Option<II> {
        let nv = self.heap.pop();
        if let Some((v, n)) = nv {
            let nextv = self.inputs
                .get_mut(n)
                .and_then(|i| i.next())
                .map(Reverse);
            self.heap.push((nextv, n));
            v.map(|x| x.0)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn simple() {
        let first = vec![1, 3].into_iter();
        let second = vec![2, 4].into_iter();
        let mut merged = IterMerger::new(vec![first, second]);
        assert_eq!(1, merged.next().unwrap());
        assert_eq!(2, merged.next().unwrap());
        assert_eq!(3, merged.next().unwrap());
        assert_eq!(4, merged.next().unwrap());
        assert_eq!(None, merged.next());
    }
}
