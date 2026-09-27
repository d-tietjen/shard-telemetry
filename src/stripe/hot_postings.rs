use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct OrdinalRun {
    pub(super) first: u32,
    pub(super) last: u32,
}

#[derive(Debug, Default)]
pub(super) struct HotPostingList {
    pub(super) runs: Vec<OrdinalRun>,
    pub(super) cardinality: usize,
}

impl HotPostingList {
    pub(super) fn push(&mut self, ordinal: u32) {
        self.push_range(ordinal, ordinal);
    }

    pub(super) fn push_range(&mut self, first: u32, final_ordinal: u32) {
        debug_assert!(first <= final_ordinal);
        let added = (final_ordinal - first) as usize + 1;
        if let Some(last_run) = self.runs.last_mut()
            && last_run.last.checked_add(1) == Some(first)
        {
            last_run.last = final_ordinal;
            self.cardinality += added;
            return;
        }
        debug_assert!(
            self.runs
                .last()
                .is_none_or(|last_run| last_run.last < first),
            "hot postings must be appended in ordinal order"
        );
        self.runs.push(OrdinalRun {
            first,
            last: final_ordinal,
        });
        self.cardinality += added;
    }

    pub(super) fn is_empty_in(&self, start: u32, end: u32) -> bool {
        self.runs
            .binary_search_by(|run| {
                if run.last < start {
                    std::cmp::Ordering::Less
                } else if run.first >= end {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_err()
    }

    pub(super) fn cardinality_in(&self, start: u32, end: u32) -> usize {
        if start == 0 && self.runs.last().is_none_or(|run| run.last < end) {
            return self.cardinality;
        }
        let (first_run, end_run) = self.run_range(start, end);
        self.runs[first_run..end_run]
            .iter()
            .map(|run| {
                let first = run.first.max(start);
                let last = run.last.min(end.saturating_sub(1));
                (last - first) as usize + 1
            })
            .try_fold(0usize, |total, count| total.checked_add(count))
            .unwrap_or(usize::MAX)
    }

    pub(super) fn collect_in(
        &self,
        start: u32,
        end: u32,
        order: QueryOrder,
        limit: Option<usize>,
    ) -> Vec<u32> {
        if start >= end {
            return Vec::new();
        }
        let take = limit.unwrap_or(usize::MAX);
        let (first_run, end_run) = self.run_range(start, end);
        let mut ordinals = Vec::new();
        match order {
            QueryOrder::OldestFirst => {
                for run in &self.runs[first_run..end_run] {
                    if ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).take(take - ordinals.len()));
                }
            }
            QueryOrder::NewestFirst => {
                for run in self.runs[first_run..end_run].iter().rev() {
                    if ordinals.len() == take {
                        break;
                    }
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    ordinals.extend((first..=last).rev().take(take - ordinals.len()));
                }
            }
        }
        ordinals
    }

    pub(super) fn contains(&self, ordinal: u32) -> bool {
        self.runs
            .binary_search_by(|run| {
                if run.last < ordinal {
                    std::cmp::Ordering::Less
                } else if run.first > ordinal {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    pub(super) fn visit_in(
        &self,
        start: u32,
        end: u32,
        order: QueryOrder,
        mut visit: impl FnMut(u32) -> bool,
    ) {
        if start >= end {
            return;
        }
        let (first_run, end_run) = self.run_range(start, end);
        match order {
            QueryOrder::OldestFirst => {
                for run in &self.runs[first_run..end_run] {
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    for ordinal in first..=last {
                        if !visit(ordinal) {
                            return;
                        }
                    }
                }
            }
            QueryOrder::NewestFirst => {
                for run in self.runs[first_run..end_run].iter().rev() {
                    let first = run.first.max(start);
                    let last = run.last.min(end.saturating_sub(1));
                    for ordinal in (first..=last).rev() {
                        if !visit(ordinal) {
                            return;
                        }
                    }
                }
            }
        }
    }

    pub(super) fn run_range(&self, start: u32, end: u32) -> (usize, usize) {
        let first = self.runs.partition_point(|run| run.last < start);
        let end = self.runs.partition_point(|run| run.first < end);
        (first.min(end), end)
    }
}

pub(super) fn collect_hot_posting_intersection(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    order: QueryOrder,
    limit: Option<usize>,
) -> Vec<u32> {
    let Some(first) = postings.first() else {
        return Vec::new();
    };
    let take = limit.unwrap_or(usize::MAX);
    if take == 0 {
        return Vec::new();
    }
    let mut ordinals = Vec::with_capacity(take.min(first.cardinality));
    first.visit_in(start, end, order, |ordinal| {
        if postings[1..]
            .iter()
            .all(|postings| postings.contains(ordinal))
        {
            ordinals.push(ordinal);
        }
        ordinals.len() < take
    });
    ordinals
}

pub(super) fn union_sorted_ordinals(existing: &mut Vec<u32>, incoming: Vec<u32>) {
    if incoming.is_empty() {
        return;
    }
    if existing.is_empty() {
        *existing = incoming;
        return;
    }
    let mut merged = Vec::with_capacity(existing.len().saturating_add(incoming.len()));
    let mut existing_index = 0usize;
    let mut incoming_index = 0usize;
    while existing_index < existing.len() && incoming_index < incoming.len() {
        match existing[existing_index].cmp(&incoming[incoming_index]) {
            std::cmp::Ordering::Less => {
                merged.push(existing[existing_index]);
                existing_index += 1;
            }
            std::cmp::Ordering::Greater => {
                merged.push(incoming[incoming_index]);
                incoming_index += 1;
            }
            std::cmp::Ordering::Equal => {
                merged.push(existing[existing_index]);
                existing_index += 1;
                incoming_index += 1;
            }
        }
    }
    merged.extend_from_slice(&existing[existing_index..]);
    merged.extend_from_slice(&incoming[incoming_index..]);
    *existing = merged;
}

pub(super) fn collect_hot_posting_union(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Vec<u32> {
    if postings.is_empty() || start >= end || limit == Some(0) {
        return Vec::new();
    }
    if postings.len() == 1 {
        return postings[0].collect_in(start, end, QueryOrder::OldestFirst, limit);
    }
    if postings.len() == 2 {
        return collect_two_hot_posting_union(postings[0], postings[1], start, end, limit);
    }

    let take = limit.unwrap_or(usize::MAX);
    let capacity = postings
        .iter()
        .map(|posting| hot_posting_cardinality_in(posting, start, end))
        .fold(0usize, usize::saturating_add)
        .min(take);
    let mut ordinals = Vec::with_capacity(capacity);
    let mut cursors = vec![(0usize, 0u32, 0u32); postings.len()];
    let mut heap = BinaryHeap::<Reverse<(u32, usize)>>::new();

    for (posting_index, posting) in postings.iter().enumerate() {
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let Some(run) = posting.runs.get(run_index) else {
            continue;
        };
        if run.first >= end {
            continue;
        }
        let current = run.first.max(start);
        let last = run.last.min(end - 1);
        cursors[posting_index] = (run_index, current, last);
        heap.push(Reverse((current, posting_index)));
    }

    let mut previous = None;
    while let Some(Reverse((ordinal, posting_index))) = heap.pop() {
        if previous != Some(ordinal) {
            ordinals.push(ordinal);
            previous = Some(ordinal);
            if ordinals.len() == take {
                break;
            }
        }

        let (mut run_index, mut current, mut last) = cursors[posting_index];
        if current < last {
            current += 1;
            cursors[posting_index] = (run_index, current, last);
            heap.push(Reverse((current, posting_index)));
            continue;
        }

        run_index += 1;
        let posting = postings[posting_index];
        while let Some(run) = posting.runs.get(run_index) {
            if run.first >= end {
                break;
            }
            if run.last >= start {
                current = run.first.max(start);
                last = run.last.min(end - 1);
                cursors[posting_index] = (run_index, current, last);
                heap.push(Reverse((current, posting_index)));
                break;
            }
            run_index += 1;
        }
    }
    ordinals
}

pub(super) fn visit_hot_posting_union(
    postings: &[&HotPostingList],
    start: u32,
    end: u32,
    mut visit: impl FnMut(u32) -> bool,
) -> bool {
    if postings.is_empty() || start >= end {
        return true;
    }
    if postings.len() == 1 {
        let mut keep_going = true;
        postings[0].visit_in(start, end, QueryOrder::OldestFirst, |ordinal| {
            keep_going = visit(ordinal);
            keep_going
        });
        return keep_going;
    }
    if postings.len() == 2 {
        let mut left = HotPostingCursor::new(postings[0], start, end);
        let mut right = HotPostingCursor::new(postings[1], start, end);
        while left.is_some() || right.is_some() {
            let ordinal = match (left.as_ref(), right.as_ref()) {
                (Some(left), Some(right)) => left.current.min(right.current),
                (Some(left), None) => left.current,
                (None, Some(right)) => right.current,
                (None, None) => break,
            };
            if !visit(ordinal) {
                return false;
            }
            if left
                .as_ref()
                .is_some_and(|cursor| cursor.current == ordinal)
                && !left.as_mut().expect("left posting cursor exists").advance()
            {
                left = None;
            }
            if right
                .as_ref()
                .is_some_and(|cursor| cursor.current == ordinal)
                && !right
                    .as_mut()
                    .expect("right posting cursor exists")
                    .advance()
            {
                right = None;
            }
        }
        return true;
    }

    let mut cursors = vec![(0usize, 0u32, 0u32); postings.len()];
    let mut heap = BinaryHeap::<Reverse<(u32, usize)>>::new();
    for (posting_index, posting) in postings.iter().enumerate() {
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let Some(run) = posting.runs.get(run_index) else {
            continue;
        };
        if run.first >= end {
            continue;
        }
        let current = run.first.max(start);
        let last = run.last.min(end - 1);
        cursors[posting_index] = (run_index, current, last);
        heap.push(Reverse((current, posting_index)));
    }

    let mut previous = None;
    while let Some(Reverse((ordinal, posting_index))) = heap.pop() {
        if previous != Some(ordinal) {
            if !visit(ordinal) {
                return false;
            }
            previous = Some(ordinal);
        }

        let (mut run_index, mut current, mut last) = cursors[posting_index];
        if current < last {
            current += 1;
            cursors[posting_index] = (run_index, current, last);
            heap.push(Reverse((current, posting_index)));
            continue;
        }

        run_index += 1;
        let posting = postings[posting_index];
        while let Some(run) = posting.runs.get(run_index) {
            if run.first >= end {
                break;
            }
            if run.last >= start {
                current = run.first.max(start);
                last = run.last.min(end - 1);
                cursors[posting_index] = (run_index, current, last);
                heap.push(Reverse((current, posting_index)));
                break;
            }
            run_index += 1;
        }
    }
    true
}

pub(super) struct HotPostingCursor<'a> {
    posting: &'a HotPostingList,
    start: u32,
    end: u32,
    run_index: usize,
    current: u32,
    last: u32,
}

impl<'a> HotPostingCursor<'a> {
    pub(super) fn new(posting: &'a HotPostingList, start: u32, end: u32) -> Option<Self> {
        if start >= end {
            return None;
        }
        let run_index = posting.runs.partition_point(|run| run.last < start);
        let run = posting.runs.get(run_index)?;
        if run.first >= end {
            return None;
        }
        Some(Self {
            posting,
            start,
            end,
            run_index,
            current: run.first.max(start),
            last: run.last.min(end - 1),
        })
    }

    pub(super) fn advance(&mut self) -> bool {
        if self.current < self.last {
            self.current += 1;
            return true;
        }
        self.run_index += 1;
        while let Some(run) = self.posting.runs.get(self.run_index) {
            if run.first >= self.end {
                return false;
            }
            if run.last >= self.start {
                self.current = run.first.max(self.start);
                self.last = run.last.min(self.end - 1);
                return true;
            }
            self.run_index += 1;
        }
        false
    }
}

pub(super) fn collect_two_hot_posting_union(
    left: &HotPostingList,
    right: &HotPostingList,
    start: u32,
    end: u32,
    limit: Option<usize>,
) -> Vec<u32> {
    let take = limit.unwrap_or(usize::MAX);
    let capacity = hot_posting_cardinality_in(left, start, end)
        .saturating_add(hot_posting_cardinality_in(right, start, end))
        .min(take);
    let mut ordinals = Vec::with_capacity(capacity);
    let mut left = HotPostingCursor::new(left, start, end);
    let mut right = HotPostingCursor::new(right, start, end);
    while left.is_some() || right.is_some() {
        let ordinal = match (left.as_ref(), right.as_ref()) {
            (Some(left), Some(right)) => left.current.min(right.current),
            (Some(left), None) => left.current,
            (None, Some(right)) => right.current,
            (None, None) => break,
        };
        ordinals.push(ordinal);
        if ordinals.len() == take {
            break;
        }
        if left
            .as_ref()
            .is_some_and(|cursor| cursor.current == ordinal)
            && !left.as_mut().expect("left posting cursor exists").advance()
        {
            left = None;
        }
        if right
            .as_ref()
            .is_some_and(|cursor| cursor.current == ordinal)
            && !right
                .as_mut()
                .expect("right posting cursor exists")
                .advance()
        {
            right = None;
        }
    }
    ordinals
}

pub(super) fn hot_posting_cardinality_in(posting: &HotPostingList, start: u32, end: u32) -> usize {
    if start == 0 && posting.runs.last().is_none_or(|run| run.last < end) {
        posting.cardinality
    } else {
        posting.cardinality_in(start, end)
    }
}
