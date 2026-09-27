use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StructuralRelation {
    Descendant,
    Ancestor,
    Child,
    Parent,
    Sibling,
}

#[derive(Debug, Clone)]
pub(super) enum SpansetExpr {
    Selector(TraceFilter),
    And(Box<Self>, Box<Self>),
    Or(Box<Self>, Box<Self>),
    Structural {
        left: Box<Self>,
        right: Box<Self>,
        relation: StructuralRelation,
        union: bool,
        negated: bool,
    },
}

impl SpansetExpr {
    pub(super) fn parse(input: &str) -> Result<Self, TraceqlError> {
        let input = trim_enclosing_parentheses(input.trim());
        if input.is_empty() {
            return Ok(Self::Selector(TraceFilter::True));
        }
        if let Some((index, token)) = find_top_level_operator(input, &["||"]) {
            return Ok(Self::Or(
                Box::new(Self::parse(&input[..index])?),
                Box::new(Self::parse(&input[index + token.len()..])?),
            ));
        }
        if let Some((index, token)) = find_top_level_operator(input, &["&&"]) {
            return Ok(Self::And(
                Box::new(Self::parse(&input[..index])?),
                Box::new(Self::parse(&input[index + token.len()..])?),
            ));
        }
        if let Some((index, token)) = find_top_level_operator(
            input,
            &[
                "!>>", "!<<", "&>>", "&<<", "!>", "!<", "!~", "&>", "&<", "&~", ">>", "<<", ">",
                "<", "~",
            ],
        ) {
            let (relation, union, negated) = match token {
                ">>" => (StructuralRelation::Descendant, false, false),
                "<<" => (StructuralRelation::Ancestor, false, false),
                ">" => (StructuralRelation::Child, false, false),
                "<" => (StructuralRelation::Parent, false, false),
                "~" => (StructuralRelation::Sibling, false, false),
                "&>>" => (StructuralRelation::Descendant, true, false),
                "&<<" => (StructuralRelation::Ancestor, true, false),
                "&>" => (StructuralRelation::Child, true, false),
                "&<" => (StructuralRelation::Parent, true, false),
                "&~" => (StructuralRelation::Sibling, true, false),
                "!>>" => (StructuralRelation::Descendant, false, true),
                "!<<" => (StructuralRelation::Ancestor, false, true),
                "!>" => (StructuralRelation::Child, false, true),
                "!<" => (StructuralRelation::Parent, false, true),
                "!~" => (StructuralRelation::Sibling, false, true),
                _ => unreachable!("operator table is exhaustive"),
            };
            return Ok(Self::Structural {
                left: Box::new(Self::parse(&input[..index])?),
                right: Box::new(Self::parse(&input[index + token.len()..])?),
                relation,
                union,
                negated,
            });
        }
        Ok(Self::Selector(TraceFilter::parse(input)?))
    }

    pub(super) fn evaluate(&self, spans: &[DurableSpan]) -> Vec<usize> {
        match self {
            Self::Selector(filter) => spans
                .iter()
                .enumerate()
                .filter_map(|(index, span)| filter.matches(span, spans).then_some(index))
                .collect(),
            Self::And(left, right) => {
                let left = left.evaluate(spans);
                let right = right.evaluate(spans);
                if left.is_empty() || right.is_empty() {
                    Vec::new()
                } else {
                    ordered_union(left, right)
                }
            }
            Self::Or(left, right) => ordered_union(left.evaluate(spans), right.evaluate(spans)),
            Self::Structural {
                left,
                right,
                relation,
                union,
                negated,
            } => {
                let left = left.evaluate(spans);
                let right = right.evaluate(spans);
                let mut matching_left = Vec::new();
                let mut matching_right = Vec::new();
                for right_index in right {
                    let related = left.iter().copied().filter(|left_index| {
                        spans_related(spans, *left_index, right_index, *relation)
                    });
                    let related = related.collect::<Vec<_>>();
                    if (*negated && related.is_empty()) || (!*negated && !related.is_empty()) {
                        matching_right.push(right_index);
                        if *union && !*negated {
                            matching_left.extend(related);
                        }
                    }
                }
                if *union {
                    ordered_union(matching_left, matching_right)
                } else {
                    sorted_unique(matching_right)
                }
            }
        }
    }

    pub(super) fn exact_trace_id(&self) -> Option<TraceId> {
        match self {
            Self::Selector(filter) => filter.exact_trace_id(),
            Self::And(left, right) | Self::Structural { left, right, .. } => {
                left.exact_trace_id().or_else(|| right.exact_trace_id())
            }
            Self::Or(_, _) => None,
        }
    }
}

pub(super) fn sorted_unique(mut indexes: Vec<usize>) -> Vec<usize> {
    indexes.sort_unstable();
    indexes.dedup();
    indexes
}

pub(super) fn ordered_union(mut left: Vec<usize>, right: Vec<usize>) -> Vec<usize> {
    left.extend(right);
    sorted_unique(left)
}

pub(super) fn spans_related(
    spans: &[DurableSpan],
    left_index: usize,
    right_index: usize,
    relation: StructuralRelation,
) -> bool {
    if left_index == right_index {
        return false;
    }
    let left = &spans[left_index];
    let right = &spans[right_index];
    match relation {
        StructuralRelation::Descendant => is_ancestor(spans, left.span_id, right_index, false),
        StructuralRelation::Ancestor => is_ancestor(spans, right.span_id, left_index, false),
        StructuralRelation::Child => right.parent_span_id == Some(left.span_id),
        StructuralRelation::Parent => left.parent_span_id == Some(right.span_id),
        StructuralRelation::Sibling => {
            left.parent_span_id.is_some() && left.parent_span_id == right.parent_span_id
        }
    }
}

pub(super) fn is_ancestor(
    spans: &[DurableSpan],
    ancestor: crate::SpanId,
    descendant_index: usize,
    include_self: bool,
) -> bool {
    let mut current = if include_self {
        Some(spans[descendant_index].span_id)
    } else {
        spans[descendant_index].parent_span_id
    };
    for _ in 0..spans.len() {
        let Some(span_id) = current else {
            return false;
        };
        if span_id == ancestor {
            return true;
        }
        current = spans
            .iter()
            .find(|span| span.span_id == span_id)
            .and_then(|span| span.parent_span_id);
    }
    false
}

pub(super) fn find_top_level_operator<'a>(
    input: &'a str,
    operators: &[&'a str],
) -> Option<(usize, &'a str)> {
    let mut quoted = false;
    let mut escaped = false;
    let mut braces = 0_u32;
    let mut parentheses = 0_u32;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
            continue;
        }
        if quoted {
            continue;
        }
        match character {
            '{' => braces = braces.saturating_add(1),
            '}' => braces = braces.saturating_sub(1),
            '(' => parentheses = parentheses.saturating_add(1),
            ')' => parentheses = parentheses.saturating_sub(1),
            _ if braces == 0 && parentheses == 0 => {
                if let Some(operator) = operators
                    .iter()
                    .copied()
                    .find(|operator| input[index..].starts_with(operator))
                {
                    return Some((index, operator));
                }
            }
            _ => {}
        }
    }
    None
}

pub(super) fn trim_enclosing_parentheses(mut input: &str) -> &str {
    loop {
        let Some(inner) = input
            .strip_prefix('(')
            .and_then(|value| value.strip_suffix(')'))
        else {
            return input;
        };
        if find_matching_parenthesis(input) != Some(input.len() - 1) {
            return input;
        }
        input = inner.trim();
    }
}

pub(super) fn find_matching_parenthesis(input: &str) -> Option<usize> {
    let mut depth = 0_u32;
    let mut quoted = false;
    let mut escaped = false;
    for (index, character) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if quoted && character == '\\' {
            escaped = true;
            continue;
        }
        if character == '"' {
            quoted = !quoted;
        } else if !quoted && character == '(' {
            depth = depth.saturating_add(1);
        } else if !quoted && character == ')' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(index);
            }
        }
    }
    None
}
