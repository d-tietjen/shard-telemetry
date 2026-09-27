use super::*;

/// Computes a stable BM25-shaped relevance score for a matched log message.
///
/// The storage index supplies the candidate set and this scorer only ranks
/// those candidates. It intentionally keeps document-frequency estimation
/// local to the request so a bounded top-k query does not require a global
/// scan or mutable statistics.
#[derive(Clone)]
pub(crate) struct RelevanceScorer {
    terms: Vec<Arc<str>>,
}

impl RelevanceScorer {
    pub(crate) fn terms(&self) -> &[Arc<str>] {
        &self.terms
    }

    pub(crate) fn from_request(request: &AnalyticsScanRequest) -> Self {
        fn add_term(terms: &mut Vec<Arc<str>>, value: &str) {
            let lowered = value.to_ascii_lowercase();
            if !lowered.is_empty() && !terms.iter().any(|known| known.as_ref() == lowered.as_str())
            {
                terms.push(Arc::from(lowered));
            }
        }
        fn collect_predicate(predicate: &LogPredicate, terms: &mut Vec<Arc<str>>) {
            match predicate {
                LogPredicate::Term(value) | LogPredicate::MessageToken { value, .. } => {
                    add_term(terms, value);
                }
                LogPredicate::MessagePhrase { terms: phrase, .. } => {
                    for value in phrase {
                        add_term(terms, value);
                    }
                }
                LogPredicate::MessageFuzzy { value, .. } => add_term(terms, value),
                LogPredicate::And(predicates) | LogPredicate::Or(predicates) => {
                    for predicate in predicates {
                        collect_predicate(predicate, terms);
                    }
                }
                LogPredicate::Not(predicate) => collect_predicate(predicate, terms),
                LogPredicate::MatchAll
                | LogPredicate::MatchNone
                | LogPredicate::Message(_)
                | LogPredicate::MessageRegex(_)
                | LogPredicate::MessageTokenRegex(_)
                | LogPredicate::MessageTokenPrefix { .. }
                | LogPredicate::FieldExists(_)
                | LogPredicate::Field { .. }
                | LogPredicate::FieldIn { .. }
                | LogPredicate::FieldRegex { .. }
                | LogPredicate::FieldNumeric { .. } => {}
            }
        }

        let mut terms = Vec::new();
        for term in &request.terms {
            add_term(&mut terms, term);
        }
        for term in &request.message_tokens {
            add_term(&mut terms, term);
        }
        for term in &request.case_insensitive_message_tokens {
            add_term(&mut terms, term);
        }
        collect_predicate(&request.predicate, &mut terms);
        Self { terms }
    }

    pub(crate) fn score(&self, message: &str) -> f64 {
        if self.terms.is_empty() {
            return 1.0;
        }
        let mut frequencies = vec![0_u32; self.terms.len()];
        let mut document_length = 0_u32;
        let mut score_token = |token: &[u8]| {
            document_length = document_length.saturating_add(1);
            for (index, expected) in self.terms.iter().enumerate() {
                let expected = expected.as_bytes();
                if token.len() == expected.len()
                    && token
                        .iter()
                        .zip(expected)
                        .all(|(left, right)| left.eq_ignore_ascii_case(right))
                {
                    frequencies[index] = frequencies[index].saturating_add(1);
                }
            }
        };
        let message = message.as_bytes();
        let mut start = 0usize;
        for (index, byte) in message.iter().copied().enumerate() {
            if crate::query::clickhouse_token_separator(byte) {
                if start < index {
                    score_token(&message[start..index]);
                }
                start = index.saturating_add(1);
            }
        }
        if start < message.len() {
            score_token(&message[start..]);
        }
        self.score_indexed(document_length, |term| {
            let expected = term.as_bytes();
            self.terms
                .iter()
                .position(|known| known.as_bytes() == expected)
                .map(|index| frequencies[index])
                .unwrap_or_default()
        })
    }

    /// Scores a document whose token frequencies were materialized by the
    /// structural frame index. Keeping the BM25 calculation here makes the
    /// indexed and fallback paths use exactly the same ranking semantics.
    pub(crate) fn score_indexed(
        &self,
        document_length: u32,
        mut frequency_for: impl FnMut(&str) -> u32,
    ) -> f64 {
        if self.terms.is_empty() {
            return 1.0;
        }
        let document_length = f64::from(document_length.max(1));
        let average_document_length = 12.0;
        let k1 = 1.2;
        let b = 0.75;
        let normalization = k1 * (1.0 - b + b * document_length / average_document_length);
        self.terms
            .iter()
            .map(|term| {
                let frequency = f64::from(frequency_for(term.as_ref()));
                if frequency == 0.0 {
                    return 0.0;
                }
                (frequency * (k1 + 1.0)) / (frequency + normalization)
            })
            .sum()
    }

    pub(crate) fn score_indexed_by_index(
        &self,
        document_length: u32,
        mut frequency_for: impl FnMut(usize) -> u32,
    ) -> f64 {
        if self.terms.is_empty() {
            return 1.0;
        }
        let document_length = f64::from(document_length.max(1));
        let normalization = 1.2 * (1.0 - 0.75 + 0.75 * document_length / 12.0);
        self.terms
            .iter()
            .enumerate()
            .map(|(index, _)| {
                let frequency = f64::from(frequency_for(index));
                if frequency == 0.0 {
                    return 0.0;
                }
                (frequency * 2.2) / (frequency + normalization)
            })
            .sum()
    }
}
