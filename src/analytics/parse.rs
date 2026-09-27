use super::*;

pub(crate) fn parse_scan_request(
    tenant: String,
    raw_query: Option<&str>,
) -> Result<AnalyticsScanRequest, LokiApiError> {
    let pairs = form_urlencoded::parse(raw_query.unwrap_or_default().as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let relations = pairs
        .iter()
        .filter(|(key, _)| key == "relation")
        .map(|(_, value)| value.as_str())
        .collect::<Vec<_>>();
    if relations.len() > 1 {
        return Err(LokiApiError::bad_request(
            "relation may be specified only once",
        ));
    }
    let relation = relations
        .first()
        .map_or(Some(AnalyticsRelation::Logs), |value| {
            AnalyticsRelation::parse(value)
        })
        .ok_or_else(|| LokiApiError::bad_request("unknown analytics relation"))?;
    let mut request = AnalyticsScanRequest::for_relation(tenant, relation);
    let mut columns_seen = false;
    let mut cardinality_seen = false;
    let mut order_seen = false;
    let mut wire_seen = false;
    let mut predicate_operator_seen = false;
    let mut predicate_any = false;
    let mut message_any = Vec::new();
    let mut message_min_match = None;
    let mut predicate_parts = Vec::new();
    for (key, value) in pairs {
        match key.as_str() {
            "relation" => {}
            "start_ns" => request.start_timestamp_unix_nanos = Some(parse_u64("start_ns", &value)?),
            "end_ns" => request.end_timestamp_unix_nanos = Some(parse_u64("end_ns", &value)?),
            "limit" => {
                request.limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| LokiApiError::bad_request("limit is not a usize"))?,
                );
            }
            "cardinality_only" => {
                if cardinality_seen {
                    return Err(LokiApiError::bad_request(
                        "cardinality_only may be specified only once",
                    ));
                }
                cardinality_seen = true;
                request.cardinality_only = match value.as_str() {
                    "1" | "true" => true,
                    "0" | "false" => false,
                    _ => {
                        return Err(LokiApiError::bad_request(
                            "cardinality_only is not a boolean",
                        ));
                    }
                };
            }
            "order" => {
                if order_seen {
                    return Err(LokiApiError::bad_request(
                        "order may be specified only once",
                    ));
                }
                order_seen = true;
                request.order = Some(match value.as_str() {
                    "timestamp_asc" => AnalyticsScanOrder::TimestampAscending,
                    "timestamp_desc" => AnalyticsScanOrder::TimestampDescending,
                    "score_desc" | "relevance_desc" => AnalyticsScanOrder::RelevanceDescending,
                    _ => return Err(LokiApiError::bad_request("unknown analytics order")),
                });
            }
            "predicate_operator" => {
                if predicate_operator_seen {
                    return Err(LokiApiError::bad_request(
                        "predicate_operator may be specified only once",
                    ));
                }
                predicate_operator_seen = true;
                predicate_any = match value.as_str() {
                    "and" => false,
                    "or" => true,
                    _ => {
                        return Err(LokiApiError::bad_request(
                            "predicate_operator must be and or or",
                        ));
                    }
                };
            }
            "wire" => {
                if wire_seen {
                    return Err(LokiApiError::bad_request("wire may be specified only once"));
                }
                wire_seen = true;
                request.wire_format = match value.as_str() {
                    "arrow" | "arrow_stream" => AnalyticsWireFormat::ArrowStream,
                    "rowbinary" => AnalyticsWireFormat::RowBinary,
                    "json" | "jsonl" | "ndjson" => AnalyticsWireFormat::JsonLines,
                    _ => return Err(LokiApiError::bad_request("unknown analytics wire format")),
                };
            }
            "term" => request.terms.push(Arc::from(value)),
            "message_token" => request.message_tokens.push(Arc::from(value)),
            "message_token_ci" => request
                .case_insensitive_message_tokens
                .push(Arc::from(value)),
            "message_any" => message_any.push(Arc::from(value)),
            "message_min_match" => {
                message_min_match =
                    Some(value.parse::<usize>().map_err(|_| {
                        LokiApiError::bad_request("message_min_match is not a usize")
                    })?);
            }
            "message_contains" => predicate_parts.push(LogPredicate::message(TextMatcher::new(
                value,
                TextMatchKind::Contains,
                CaseSensitivity::Insensitive,
            ))),
            "message_prefix" => predicate_parts.push(LogPredicate::message(TextMatcher::new(
                value,
                TextMatchKind::Prefix,
                CaseSensitivity::Insensitive,
            ))),
            "message_suffix" => predicate_parts.push(LogPredicate::message(TextMatcher::new(
                value,
                TextMatchKind::Suffix,
                CaseSensitivity::Insensitive,
            ))),
            "message_regex" | "message_regex_ci" => {
                let sensitivity = if key == "message_regex_ci" {
                    CaseSensitivity::Insensitive
                } else {
                    CaseSensitivity::Sensitive
                };
                let predicate = LogPredicate::message_regex(value, sensitivity)
                    .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                predicate_parts.push(predicate);
            }
            "message_token_regex" | "message_token_regex_ci" => {
                let sensitivity = if key == "message_token_regex_ci" {
                    CaseSensitivity::Insensitive
                } else {
                    CaseSensitivity::Sensitive
                };
                let predicate = LogPredicate::message_token_regex(value, sensitivity)
                    .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                predicate_parts.push(predicate);
            }
            "message_token_prefix" | "message_token_prefix_ci" => {
                let sensitivity = if key == "message_token_prefix_ci" {
                    CaseSensitivity::Insensitive
                } else {
                    CaseSensitivity::Sensitive
                };
                predicate_parts.push(LogPredicate::message_token_prefix(value, sensitivity));
            }
            "message_phrase" | "message_proximity" => {
                let (raw_terms, raw_gap) = value.split_once(':').unwrap_or((&value, "0"));
                let max_gap = raw_gap
                    .parse::<usize>()
                    .map_err(|_| LokiApiError::bad_request("message phrase gap is not a usize"))?;
                let terms = raw_terms
                    .split('|')
                    .filter(|term| !term.is_empty())
                    .map(Arc::<str>::from)
                    .collect::<Vec<_>>();
                if terms.is_empty() {
                    return Err(LokiApiError::bad_request(
                        "message phrase requires at least one term",
                    ));
                }
                predicate_parts.push(LogPredicate::message_phrase(
                    terms,
                    max_gap,
                    CaseSensitivity::Insensitive,
                ));
            }
            "message_fuzzy" => {
                let (term, raw_distance) = value.split_once(':').ok_or_else(|| {
                    LokiApiError::bad_request("message_fuzzy must be encoded as term:distance")
                })?;
                let distance = raw_distance
                    .parse::<u8>()
                    .map_err(|_| LokiApiError::bad_request("message_fuzzy distance is not a u8"))?;
                predicate_parts.push(LogPredicate::message_fuzzy(term, distance));
            }
            "message_like" => {
                let predicate = LogPredicate::message_token_regex(
                    wildcard_pattern_to_regex(&value),
                    CaseSensitivity::Insensitive,
                )
                .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                predicate_parts.push(predicate);
            }
            "message_not" => predicate_parts.push(LogPredicate::negate(
                LogPredicate::message_token(value, CaseSensitivity::Insensitive),
            )),
            "field_exists" => predicate_parts.push(LogPredicate::field_exists(value)),
            key if key.starts_with("field_equals.") => predicate_parts.push(
                LogPredicate::field_equals(&key["field_equals.".len()..], value),
            ),
            key if key.starts_with("field_contains.") => predicate_parts.push(LogPredicate::field(
                &key["field_contains.".len()..],
                TextMatcher::new(value, TextMatchKind::Contains, CaseSensitivity::Insensitive),
            )),
            key if key.starts_with("field_prefix.") => predicate_parts.push(LogPredicate::field(
                &key["field_prefix.".len()..],
                TextMatcher::new(value, TextMatchKind::Prefix, CaseSensitivity::Insensitive),
            )),
            key if key.starts_with("field_suffix.") => predicate_parts.push(LogPredicate::field(
                &key["field_suffix.".len()..],
                TextMatcher::new(value, TextMatchKind::Suffix, CaseSensitivity::Insensitive),
            )),
            key if key.starts_with("field_regex.") => {
                let field = &key["field_regex.".len()..];
                let predicate = LogPredicate::field_regex(field, value, CaseSensitivity::Sensitive)
                    .map_err(|error| LokiApiError::bad_request(error.to_string()))?;
                predicate_parts.push(predicate);
            }
            key if key.starts_with("field_in.") => {
                predicate_parts.push(LogPredicate::field_in(
                    &key["field_in.".len()..],
                    value.split('|'),
                ));
            }
            key if key.starts_with("field_numeric.") => {
                let field = &key["field_numeric.".len()..];
                let (operator, raw_value) = value.split_once(':').ok_or_else(|| {
                    LokiApiError::bad_request("field_numeric must be encoded as operator:value")
                })?;
                let comparison = match operator {
                    "eq" => NumericComparison::Equal,
                    "ne" => NumericComparison::NotEqual,
                    "lt" => NumericComparison::LessThan,
                    "le" => NumericComparison::LessThanOrEqual,
                    "gt" => NumericComparison::GreaterThan,
                    "ge" => NumericComparison::GreaterThanOrEqual,
                    _ => return Err(LokiApiError::bad_request("unknown numeric comparison")),
                };
                let number = raw_value
                    .parse::<i128>()
                    .map_err(|_| LokiApiError::bad_request("field_numeric value is not an i128"))?;
                predicate_parts.push(LogPredicate::field_numeric(field, comparison, number));
            }
            "trace_id" => request.trace_id = Some(parse_trace_id(&value)?),
            "trace_join_service" => request.trace_join_service = Some(Arc::from(value)),
            "distinct_trace_id" => {
                request.distinct_trace_id = match value.as_str() {
                    "1" | "true" => true,
                    "0" | "false" => false,
                    _ => {
                        return Err(LokiApiError::bad_request(
                            "distinct_trace_id is not a boolean",
                        ));
                    }
                };
            }
            "span_id" => request.span_id = Some(parse_span_id(&value)?),
            "series_id" => request.series_id = Some(parse_series_id(&value)?),
            "name" => request.name = Some(Arc::from(value)),
            "group_by" => {
                request.group_by = value
                    .split(',')
                    .map(|key| {
                        AnalyticsGroupKey::parse(key).ok_or_else(|| {
                            LokiApiError::bad_request(format!(
                                "unknown analytics group key {key:?}"
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if request.group_by.is_empty() {
                    return Err(LokiApiError::bad_request(
                        "group_by requires at least one key",
                    ));
                }
            }
            "group_limit" => {
                request.group_limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| LokiApiError::bad_request("group_limit is not a usize"))?,
                );
            }
            "group_order" => {
                request.group_order = match value.as_str() {
                    "count_desc" => AnalyticsGroupOrder::CountDescending,
                    "key_asc" => AnalyticsGroupOrder::KeyAscending,
                    _ => return Err(LokiApiError::bad_request("unknown analytics group order")),
                };
            }
            "columns" => {
                if columns_seen {
                    return Err(LokiApiError::bad_request(
                        "columns may be specified only once",
                    ));
                }
                columns_seen = true;
                request.columns = value
                    .split(',')
                    .map(|name| {
                        AnalyticsColumn::parse(name).ok_or_else(|| {
                            LokiApiError::bad_request(format!("unknown analytics column {name:?}"))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
            }
            key if key.starts_with("label.") => {
                push_field(&mut request.labels, key, "label.", &value)?;
            }
            key if key.starts_with("metadata.") => {
                push_field(&mut request.metadata, key, "metadata.", &value)?;
            }
            key if key.starts_with("attribute.") => {
                push_field(&mut request.attributes, key, "attribute.", &value)?;
            }
            key if key.starts_with("resource.") => {
                push_field(&mut request.resource_attributes, key, "resource.", &value)?;
            }
            key if key.starts_with("scope.") => {
                push_field(&mut request.scope_attributes, key, "scope.", &value)?;
            }
            unknown => {
                return Err(LokiApiError::bad_request(format!(
                    "unknown analytics parameter {unknown:?}"
                )));
            }
        }
    }
    if !message_any.is_empty() {
        let tokens = message_any
            .into_iter()
            .map(|value| LogPredicate::message_token(value, CaseSensitivity::Insensitive))
            .collect::<Vec<_>>();
        if let Some(minimum) = message_min_match {
            predicate_parts.push(min_match_predicate(tokens, minimum)?);
        } else {
            predicate_parts.push(LogPredicate::or(tokens));
        }
    } else if message_min_match.is_some() {
        return Err(LokiApiError::bad_request(
            "message_min_match requires at least one message_any parameter",
        ));
    }
    if predicate_any {
        predicate_parts.extend(request.terms.drain(..).map(LogPredicate::Term));
        predicate_parts.extend(
            request
                .message_tokens
                .drain(..)
                .map(|value| LogPredicate::message_token(value, CaseSensitivity::Sensitive)),
        );
        predicate_parts.extend(
            request
                .case_insensitive_message_tokens
                .drain(..)
                .map(|value| LogPredicate::message_token(value, CaseSensitivity::Insensitive)),
        );
        request.predicate = LogPredicate::or(predicate_parts);
    } else {
        request.predicate = LogPredicate::and(predicate_parts);
    }
    request.predicate_any = predicate_any;
    request.validate()?;
    Ok(request)
}

pub(super) fn min_match_predicate(
    predicates: Vec<LogPredicate>,
    minimum: usize,
) -> Result<LogPredicate, LokiApiError> {
    if minimum == 0 {
        return Ok(LogPredicate::MatchAll);
    }
    if minimum > predicates.len() {
        return Err(LokiApiError::bad_request(
            "message_min_match exceeds the number of message_any parameters",
        ));
    }
    let mut combinations = Vec::new();
    fn visit(
        predicates: &[LogPredicate],
        minimum: usize,
        start: usize,
        selected: &mut Vec<LogPredicate>,
        combinations: &mut Vec<LogPredicate>,
    ) {
        if selected.len() == minimum {
            combinations.push(LogPredicate::and(selected.clone()));
            return;
        }
        let remaining = minimum - selected.len();
        let last = predicates.len().saturating_sub(remaining);
        for index in start..=last {
            selected.push(predicates[index].clone());
            visit(predicates, minimum, index + 1, selected, combinations);
            selected.pop();
        }
    }
    visit(&predicates, minimum, 0, &mut Vec::new(), &mut combinations);
    if combinations.len() > 1_024 {
        return Err(LokiApiError::bad_request(
            "message_min_match expands to too many combinations",
        ));
    }
    Ok(LogPredicate::or(combinations))
}

pub(super) fn push_field(
    fields: &mut Vec<MetadataField>,
    key: &str,
    prefix: &str,
    value: &str,
) -> Result<(), LokiApiError> {
    let name = &key[prefix.len()..];
    if name.is_empty() {
        return Err(LokiApiError::bad_request(
            "attribute name must not be empty",
        ));
    }
    fields.push(MetadataField::new(name, value));
    Ok(())
}

pub(super) fn parse_u64(name: &str, value: &str) -> Result<u64, LokiApiError> {
    value
        .parse::<u64>()
        .map_err(|_| LokiApiError::bad_request(format!("{name} is not a u64")))
}

pub(super) fn parse_trace_id(value: &str) -> Result<TraceId, LokiApiError> {
    TraceId::from_bytes(decode_hex(value)?)
        .map_err(|_| LokiApiError::bad_request("trace_id is not a valid nonzero 128-bit hex ID"))
}

pub(super) fn parse_span_id(value: &str) -> Result<SpanId, LokiApiError> {
    SpanId::from_bytes(decode_hex(value)?)
        .map_err(|_| LokiApiError::bad_request("span_id is not a valid nonzero 64-bit hex ID"))
}

pub(super) fn parse_series_id(value: &str) -> Result<SeriesFingerprint, LokiApiError> {
    let bytes: [u8; 16] = decode_hex(value)?;
    Ok(SeriesFingerprint::from_raw(u128::from_be_bytes(bytes)))
}

pub(super) fn decode_hex<const N: usize>(value: &str) -> Result<[u8; N], LokiApiError> {
    if value.len() != N * 2 {
        return Err(LokiApiError::bad_request(
            "hex identifier has the wrong length",
        ));
    }
    let mut output = [0; N];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

pub(super) fn hex_nibble(value: u8) -> Result<u8, LokiApiError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        b'A'..=b'F' => Ok(value - b'A' + 10),
        _ => Err(LokiApiError::bad_request(
            "identifier contains non-hex bytes",
        )),
    }
}
