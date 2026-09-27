use super::*;

pub(super) fn parse_settings() -> Result<Settings, Box<dyn Error>> {
    let mut arguments = env::args_os().skip(1);
    let input = arguments
        .next()
        .map(PathBuf::from)
        .ok_or("usage: shard-telemetry-structural-bench <docker-json.log> [--report PATH] [--output-dir PATH] [--limit-bytes N] [--block-bytes N] [--workers N] [--locality enabled|disabled] [--dictionary disabled|realtime] [--index disabled|persistent]")?;
    let mut settings = Settings {
        input,
        report_path: None,
        limit_bytes: DEFAULT_LIMIT_BYTES,
        block_bytes: DEFAULT_BLOCK_BYTES,
        workers: 1,
        output_dir: None,
        locality_routing: false,
        realtime_dictionary: false,
        persistent_query_index: false,
    };
    while let Some(flag) = arguments.next() {
        match flag.to_string_lossy().as_ref() {
            "--report" => {
                settings.report_path = Some(PathBuf::from(
                    arguments.next().ok_or("--report requires a path")?,
                ));
            }
            "--output-dir" => {
                settings.output_dir = Some(PathBuf::from(
                    arguments.next().ok_or("--output-dir requires a path")?,
                ));
            }
            "--limit-bytes" => {
                settings.limit_bytes = parse_byte_count(
                    &arguments
                        .next()
                        .ok_or("--limit-bytes requires a value")?
                        .to_string_lossy(),
                )?;
            }
            "--block-bytes" => {
                settings.block_bytes = usize::try_from(parse_byte_count(
                    &arguments
                        .next()
                        .ok_or("--block-bytes requires a value")?
                        .to_string_lossy(),
                )?)?;
            }
            "--workers" => {
                settings.workers = arguments
                    .next()
                    .ok_or("--workers requires a value")?
                    .to_string_lossy()
                    .parse()?;
            }
            "--locality" => {
                settings.locality_routing = match arguments
                    .next()
                    .ok_or("--locality requires enabled or disabled")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "enabled" => true,
                    "disabled" => false,
                    value => {
                        return Err(
                            format!("--locality must be enabled or disabled, got {value}").into(),
                        );
                    }
                };
            }
            "--dictionary" => {
                settings.realtime_dictionary = match arguments
                    .next()
                    .ok_or("--dictionary requires disabled or realtime")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "disabled" => false,
                    "realtime" => true,
                    value => {
                        return Err(format!(
                            "--dictionary must be disabled or realtime, got {value}"
                        )
                        .into());
                    }
                };
            }
            "--index" => {
                settings.persistent_query_index = match arguments
                    .next()
                    .ok_or("--index requires disabled or persistent")?
                    .to_string_lossy()
                    .as_ref()
                {
                    "disabled" => false,
                    "persistent" => true,
                    value => {
                        return Err(
                            format!("--index must be disabled or persistent, got {value}").into(),
                        );
                    }
                };
            }
            _ => return Err(format!("unknown argument: {}", flag.to_string_lossy()).into()),
        }
    }
    if settings.limit_bytes == 0 || settings.block_bytes == 0 || settings.workers == 0 {
        return Err("byte limits must be nonzero".into());
    }
    if settings.realtime_dictionary && settings.locality_routing {
        return Err(
            "the real-time dictionary benchmark currently requires --locality disabled".into(),
        );
    }
    if settings.persistent_query_index && settings.locality_routing {
        return Err(
            "the persistent query-index benchmark currently requires --locality disabled".into(),
        );
    }
    Ok(settings)
}

pub(super) fn parse_byte_count(input: &str) -> Result<u64, Box<dyn Error>> {
    let trimmed = input.trim();
    let split = trimmed
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(trimmed.len());
    let (digits, suffix) = trimmed.split_at(split);
    if digits.is_empty() {
        return Err(format!("invalid byte count: {input}").into());
    }
    let multiplier = match suffix.to_ascii_lowercase().as_str() {
        "" | "b" => 1,
        "kib" => 1024,
        "mib" => 1024 * 1024,
        "gib" => 1024 * 1024 * 1024,
        _ => return Err(format!("unsupported byte suffix: {suffix}").into()),
    };
    digits
        .parse::<u64>()?
        .checked_mul(multiplier)
        .ok_or_else(|| format!("byte count overflows u64: {input}").into())
}
