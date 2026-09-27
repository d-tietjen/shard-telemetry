use super::*;

impl CompressionBlockCollator {
    /// Creates preallocated collation state for one stripe.
    pub fn new(
        config: CompressionLocalityConfig,
        target_block_bytes: u64,
    ) -> TelemetryResult<Self> {
        config.validate().map_err(TelemetryError::InvalidConfig)?;
        if target_block_bytes == 0 {
            return Err(TelemetryError::InvalidConfig(
                "locality target_block_bytes must be nonzero",
            ));
        }
        let allocated_state_bytes = estimated_state_bytes(&config);
        let shard_capacity = config.max_compression_shards;
        Ok(Self {
            config,
            target_block_bytes,
            shards: Vec::with_capacity(shard_capacity),
            split_explorations: [SplitExploration::default(); SPLIT_EXPLORATION_SLOTS],
            stats: CompressionLocalityStats {
                allocated_state_bytes,
                ..CompressionLocalityStats::default()
            },
        })
    }

    /// Returns whether block collation is enabled.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Returns a snapshot of cumulative collation diagnostics.
    #[must_use]
    pub fn stats(&self) -> CompressionLocalityStats {
        let mut stats = self.stats;
        stats.active_compression_shards = self.shards.len();
        stats
    }

    /// Returns current read-only compression-shard profiles.
    pub fn compression_shards(
        &self,
    ) -> impl ExactSizeIterator<Item = CompressionShardProfile> + '_ {
        self.shards.iter().map(|shard| shard.snapshot)
    }

    /// Selects the tentative block collector for one record.
    ///
    /// This is only an ingress hint. The final placement is calculated when the
    /// complete block is scored and may differ after outlier filtering.
    #[must_use]
    pub fn tentative_placement(
        &self,
        source: CompressionCohortId,
        fingerprint: MessageFingerprint,
    ) -> CompressionPlacement {
        let temperature = CompressionTemperature::new(fingerprint.locality_signature);
        if !self.config.enabled {
            return CompressionPlacement::base(source, temperature);
        }
        if let Some((index, distance)) =
            self.nearest_shard(source, temperature, fingerprint.shape_hash)
            && distance <= self.config.max_assignment_distance
        {
            return CompressionPlacement {
                placement_id: self.shards[index].snapshot.placement_id,
                temperature,
                granularity: LocalityGranularity::Collated,
                distance_to_shard: distance,
                internal_variance_q8: 0,
            };
        }
        CompressionPlacement::base(source, temperature)
    }

    /// Scores, recursively splits, and assigns one complete candidate block.
    ///
    /// Membership lists are packed as `u32` indices. `bytes-handoff` splits the
    /// packed left prefix from the right tail without per-record channels or
    /// cloned log payloads.
    pub fn collate(
        &mut self,
        source: CompressionCohortId,
        home: CompressionPlacementId,
        records: &[CompressionLocalityRecord],
    ) -> Vec<CompressionBlockAssignment> {
        if records.is_empty() {
            return Vec::new();
        }
        self.stats.observations = self
            .stats
            .observations
            .saturating_add(u64::try_from(records.len()).unwrap_or(u64::MAX));
        let root = BlockMembership::Contiguous(0..records.len());

        let mut work = VecDeque::from([WorkBlock {
            membership: root,
            depth: 0,
        }]);
        let explore_splits = self.should_explore_splits(source);
        let mut split_attempted = false;
        let mut leaves = Vec::new();
        while let Some(block) = work.pop_front() {
            let scored = score_membership(records, &block.membership);
            let score = scored.score;
            self.observe_score(score);
            let nearest = self.nearest_shard(source, score.temperature, score.shape_hash);
            let closer_to_other_shard = nearest.is_some_and(|(index, distance)| {
                let nearest_id = self.shards[index].snapshot.placement_id;
                nearest_id != home
                    && self
                        .profile_distance(home, score.temperature, score.shape_hash)
                        .is_none_or(|home_distance| distance < home_distance)
            });
            let should_split = self.config.enabled
                && explore_splits
                && block.depth < self.config.max_split_depth
                && (score.internal_variance_q8 > self.config.split_variance_q8
                    || closer_to_other_shard);

            if should_split {
                split_attempted = true;
                if let Some((left, right)) =
                    split_membership(records, &block.membership, &self.config, scored)
                {
                    self.stats.blocks_split = self.stats.blocks_split.saturating_add(1);
                    self.stats.subblocks_created = self.stats.subblocks_created.saturating_add(2);
                    self.stats.handoff_membership_bytes =
                        self.stats.handoff_membership_bytes.saturating_add(
                            u64::try_from(left.encoded_len().saturating_add(right.encoded_len()))
                                .unwrap_or(u64::MAX),
                        );
                    work.push_back(WorkBlock {
                        membership: left,
                        depth: block.depth.saturating_add(1),
                    });
                    work.push_back(WorkBlock {
                        membership: right,
                        depth: block.depth.saturating_add(1),
                    });
                    continue;
                }
            }
            leaves.push((block.membership, score));
        }

        let mut assignments = Vec::with_capacity(leaves.len());
        for (membership, score) in leaves {
            let placement = self.assign_leaf(source, records, &membership, score);
            let record_count = u64::try_from(score.record_count).unwrap_or(u64::MAX);
            if placement.granularity == LocalityGranularity::Base {
                self.stats.base_placements =
                    self.stats.base_placements.saturating_add(record_count);
            } else {
                self.stats.collated_placements =
                    self.stats.collated_placements.saturating_add(record_count);
            }
            if placement.placement_id != home {
                self.stats.records_reassigned =
                    self.stats.records_reassigned.saturating_add(record_count);
                self.stats.bytes_reassigned = self
                    .stats
                    .bytes_reassigned
                    .saturating_add(score.source_bytes);
            }
            assignments.push(CompressionBlockAssignment {
                placement,
                score,
                membership,
            });
        }
        assignments.sort_unstable_by_key(|assignment| {
            assignment.record_indices().next().unwrap_or(usize::MAX)
        });
        let specialized = assignments
            .iter()
            .any(|assignment| assignment.placement.granularity == LocalityGranularity::Collated);
        self.observe_split_exploration(source, split_attempted, specialized);
        assignments
    }

    fn should_explore_splits(&mut self, source: CompressionCohortId) -> bool {
        if !self.config.enabled
            || self
                .shards
                .iter()
                .any(|shard| shard.snapshot.source_cohort == source)
        {
            return true;
        }
        let slot = &mut self.split_explorations[usize::try_from(splitmix64(source.get()))
            .unwrap_or(0)
            & (SPLIT_EXPLORATION_SLOTS - 1)];
        if !slot.occupied || slot.source_tag != source.get() {
            *slot = SplitExploration {
                source_tag: source.get(),
                occupied: true,
                ..SplitExploration::default()
            };
        }
        if slot.failed_explorations < SPLIT_FAILURES_BEFORE_BACKOFF
            || slot.blocks_since_exploration >= SPLIT_BACKOFF_BLOCKS
        {
            slot.blocks_since_exploration = 0;
            true
        } else {
            slot.blocks_since_exploration = slot.blocks_since_exploration.saturating_add(1);
            self.stats.split_explorations_suppressed =
                self.stats.split_explorations_suppressed.saturating_add(1);
            false
        }
    }

    fn observe_split_exploration(
        &mut self,
        source: CompressionCohortId,
        attempted: bool,
        specialized: bool,
    ) {
        if !attempted && !specialized {
            return;
        }
        let slot = &mut self.split_explorations[usize::try_from(splitmix64(source.get()))
            .unwrap_or(0)
            & (SPLIT_EXPLORATION_SLOTS - 1)];
        if slot.occupied && slot.source_tag == source.get() {
            if specialized {
                slot.failed_explorations = 0;
                slot.blocks_since_exploration = 0;
            } else if attempted {
                slot.failed_explorations = slot.failed_explorations.saturating_add(1);
            }
        }
    }

    fn assign_leaf(
        &mut self,
        source: CompressionCohortId,
        records: &[CompressionLocalityRecord],
        membership: &BlockMembership,
        score: CompressionBlockScore,
    ) -> CompressionPlacement {
        if !self.config.enabled || score.internal_variance_q8 > self.config.max_shard_variance_q8 {
            return CompressionPlacement {
                internal_variance_q8: score.internal_variance_q8,
                ..CompressionPlacement::base(source, score.temperature)
            };
        }

        let selected = self
            .nearest_shard(source, score.temperature, score.shape_hash)
            .filter(|(_, distance)| *distance <= self.config.max_assignment_distance)
            .or_else(|| {
                (score.source_bytes >= self.config.min_admission_bytes.min(self.target_block_bytes)
                    && self.shards.len() < self.config.max_compression_shards)
                    .then(|| {
                        let index = self.admit_shard(source, score.temperature, score.shape_hash);
                        (index, 0)
                    })
            });

        let Some((index, distance)) = selected else {
            return CompressionPlacement {
                internal_variance_q8: score.internal_variance_q8,
                ..CompressionPlacement::base(source, score.temperature)
            };
        };
        self.observe_shard(index, records, membership, score);
        CompressionPlacement {
            placement_id: self.shards[index].snapshot.placement_id,
            temperature: score.temperature,
            granularity: LocalityGranularity::Collated,
            distance_to_shard: distance,
            internal_variance_q8: score.internal_variance_q8,
        }
    }

    fn nearest_shard(
        &self,
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Option<(usize, u8)> {
        self.shards
            .iter()
            .enumerate()
            .filter(|(_, shard)| shard.snapshot.source_cohort == source)
            .map(|(index, shard)| {
                (
                    index,
                    locality_distance(
                        temperature,
                        shape_hash,
                        shard.snapshot.temperature,
                        shard.snapshot.shape_hash,
                    ),
                    shard.snapshot.variance_q8,
                    shard.snapshot.placement_id,
                )
            })
            .min_by_key(|(_, distance, variance, placement)| {
                (*distance, *variance, placement.get())
            })
            .map(|(index, distance, _, _)| (index, distance))
    }

    fn profile_distance(
        &self,
        placement_id: CompressionPlacementId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> Option<u8> {
        self.shards
            .iter()
            .find(|shard| shard.snapshot.placement_id == placement_id)
            .map(|shard| {
                locality_distance(
                    temperature,
                    shape_hash,
                    shard.snapshot.temperature,
                    shard.snapshot.shape_hash,
                )
            })
    }

    fn admit_shard(
        &mut self,
        source: CompressionCohortId,
        temperature: CompressionTemperature,
        shape_hash: u64,
    ) -> usize {
        let mut placement_id =
            CompressionPlacementId::from_temperature(source, temperature, shape_hash);
        if self
            .shards
            .iter()
            .any(|shard| shard.snapshot.placement_id == placement_id)
        {
            placement_id = CompressionPlacementId::new(splitmix64(
                placement_id.get() ^ u64::try_from(self.shards.len()).unwrap_or(u64::MAX),
            ));
        }
        self.shards.push(CompressionShard {
            snapshot: CompressionShardProfile {
                placement_id,
                source_cohort: source,
                temperature,
                shape_hash,
                variance_q8: 0,
                blocks: 0,
                source_bytes: 0,
            },
            one_weights: [0; 16],
            total_weight: 0,
            shape_vote_weight: 0,
        });
        self.shards.len() - 1
    }

    fn observe_shard(
        &mut self,
        index: usize,
        records: &[CompressionLocalityRecord],
        membership: &BlockMembership,
        score: CompressionBlockScore,
    ) {
        let shard = &mut self.shards[index];
        if shard.total_weight > u64::MAX / 4 {
            shard.total_weight >>= 1;
            shard.shape_vote_weight >>= 1;
            for weight in &mut shard.one_weights {
                *weight >>= 1;
            }
        }
        for record_index in membership.indices() {
            let record = records[record_index];
            let weight = locality_weight(record.source_bytes);
            shard.total_weight = shard.total_weight.saturating_add(weight);
            for bit in 0..16 {
                if record.fingerprint.locality_signature & (1u16 << bit) != 0 {
                    shard.one_weights[bit] = shard.one_weights[bit].saturating_add(weight);
                }
            }
        }
        shard.snapshot.temperature = CompressionTemperature::new(centroid_from_weights(
            &shard.one_weights,
            shard.total_weight,
        ));
        if shard.snapshot.blocks == 0 || shard.snapshot.shape_hash == score.shape_hash {
            shard.snapshot.shape_hash = score.shape_hash;
            shard.shape_vote_weight = shard.shape_vote_weight.saturating_add(score.source_bytes);
        } else if shard.shape_vote_weight > score.source_bytes {
            shard.shape_vote_weight = shard.shape_vote_weight.saturating_sub(score.source_bytes);
        } else {
            shard.snapshot.shape_hash = score.shape_hash;
            shard.shape_vote_weight = score.source_bytes.saturating_sub(shard.shape_vote_weight);
        }
        shard.snapshot.variance_q8 = if shard.snapshot.blocks == 0 {
            score.internal_variance_q8
        } else {
            u16::try_from(
                (u32::from(shard.snapshot.variance_q8) * 7 + u32::from(score.internal_variance_q8))
                    / 8,
            )
            .expect("variance EWMA remains u16")
        };
        shard.snapshot.blocks = shard.snapshot.blocks.saturating_add(1);
        shard.snapshot.source_bytes = shard
            .snapshot
            .source_bytes
            .saturating_add(score.source_bytes);
    }

    fn observe_score(&mut self, score: CompressionBlockScore) {
        self.stats.blocks_scored = self.stats.blocks_scored.saturating_add(1);
        self.stats.max_internal_variance_q8 = self
            .stats
            .max_internal_variance_q8
            .max(score.internal_variance_q8);
    }
}
