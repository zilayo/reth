use super::{
    metrics::StaticFileProviderMetrics, writer::StaticFileWriters, LoadedJar,
    StaticFileJarProvider, StaticFileProviderRW, StaticFileProviderRWRefMut,
};
use crate::{
    to_range, BlockHashReader, BlockNumReader, BlockReader, BlockSource, HeaderProvider,
    ReceiptProvider, StageCheckpointReader, StatsReader, TransactionVariant, TransactionsProvider,
    TransactionsProviderExt,
};
use alloy_consensus::{
    transaction::{SignerRecoverable, TransactionMeta},
    Header,
};
use alloy_eips::{eip2718::Encodable2718, BlockHashOrNumber};
use alloy_primitives::{
    b256, keccak256, Address, BlockHash, BlockNumber, TxHash, TxNumber, B256, U256,
};
use dashmap::DashMap;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::RwLock;
use reth_chainspec::{ChainInfo, ChainSpecProvider, EthChainSpec};
use reth_db::{
    lockfile::StorageLock,
    static_file::{
        iter_static_files, BlockHashMask, BodyIndicesMask, HeaderMask, HeaderWithHashMask,
        ReceiptMask, StaticFileCursor, TDWithHashMask, TransactionMask,
    },
};
use reth_db_api::{
    cursor::DbCursorRO,
    models::StoredBlockBodyIndices,
    table::{Decompress, Table, Value},
    tables,
    transaction::DbTx,
};
use reth_ethereum_primitives::{Receipt, TransactionSigned};
use reth_nippy_jar::{NippyJar, NippyJarChecker, CONFIG_FILE_EXTENSION};
use reth_node_types::{FullNodePrimitives, NodePrimitives};
use reth_primitives_traits::{RecoveredBlock, SealedHeader, SignedTransaction};
use reth_stages_types::{PipelineTarget, StageId};
use reth_static_file_types::{
    find_fixed_range, HighestStaticFiles, SegmentHeader, SegmentRangeInclusive, StaticFileSegment,
    DEFAULT_BLOCKS_PER_STATIC_FILE,
};
use reth_storage_api::{BlockBodyIndicesProvider, DBProvider};
use reth_storage_errors::provider::{ProviderError, ProviderResult};
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap},
    fmt::Debug,
    marker::PhantomData,
    ops::{Deref, Range, RangeBounds, RangeInclusive},
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, mpsc, Arc},
};
use tracing::{debug, info, trace, warn};

/// Alias type for a map that can be queried for block ranges from a transaction
/// segment respectively. It uses `TxNumber` to represent the transaction end of a static file
/// range.
type SegmentRanges = HashMap<StaticFileSegment, BTreeMap<TxNumber, SegmentRangeInclusive>>;

/// Access mode on a static file provider. RO/RW.
#[derive(Debug, Default, PartialEq, Eq)]
pub enum StaticFileAccess {
    /// Read-only access.
    #[default]
    RO,
    /// Read-write access.
    RW,
}

impl StaticFileAccess {
    /// Returns `true` if read-only access.
    pub const fn is_read_only(&self) -> bool {
        matches!(self, Self::RO)
    }

    /// Returns `true` if read-write access.
    pub const fn is_read_write(&self) -> bool {
        matches!(self, Self::RW)
    }
}

/// [`StaticFileProvider`] manages all existing [`StaticFileJarProvider`].
///
/// "Static files" contain immutable chain history data, such as:
///  - transactions
///  - headers
///  - receipts
///
/// This provider type is responsible for reading and writing to static files.
#[derive(Debug)]
pub struct StaticFileProvider<N>(pub(crate) Arc<StaticFileProviderInner<N>>);

impl<N> Clone for StaticFileProvider<N> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Creates a new [`StaticFileProvider`] with the given [`StaticFileAccess`].
    fn new(path: impl AsRef<Path>, access: StaticFileAccess) -> ProviderResult<Self> {
        let provider = Self(Arc::new(StaticFileProviderInner::new(path, access)?));
        provider.initialize_index()?;
        Ok(provider)
    }

    /// Creates a new [`StaticFileProvider`] with read-only access.
    ///
    /// Set `watch_directory` to `true` to track the most recent changes in static files. Otherwise,
    /// new data won't be detected or queryable.
    ///
    /// Watching is recommended if the read-only provider is used on a directory that an active node
    /// instance is modifying.
    ///
    /// See also [`StaticFileProvider::watch_directory`].
    pub fn read_only(path: impl AsRef<Path>, watch_directory: bool) -> ProviderResult<Self> {
        let provider = Self::new(path, StaticFileAccess::RO)?;

        if watch_directory {
            provider.watch_directory();
        }

        Ok(provider)
    }

    /// Creates a new [`StaticFileProvider`] with read-write access.
    pub fn read_write(path: impl AsRef<Path>) -> ProviderResult<Self> {
        Self::new(path, StaticFileAccess::RW)
    }

    /// Watches the directory for changes and updates the in-memory index when modifications
    /// are detected.
    ///
    /// This may be necessary, since a non-node process that owns a [`StaticFileProvider`] does not
    /// receive `update_index` notifications from a node that appends/truncates data.
    pub fn watch_directory(&self) {
        let provider = self.clone();
        std::thread::spawn(move || {
            let (tx, rx) = std::sync::mpsc::channel();
            let mut watcher = RecommendedWatcher::new(
                move |res| tx.send(res).unwrap(),
                notify::Config::default(),
            )
            .expect("failed to create watcher");

            watcher
                .watch(&provider.path, RecursiveMode::NonRecursive)
                .expect("failed to watch path");

            // Some backends send repeated modified events
            let mut last_event_timestamp = None;

            while let Ok(res) = rx.recv() {
                match res {
                    Ok(event) => {
                        // We only care about modified data events
                        if !matches!(
                            event.kind,
                            notify::EventKind::Modify(_) |
                                notify::EventKind::Create(_) |
                                notify::EventKind::Remove(_)
                        ) {
                            continue
                        }

                        // We only trigger a re-initialization if a configuration file was
                        // modified. This means that a
                        // static_file_provider.commit() was called on the node after
                        // appending/truncating rows
                        for segment in event.paths {
                            // Ensure it's a file with the .conf extension
                            if segment
                                .extension()
                                .is_none_or(|s| s.to_str() != Some(CONFIG_FILE_EXTENSION))
                            {
                                continue
                            }

                            // Ensure it's well formatted static file name
                            if StaticFileSegment::parse_filename(
                                &segment.file_stem().expect("qed").to_string_lossy(),
                            )
                            .is_none()
                            {
                                continue
                            }

                            // If we can read the metadata and modified timestamp, ensure this is
                            // not an old or repeated event.
                            if let Ok(current_modified_timestamp) =
                                std::fs::metadata(&segment).and_then(|m| m.modified())
                            {
                                if last_event_timestamp.is_some_and(|last_timestamp| {
                                    last_timestamp >= current_modified_timestamp
                                }) {
                                    continue
                                }
                                last_event_timestamp = Some(current_modified_timestamp);
                            }

                            info!(target: "providers::static_file", updated_file = ?segment.file_stem(), "re-initializing static file provider index");
                            if let Err(err) = provider.initialize_index() {
                                warn!(target: "providers::static_file", "failed to re-initialize index: {err}");
                            }
                            break
                        }
                    }

                    Err(err) => warn!(target: "providers::watcher", "watch error: {err:?}"),
                }
            }
        });
    }
}

impl<N: NodePrimitives> Deref for StaticFileProvider<N> {
    type Target = StaticFileProviderInner<N>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// [`StaticFileProviderInner`] manages all existing [`StaticFileJarProvider`].
#[derive(Debug)]
pub struct StaticFileProviderInner<N> {
    /// Maintains a map which allows for concurrent access to different `NippyJars`, over different
    /// segments and ranges.
    map: DashMap<(BlockNumber, StaticFileSegment), LoadedJar>,
    /// Min static file range for each segment.
    /// This index is initialized on launch to keep track of the lowest, non-expired static file
    /// per segment.
    ///
    /// This tracks the lowest static file per segment together with the block range in that
    /// file. E.g. static file is batched in 500k block intervals then the lowest static file
    /// is [0..499K], and the block range is start = 0, end = 499K.
    /// This index is mainly used to History expiry, which targets transactions, e.g. pre-merge
    /// history expiry would lead to removing all static files below the merge height.
    static_files_min_block: RwLock<HashMap<StaticFileSegment, SegmentRangeInclusive>>,
    /// This is an additional index that tracks the expired height, this will track the highest
    /// block number that has been expired (missing). The first, non expired block is
    /// `expired_history_height + 1`.
    ///
    /// This is effectively the transaction range that has been expired:
    /// [`StaticFileProvider::delete_transactions_below`] and mirrors
    /// `static_files_min_block[transactions] - blocks_per_file`.
    ///
    /// This additional tracker exists for more efficient lookups because the node must be aware of
    /// the expired height.
    earliest_history_height: AtomicU64,
    /// Max static file block for each segment
    static_files_max_block: RwLock<HashMap<StaticFileSegment, u64>>,
    /// Available static file block ranges on disk indexed by max transactions.
    static_files_tx_index: RwLock<SegmentRanges>,
    /// Directory where `static_files` are located
    path: PathBuf,
    /// Maintains a writer set of [`StaticFileSegment`].
    writers: StaticFileWriters<N>,
    /// Metrics for the static files.
    metrics: Option<Arc<StaticFileProviderMetrics>>,
    /// Access rights of the provider.
    access: StaticFileAccess,
    /// Number of blocks per file.
    blocks_per_file: u64,
    /// Write lock for when access is [`StaticFileAccess::RW`].
    _lock_file: Option<StorageLock>,
    /// Node primitives
    _pd: PhantomData<N>,
}

impl<N: NodePrimitives> StaticFileProviderInner<N> {
    /// Creates a new [`StaticFileProviderInner`].
    fn new(path: impl AsRef<Path>, access: StaticFileAccess) -> ProviderResult<Self> {
        let _lock_file = if access.is_read_write() {
            StorageLock::try_acquire(path.as_ref()).map_err(ProviderError::other)?.into()
        } else {
            None
        };

        let provider = Self {
            map: Default::default(),
            writers: Default::default(),
            static_files_min_block: Default::default(),
            earliest_history_height: Default::default(),
            static_files_max_block: Default::default(),
            static_files_tx_index: Default::default(),
            path: path.as_ref().to_path_buf(),
            metrics: None,
            access,
            blocks_per_file: DEFAULT_BLOCKS_PER_STATIC_FILE,
            _lock_file,
            _pd: Default::default(),
        };

        Ok(provider)
    }

    pub const fn is_read_only(&self) -> bool {
        self.access.is_read_only()
    }

    /// Each static file has a fixed number of blocks. This gives out the range where the requested
    /// block is positioned.
    pub const fn find_fixed_range(&self, block: BlockNumber) -> SegmentRangeInclusive {
        find_fixed_range(block, self.blocks_per_file)
    }
}

impl<N: NodePrimitives> StaticFileProvider<N> {
    /// Set a custom number of blocks per file.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn with_custom_blocks_per_file(self, blocks_per_file: u64) -> Self {
        let mut provider =
            Arc::try_unwrap(self.0).expect("should be called when initializing only");
        provider.blocks_per_file = blocks_per_file;
        Self(Arc::new(provider))
    }

    /// Enables metrics on the [`StaticFileProvider`].
    pub fn with_metrics(self) -> Self {
        let mut provider =
            Arc::try_unwrap(self.0).expect("should be called when initializing only");
        provider.metrics = Some(Arc::new(StaticFileProviderMetrics::default()));
        Self(Arc::new(provider))
    }

    /// Reports metrics for the static files.
    pub fn report_metrics(&self) -> ProviderResult<()> {
        let Some(metrics) = &self.metrics else { return Ok(()) };

        let static_files = iter_static_files(&self.path).map_err(ProviderError::other)?;
        for (segment, ranges) in static_files {
            let mut entries = 0;
            let mut size = 0;

            for (block_range, _) in &ranges {
                let fixed_block_range = self.find_fixed_range(block_range.start());
                let jar_provider = self
                    .get_segment_provider(segment, || Some(fixed_block_range), None)?
                    .ok_or_else(|| {
                        ProviderError::MissingStaticFileBlock(segment, block_range.start())
                    })?;

                entries += jar_provider.rows();

                let data_size = reth_fs_util::metadata(jar_provider.data_path())
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let index_size = reth_fs_util::metadata(jar_provider.index_path())
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let offsets_size = reth_fs_util::metadata(jar_provider.offsets_path())
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();
                let config_size = reth_fs_util::metadata(jar_provider.config_path())
                    .map(|metadata| metadata.len())
                    .unwrap_or_default();

                size += data_size + index_size + offsets_size + config_size;
            }

            metrics.record_segment(segment, size, ranges.len(), entries);
        }

        Ok(())
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and block.
    pub fn get_segment_provider_from_block(
        &self,
        segment: StaticFileSegment,
        block: BlockNumber,
        path: Option<&Path>,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        self.get_segment_provider(
            segment,
            || self.get_segment_ranges_from_block(segment, block),
            path,
        )?
        .ok_or(ProviderError::MissingStaticFileBlock(segment, block))
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and transaction.
    pub fn get_segment_provider_from_transaction(
        &self,
        segment: StaticFileSegment,
        tx: TxNumber,
        path: Option<&Path>,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        self.get_segment_provider(
            segment,
            || self.get_segment_ranges_from_transaction(segment, tx),
            path,
        )?
        .ok_or(ProviderError::MissingStaticFileTx(segment, tx))
    }

    /// Gets the [`StaticFileJarProvider`] of the requested segment and block or transaction.
    ///
    /// `fn_range` should make sure the range goes through `find_fixed_range`.
    pub fn get_segment_provider(
        &self,
        segment: StaticFileSegment,
        fn_range: impl Fn() -> Option<SegmentRangeInclusive>,
        path: Option<&Path>,
    ) -> ProviderResult<Option<StaticFileJarProvider<'_, N>>> {
        // If we have a path, then get the block range from its name.
        // Otherwise, check `self.available_static_files`
        let block_range = match path {
            Some(path) => StaticFileSegment::parse_filename(
                &path
                    .file_name()
                    .ok_or_else(|| {
                        ProviderError::MissingStaticFilePath(segment, path.to_path_buf())
                    })?
                    .to_string_lossy(),
            )
            .and_then(|(parsed_segment, block_range)| {
                if parsed_segment == segment {
                    return Some(block_range)
                }
                None
            }),
            None => fn_range(),
        };

        // Return cached `LoadedJar` or insert it for the first time, and then, return it.
        if let Some(block_range) = block_range {
            return Ok(Some(self.get_or_create_jar_provider(segment, &block_range)?))
        }

        Ok(None)
    }

    /// Given a segment and block range it removes the cached provider from the map.
    ///
    /// CAUTION: cached provider should be dropped before calling this or IT WILL deadlock.
    pub fn remove_cached_provider(
        &self,
        segment: StaticFileSegment,
        fixed_block_range_end: BlockNumber,
    ) {
        self.map.remove(&(fixed_block_range_end, segment));
    }

    /// This handles history expiry by deleting all transaction static files below the given block.
    ///
    /// For example if block is 1M and the blocks per file are 500K this will delete all individual
    /// files below 1M, so 0-499K and 500K-999K.
    ///
    /// This will not delete the file that contains the block itself, because files can only be
    /// removed entirely.
    pub fn delete_transactions_below(&self, block: BlockNumber) -> ProviderResult<()> {
        // Nothing to delete if block is 0.
        if block == 0 {
            return Ok(())
        }

        loop {
            let Some(block_height) =
                self.get_lowest_static_file_block(StaticFileSegment::Transactions)
            else {
                return Ok(())
            };

            if block_height >= block {
                return Ok(())
            }

            debug!(
                target: "provider::static_file",
                ?block_height,
                "Deleting transaction static file below block"
            );

            // now we need to wipe the static file, this will take care of updating the index and
            // advance the lowest tracked block height for the transactions segment.
            self.delete_jar(StaticFileSegment::Transactions, block_height)
                .inspect_err(|err| {
                    warn!( target: "provider::static_file", %block_height, ?err, "Failed to delete transaction static file below block")
                })
                ?;
        }
    }

    /// Given a segment and block, it deletes the jar and all files from the respective block range.
    ///
    /// CAUTION: destructive. Deletes files on disk.
    ///
    /// This will re-initialize the index after deletion, so all files are tracked.
    pub fn delete_jar(&self, segment: StaticFileSegment, block: BlockNumber) -> ProviderResult<()> {
        let fixed_block_range = self.find_fixed_range(block);
        let key = (fixed_block_range.end(), segment);
        let jar = if let Some((_, jar)) = self.map.remove(&key) {
            jar.jar
        } else {
            let file = self.path.join(segment.filename(&fixed_block_range));
            debug!(
                target: "provider::static_file",
                ?file,
                ?fixed_block_range,
                ?block,
                "Loading static file jar for deletion"
            );
            NippyJar::<SegmentHeader>::load(&file).map_err(ProviderError::other)?
        };

        jar.delete().map_err(ProviderError::other)?;

        self.initialize_index()?;

        Ok(())
    }

    /// Given a segment and block range it returns a cached
    /// [`StaticFileJarProvider`]. TODO(joshie): we should check the size and pop N if there's too
    /// many.
    fn get_or_create_jar_provider(
        &self,
        segment: StaticFileSegment,
        fixed_block_range: &SegmentRangeInclusive,
    ) -> ProviderResult<StaticFileJarProvider<'_, N>> {
        let key = (fixed_block_range.end(), segment);

        // Avoid using `entry` directly to avoid a write lock in the common case.
        trace!(target: "provider::static_file", ?segment, ?fixed_block_range, "Getting provider");
        let mut provider: StaticFileJarProvider<'_, N> = if let Some(jar) = self.map.get(&key) {
            trace!(target: "provider::static_file", ?segment, ?fixed_block_range, "Jar found in cache");
            jar.into()
        } else {
            trace!(target: "provider::static_file", ?segment, ?fixed_block_range, "Creating jar from scratch");
            let path = self.path.join(segment.filename(fixed_block_range));
            let jar = NippyJar::load(&path).map_err(ProviderError::other)?;
            self.map.entry(key).insert(LoadedJar::new(jar)?).downgrade().into()
        };

        if let Some(metrics) = &self.metrics {
            provider = provider.with_metrics(metrics.clone());
        }
        Ok(provider)
    }

    /// Gets a static file segment's block range from the provider inner block
    /// index.
    fn get_segment_ranges_from_block(
        &self,
        segment: StaticFileSegment,
        block: u64,
    ) -> Option<SegmentRangeInclusive> {
        self.static_files_max_block
            .read()
            .get(&segment)
            .filter(|max| **max >= block)
            .map(|_| self.find_fixed_range(block))
    }

    /// Gets a static file segment's fixed block range from the provider inner
    /// transaction index.
    fn get_segment_ranges_from_transaction(
        &self,
        segment: StaticFileSegment,
        tx: u64,
    ) -> Option<SegmentRangeInclusive> {
        let static_files = self.static_files_tx_index.read();
        let segment_static_files = static_files.get(&segment)?;

        // It's more probable that the request comes from a newer tx height, so we iterate
        // the static_files in reverse.
        let mut static_files_rev_iter = segment_static_files.iter().rev().peekable();

        while let Some((tx_end, block_range)) = static_files_rev_iter.next() {
            if tx > *tx_end {
                // request tx is higher than highest static file tx
                return None
            }
            let tx_start = static_files_rev_iter.peek().map(|(tx_end, _)| *tx_end + 1).unwrap_or(0);
            if tx_start <= tx {
                return Some(self.find_fixed_range(block_range.end()))
            }
        }
        None
    }

    /// Updates the inner transaction and block indexes alongside the internal cached providers in
    /// `self.map`.
    ///
    /// Any entry higher than `segment_max_block` will be deleted from the previous structures.
    ///
    /// If `segment_max_block` is None it means there's no static file for this segment.
    pub fn update_index(
        &self,
        segment: StaticFileSegment,
        segment_max_block: Option<BlockNumber>,
    ) -> ProviderResult<()> {
        let mut max_block = self.static_files_max_block.write();
        let mut tx_index = self.static_files_tx_index.write();

        match segment_max_block {
            Some(segment_max_block) => {
                // Update the max block for the segment
                max_block.insert(segment, segment_max_block);
                let fixed_range = self.find_fixed_range(segment_max_block);

                let jar = NippyJar::<SegmentHeader>::load(
                    &self.path.join(segment.filename(&fixed_range)),
                )
                .map_err(ProviderError::other)?;

                // Updates the tx index by first removing all entries which have a higher
                // block_start than our current static file.
                if let Some(tx_range) = jar.user_header().tx_range() {
                    let tx_end = tx_range.end();

                    // Current block range has the same block start as `fixed_range``, but block end
                    // might be different if we are still filling this static file.
                    if let Some(current_block_range) = jar.user_header().block_range().copied() {
                        // Considering that `update_index` is called when we either append/truncate,
                        // we are sure that we are handling the latest data
                        // points.
                        //
                        // Here we remove every entry of the index that has a block start higher or
                        // equal than our current one. This is important in the case
                        // that we prune a lot of rows resulting in a file (and thus
                        // a higher block range) deletion.
                        tx_index
                            .entry(segment)
                            .and_modify(|index| {
                                index.retain(|_, block_range| {
                                    block_range.start() < fixed_range.start()
                                });
                                index.insert(tx_end, current_block_range);
                            })
                            .or_insert_with(|| BTreeMap::from([(tx_end, current_block_range)]));
                    }
                } else if segment.is_tx_based() {
                    // The unwinded file has no more transactions/receipts. However, the highest
                    // block is within this files' block range. We only retain
                    // entries with block ranges before the current one.
                    tx_index.entry(segment).and_modify(|index| {
                        index.retain(|_, block_range| block_range.start() < fixed_range.start());
                    });

                    // If the index is empty, just remove it.
                    if tx_index.get(&segment).is_some_and(|index| index.is_empty()) {
                        tx_index.remove(&segment);
                    }
                }

                // Update the cached provider.
                self.map.insert((fixed_range.end(), segment), LoadedJar::new(jar)?);

                // Delete any cached provider that no longer has an associated jar.
                self.map.retain(|(end, seg), _| !(*seg == segment && *end > fixed_range.end()));
            }
            None => {
                tx_index.remove(&segment);
                max_block.remove(&segment);
            }
        };

        Ok(())
    }

    /// Initializes the inner transaction and block index
    pub fn initialize_index(&self) -> ProviderResult<()> {
        let mut min_block = self.static_files_min_block.write();
        let mut max_block = self.static_files_max_block.write();
        let mut tx_index = self.static_files_tx_index.write();

        min_block.clear();
        max_block.clear();
        tx_index.clear();

        for (segment, ranges) in iter_static_files(&self.path).map_err(ProviderError::other)? {
            // Update first and last block for each segment
            if let Some((first_block_range, _)) = ranges.first() {
                min_block.insert(segment, *first_block_range);
            }
            if let Some((last_block_range, _)) = ranges.last() {
                max_block.insert(segment, last_block_range.end());
            }

            // Update tx -> block_range index
            for (block_range, tx_range) in ranges {
                if let Some(tx_range) = tx_range {
                    let tx_end = tx_range.end();

                    match tx_index.entry(segment) {
                        Entry::Occupied(mut index) => {
                            index.get_mut().insert(tx_end, block_range);
                        }
                        Entry::Vacant(index) => {
                            index.insert(BTreeMap::from([(tx_end, block_range)]));
                        }
                    };
                }
            }
        }

        // If this is a re-initialization, we need to clear this as well
        self.map.clear();

        // initialize the expired history height to the lowest static file block
        if let Some(lowest_range) = min_block.get(&StaticFileSegment::Transactions) {
            // the earliest height is the lowest available block number
            self.earliest_history_height
                .store(lowest_range.start(), std::sync::atomic::Ordering::Relaxed);
        }

        Ok(())
    }

    /// Ensures that any broken invariants which cannot be healed on the spot return a pipeline
    /// target to unwind to.
    ///
    /// Two types of consistency checks are done for:
    ///
    /// 1) When a static file fails to commit but the underlying data was changed.
    /// 2) When a static file was committed, but the required database transaction was not.
    ///
    /// For 1) it can self-heal if `self.access.is_read_only()` is set to `false`. Otherwise, it
    /// will return an error.
    /// For 2) the invariants below are checked, and if broken, might require a pipeline unwind
    /// to heal.
    ///
    /// For each static file segment:
    /// * the corresponding database table should overlap or have continuity in their keys
    ///   ([`TxNumber`] or [`BlockNumber`]).
    /// * its highest block should match the stage checkpoint block number if it's equal or higher
    ///   than the corresponding database table last entry.
    ///
    /// Returns a [`Option`] of [`PipelineTarget::Unwind`] if any healing is further required.
    ///
    /// WARNING: No static file writer should be held before calling this function, otherwise it
    /// will deadlock.
    pub fn check_consistency<Provider>(
        &self,
        provider: &Provider,
        has_receipt_pruning: bool,
    ) -> ProviderResult<Option<PipelineTarget>>
    where
        Provider: DBProvider + BlockReader + StageCheckpointReader + ChainSpecProvider,
        N: NodePrimitives<Receipt: Value, BlockHeader: Value, SignedTx: Value>,
    {
        // OVM historical import is broken and does not work with this check. It's importing
        // duplicated receipts resulting in having more receipts than the expected transaction
        // range.
        //
        // If we detect an OVM import was done (block #1 <https://optimistic.etherscan.io/block/1>), skip it.
        // More on [#11099](https://github.com/paradigmxyz/reth/pull/11099).
        if provider.chain_spec().is_optimism() &&
            reth_chainspec::Chain::optimism_mainnet() == provider.chain_spec().chain_id()
        {
            // check whether we have the first OVM block: <https://optimistic.etherscan.io/block/0xbee7192e575af30420cae0c7776304ac196077ee72b048970549e4f08e875453>
            const OVM_HEADER_1_HASH: B256 =
                b256!("0xbee7192e575af30420cae0c7776304ac196077ee72b048970549e4f08e875453");
            if provider.block_number(OVM_HEADER_1_HASH)?.is_some() {
                info!(target: "reth::cli",
                    "Skipping storage verification for OP mainnet, expected inconsistency in OVM chain"
                );
                return Ok(None)
            }
        }

        info!(target: "reth::cli", "Verifying storage consistency.");

        let mut unwind_target: Option<BlockNumber> = None;
        let mut update_unwind_target = |new_target: BlockNumber| {
            if let Some(target) = unwind_target.as_mut() {
                *target = (*target).min(new_target);
            } else {
                unwind_target = Some(new_target);
            }
        };

        for segment in StaticFileSegment::iter() {
            // Not integrated yet
            if segment.is_block_meta() {
                continue
            }

            if has_receipt_pruning && segment.is_receipts() {
                // Pruned nodes (including full node) do not store receipts as static files.
                continue
            }

            let initial_highest_block = self.get_highest_static_file_block(segment);

            //  File consistency is broken if:
            //
            // * appending data was interrupted before a config commit, then data file will be
            //   truncated according to the config.
            //
            // * pruning data was interrupted before a config commit, then we have deleted data that
            //   we are expected to still have. We need to check the Database and unwind everything
            //   accordingly.
            if self.access.is_read_only() {
                self.check_segment_consistency(segment)?;
            } else {
                // Fetching the writer will attempt to heal any file level inconsistency.
                self.latest_writer(segment)?;
            }

            // Only applies to block-based static files. (Headers)
            //
            // The updated `highest_block` may have decreased if we healed from a pruning
            // interruption.
            let mut highest_block = self.get_highest_static_file_block(segment);
            if initial_highest_block != highest_block {
                info!(
                    target: "reth::providers::static_file",
                    ?initial_highest_block,
                    unwind_target = highest_block,
                    ?segment,
                    "Setting unwind target."
                );
                update_unwind_target(highest_block.unwrap_or_default());
            }

            // Only applies to transaction-based static files. (Receipts & Transactions)
            //
            // Make sure the last transaction matches the last block from its indices, since a heal
            // from a pruning interruption might have decreased the number of transactions without
            // being able to update the last block of the static file segment.
            let highest_tx = self.get_highest_static_file_tx(segment);
            if let Some(highest_tx) = highest_tx {
                let mut last_block = highest_block.unwrap_or_default();
                loop {
                    if let Some(indices) = provider.block_body_indices(last_block)? {
                        if indices.last_tx_num() <= highest_tx {
                            break
                        }
                    } else {
                        // If the block body indices can not be found, then it means that static
                        // files is ahead of database, and the `ensure_invariants` check will fix
                        // it by comparing with stage checkpoints.
                        break
                    }
                    if last_block == 0 {
                        break
                    }
                    last_block -= 1;

                    info!(
                        target: "reth::providers::static_file",
                        highest_block = self.get_highest_static_file_block(segment),
                        unwind_target = last_block,
                        ?segment,
                        "Setting unwind target."
                    );
                    highest_block = Some(last_block);
                    update_unwind_target(last_block);
                }
            }

            if let Some(unwind) = match segment {
                StaticFileSegment::Headers => self
                    .ensure_invariants::<_, tables::Headers<N::BlockHeader>>(
                        provider,
                        segment,
                        highest_block,
                        highest_block,
                    )?,
                StaticFileSegment::Transactions => self
                    .ensure_invariants::<_, tables::Transactions<N::SignedTx>>(
                        provider,
                        segment,
                        highest_tx,
                        highest_block,
                    )?,
                StaticFileSegment::Receipts => self
                    .ensure_invariants::<_, tables::Receipts<N::Receipt>>(
                        provider,
                        segment,
                        highest_tx,
                        highest_block,
                    )?,
                StaticFileSegment::BlockMeta => self
                    .ensure_invariants::<_, tables::BlockBodyIndices>(
                        provider,
                        segment,
                        highest_block,
                        highest_block,
                    )?,
            } {
                update_unwind_target(unwind);
            }
        }

        Ok(unwind_target.map(PipelineTarget::Unwind))
    }

    /// Checks consistency of the latest static file segment and throws an error if at fault.
    /// Read-only.
    pub fn check_segment_consistency(&self, segment: StaticFileSegment) -> ProviderResult<()> {
        if let Some(latest_block) = self.get_highest_static_file_block(segment) {
            let file_path =
                self.directory().join(segment.filename(&self.find_fixed_range(latest_block)));

            let jar = NippyJar::<SegmentHeader>::load(&file_path).map_err(ProviderError::other)?;

            NippyJarChecker::new(jar).check_consistency().map_err(ProviderError::other)?;
        }
        Ok(())
    }

    /// Check invariants for each corresponding table and static file segment:
    ///
    /// * the corresponding database table should overlap or have continuity in their keys
    ///   ([`TxNumber`] or [`BlockNumber`]).
    /// * its highest block should match the stage checkpoint block number if it's equal or higher
    ///   than the corresponding database table last entry.
    ///   * If the checkpoint block is higher, then request a pipeline unwind to the static file
    ///     block. This is expressed by returning [`Some`] with the requested pipeline unwind
    ///     target.
    ///   * If the checkpoint block is lower, then heal by removing rows from the static file. In
    ///     this case, the rows will be removed and [`None`] will be returned.
    ///
    /// * If the database tables overlap with static files and have contiguous keys, or the
    ///   checkpoint block matches the highest static files block, then [`None`] will be returned.
    fn ensure_invariants<Provider, T: Table<Key = u64>>(
        &self,
        provider: &Provider,
        segment: StaticFileSegment,
        highest_static_file_entry: Option<u64>,
        highest_static_file_block: Option<BlockNumber>,
    ) -> ProviderResult<Option<BlockNumber>>
    where
        Provider: DBProvider + BlockReader + StageCheckpointReader,
    {
        let mut db_cursor = provider.tx_ref().cursor_read::<T>()?;

        if let Some((db_first_entry, _)) = db_cursor.first()? {
            if let (Some(highest_entry), Some(highest_block)) =
                (highest_static_file_entry, highest_static_file_block)
            {
                // If there is a gap between the entry found in static file and
                // database, then we have most likely lost static file data and need to unwind so we
                // can load it again
                if !(db_first_entry <= highest_entry || highest_entry + 1 == db_first_entry) {
                    info!(
                        target: "reth::providers::static_file",
                        ?db_first_entry,
                        ?highest_entry,
                        unwind_target = highest_block,
                        ?segment,
                        "Setting unwind target."
                    );
                    return Ok(Some(highest_block))
                }
            }

            if let Some((db_last_entry, _)) = db_cursor.last()? {
                if highest_static_file_entry
                    .is_none_or(|highest_entry| db_last_entry > highest_entry)
                {
                    return Ok(None)
                }
            }
        }

        let highest_static_file_entry = highest_static_file_entry.unwrap_or_default();
        let highest_static_file_block = highest_static_file_block.unwrap_or_default();

        // If static file entry is ahead of the database entries, then ensure the checkpoint block
        // number matches.
        let checkpoint_block_number = provider
            .get_stage_checkpoint(match segment {
                StaticFileSegment::Headers => StageId::Headers,
                StaticFileSegment::Transactions | StaticFileSegment::BlockMeta => StageId::Bodies,
                StaticFileSegment::Receipts => StageId::Execution,
            })?
            .unwrap_or_default()
            .block_number;

        // If the checkpoint is ahead, then we lost static file data. May be data corruption.
        if checkpoint_block_number > highest_static_file_block {
            info!(
                target: "reth::providers::static_file",
                checkpoint_block_number,
                unwind_target = highest_static_file_block,
                ?segment,
                "Setting unwind target."
            );
            return Ok(Some(highest_static_file_block))
        }

        // If the checkpoint is behind, then we failed to do a database commit **but committed** to
        // static files on executing a stage, or the reverse on unwinding a stage.
        // All we need to do is to prune the extra static file rows.
        if checkpoint_block_number < highest_static_file_block {
            info!(
                target: "reth::providers",
                ?segment,
                from = highest_static_file_block,
                to = checkpoint_block_number,
                "Unwinding static file segment."
            );
            let mut writer = self.latest_writer(segment)?;
            if segment.is_headers() {
                // TODO(joshie): is_block_meta
                writer.prune_headers(highest_static_file_block - checkpoint_block_number)?;
            } else if let Some(block) = provider.block_body_indices(checkpoint_block_number)? {
                // todo joshie: is querying block_body_indices a potential issue once bbi is moved
                // to sf as well
                let number = highest_static_file_entry - block.last_tx_num();
                if segment.is_receipts() {
                    writer.prune_receipts(number, checkpoint_block_number)?;
                } else {
                    writer.prune_transactions(number, checkpoint_block_number)?;
                }
            }
            writer.commit()?;
        }

        Ok(None)
    }

    /// Returns the earliest available block number that has not been expired and is still
    /// available.
    ///
    /// This means that the highest expired block (or expired block height) is
    /// `earliest_history_height.saturating_sub(1)`.
    ///
    /// Returns `0` if no history has been expired.
    pub fn earliest_history_height(&self) -> BlockNumber {
        self.earliest_history_height.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Gets the lowest transaction static file block if it exists.
    ///
    /// For example if the transactions static file has blocks 0-499, this will return 499..
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_lowest_transaction_static_file_block(&self) -> Option<BlockNumber> {
        self.get_lowest_static_file_block(StaticFileSegment::Transactions)
    }

    /// Gets the lowest static file's block height if it exists for a static file segment.
    ///
    /// For example if the static file has blocks 0-499, this will return 499..
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_lowest_static_file_block(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        self.static_files_min_block.read().get(&segment).map(|range| range.end())
    }

    /// Gets the highest static file's block height if it exists for a static file segment.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_highest_static_file_block(&self, segment: StaticFileSegment) -> Option<BlockNumber> {
        self.static_files_max_block.read().get(&segment).copied()
    }

    /// Gets the highest static file transaction.
    ///
    /// If there is nothing on disk for the given segment, this will return [`None`].
    pub fn get_highest_static_file_tx(&self, segment: StaticFileSegment) -> Option<TxNumber> {
        self.static_files_tx_index
            .read()
            .get(&segment)
            .and_then(|index| index.last_key_value().map(|(last_tx, _)| *last_tx))
    }

    /// Gets the highest static file block for all segments.
    pub fn get_highest_static_files(&self) -> HighestStaticFiles {
        HighestStaticFiles {
            headers: self.get_highest_static_file_block(StaticFileSegment::Headers),
            receipts: self.get_highest_static_file_block(StaticFileSegment::Receipts),
            transactions: self.get_highest_static_file_block(StaticFileSegment::Transactions),
            block_meta: self.get_highest_static_file_block(StaticFileSegment::BlockMeta),
        }
    }

    /// Iterates through segment `static_files` in reverse order, executing a function until it
    /// returns some object. Useful for finding objects by [`TxHash`] or [`BlockHash`].
    pub fn find_static_file<T>(
        &self,
        segment: StaticFileSegment,
        func: impl Fn(StaticFileJarProvider<'_, N>) -> ProviderResult<Option<T>>,
    ) -> ProviderResult<Option<T>> {
        if let Some(highest_block) = self.get_highest_static_file_block(segment) {
            let mut range = self.find_fixed_range(highest_block);
            while range.end() > 0 {
                if let Some(res) = func(self.get_or_create_jar_provider(segment, &range)?)? {
                    return Ok(Some(res))
                }
                range = SegmentRangeInclusive::new(
                    range.start().saturating_sub(self.blocks_per_file),
                    range.end().saturating_sub(self.blocks_per_file),
                );
            }
        }

        Ok(None)
    }

    /// Fetches data within a specified range across multiple static files.
    ///
    /// This function iteratively retrieves data using `get_fn` for each item in the given range.
    /// It continues fetching until the end of the range is reached or the provided `predicate`
    /// returns false.
    pub fn fetch_range_with_predicate<T, F, P>(
        &self,
        segment: StaticFileSegment,
        range: Range<u64>,
        mut get_fn: F,
        mut predicate: P,
    ) -> ProviderResult<Vec<T>>
    where
        F: FnMut(&mut StaticFileCursor<'_>, u64) -> ProviderResult<Option<T>>,
        P: FnMut(&T) -> bool,
    {
        let get_provider = |start: u64| {
            if segment.is_block_based() {
                self.get_segment_provider_from_block(segment, start, None)
            } else {
                self.get_segment_provider_from_transaction(segment, start, None)
            }
        };

        let mut result = Vec::with_capacity((range.end - range.start).min(100) as usize);
        let mut provider = get_provider(range.start)?;
        let mut cursor = provider.cursor()?;

        // advances number in range
        'outer: for number in range {
            // The `retrying` flag ensures a single retry attempt per `number`. If `get_fn` fails to
            // access data in two different static files, it halts further attempts by returning
            // an error, effectively preventing infinite retry loops.
            let mut retrying = false;

            // advances static files if `get_fn` returns None
            'inner: loop {
                match get_fn(&mut cursor, number)? {
                    Some(res) => {
                        if !predicate(&res) {
                            break 'outer
                        }
                        result.push(res);
                        break 'inner
                    }
                    None => {
                        if retrying {
                            warn!(
                                target: "provider::static_file",
                                ?segment,
                                ?number,
                                "Could not find block or tx number on a range request"
                            );

                            let err = if segment.is_block_based() {
                                ProviderError::MissingStaticFileBlock(segment, number)
                            } else {
                                ProviderError::MissingStaticFileTx(segment, number)
                            };
                            return Err(err)
                        }
                        // There is a very small chance of hitting a deadlock if two consecutive
                        // static files share the same bucket in the
                        // internal dashmap and we don't drop the current provider
                        // before requesting the next one.
                        drop(cursor);
                        drop(provider);
                        provider = get_provider(number)?;
                        cursor = provider.cursor()?;
                        retrying = true;
                    }
                }
            }
        }

        Ok(result)
    }

    /// Fetches data within a specified range across multiple static files.
    ///
    /// Returns an iterator over the data
    pub fn fetch_range_iter<'a, T, F>(
        &'a self,
        segment: StaticFileSegment,
        range: Range<u64>,
        get_fn: F,
    ) -> ProviderResult<impl Iterator<Item = ProviderResult<T>> + 'a>
    where
        F: Fn(&mut StaticFileCursor<'_>, u64) -> ProviderResult<Option<T>> + 'a,
        T: std::fmt::Debug,
    {
        let get_provider = move |start: u64| {
            if segment.is_block_based() {
                self.get_segment_provider_from_block(segment, start, None)
            } else {
                self.get_segment_provider_from_transaction(segment, start, None)
            }
        };

        let mut provider = Some(get_provider(range.start)?);
        Ok(range.filter_map(move |number| {
            match get_fn(&mut provider.as_ref().expect("qed").cursor().ok()?, number).transpose() {
                Some(result) => Some(result),
                None => {
                    // There is a very small chance of hitting a deadlock if two consecutive static
                    // files share the same bucket in the internal dashmap and
                    // we don't drop the current provider before requesting the
                    // next one.
                    provider.take();
                    provider = Some(get_provider(number).ok()?);
                    get_fn(&mut provider.as_ref().expect("qed").cursor().ok()?, number).transpose()
                }
            }
        }))
    }

    /// Returns directory where `static_files` are located.
    pub fn directory(&self) -> &Path {
        &self.path
    }

    /// Retrieves data from the database or static file, wherever it's available.
    ///
    /// # Arguments
    /// * `segment` - The segment of the static file to check against.
    /// * `index_key` - Requested index key, usually a block or transaction number.
    /// * `fetch_from_static_file` - A closure that defines how to fetch the data from the static
    ///   file provider.
    /// * `fetch_from_database` - A closure that defines how to fetch the data from the database
    ///   when the static file doesn't contain the required data or is not available.
    pub fn get_with_static_file_or_database<T, FS, FD>(
        &self,
        segment: StaticFileSegment,
        number: u64,
        fetch_from_static_file: FS,
        fetch_from_database: FD,
    ) -> ProviderResult<Option<T>>
    where
        FS: Fn(&Self) -> ProviderResult<Option<T>>,
        FD: Fn() -> ProviderResult<Option<T>>,
    {
        // If there is, check the maximum block or transaction number of the segment.
        let static_file_upper_bound = if segment.is_block_based() {
            self.get_highest_static_file_block(segment)
        } else {
            self.get_highest_static_file_tx(segment)
        };

        if static_file_upper_bound
            .is_some_and(|static_file_upper_bound| static_file_upper_bound >= number)
        {
            return fetch_from_static_file(self)
        }
        fetch_from_database()
    }

    /// Gets data within a specified range, potentially spanning different `static_files` and
    /// database.
    ///
    /// # Arguments
    /// * `segment` - The segment of the static file to query.
    /// * `block_range` - The range of data to fetch.
    /// * `fetch_from_static_file` - A function to fetch data from the `static_file`.
    /// * `fetch_from_database` - A function to fetch data from the database.
    /// * `predicate` - A function used to evaluate each item in the fetched data. Fetching is
    ///   terminated when this function returns false, thereby filtering the data based on the
    ///   provided condition.
    pub fn get_range_with_static_file_or_database<T, P, FS, FD>(
        &self,
        segment: StaticFileSegment,
        mut block_or_tx_range: Range<u64>,
        fetch_from_static_file: FS,
        mut fetch_from_database: FD,
        mut predicate: P,
    ) -> ProviderResult<Vec<T>>
    where
        FS: Fn(&Self, Range<u64>, &mut P) -> ProviderResult<Vec<T>>,
        FD: FnMut(Range<u64>, P) -> ProviderResult<Vec<T>>,
        P: FnMut(&T) -> bool,
    {
        let mut data = Vec::new();

        // If there is, check the maximum block or transaction number of the segment.
        if let Some(static_file_upper_bound) = if segment.is_block_based() {
            self.get_highest_static_file_block(segment)
        } else {
            self.get_highest_static_file_tx(segment)
        } {
            if block_or_tx_range.start <= static_file_upper_bound {
                let end = block_or_tx_range.end.min(static_file_upper_bound + 1);
                data.extend(fetch_from_static_file(
                    self,
                    block_or_tx_range.start..end,
                    &mut predicate,
                )?);
                block_or_tx_range.start = end;
            }
        }

        if block_or_tx_range.end > block_or_tx_range.start {
            data.extend(fetch_from_database(block_or_tx_range, predicate)?)
        }

        Ok(data)
    }

    /// Returns `static_files` directory
    #[cfg(any(test, feature = "test-utils"))]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns `static_files` transaction index
    #[cfg(any(test, feature = "test-utils"))]
    pub fn tx_index(&self) -> &RwLock<SegmentRanges> {
        &self.static_files_tx_index
    }
}

/// Helper trait to manage different [`StaticFileProviderRW`] of an `Arc<StaticFileProvider`
pub trait StaticFileWriter {
    /// The primitives type used by the static file provider.
    type Primitives: Send + Sync + 'static;

    /// Returns a mutable reference to a [`StaticFileProviderRW`] of a [`StaticFileSegment`].
    fn get_writer(
        &self,
        block: BlockNumber,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>>;

    /// Returns a mutable reference to a [`StaticFileProviderRW`] of the latest
    /// [`StaticFileSegment`].
    fn latest_writer(
        &self,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>>;

    /// Commits all changes of all [`StaticFileProviderRW`] of all [`StaticFileSegment`].
    fn commit(&self) -> ProviderResult<()>;
}

impl<N: NodePrimitives> StaticFileWriter for StaticFileProvider<N> {
    type Primitives = N;

    fn get_writer(
        &self,
        block: BlockNumber,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>> {
        if self.access.is_read_only() {
            return Err(ProviderError::ReadOnlyStaticFileAccess)
        }

        trace!(target: "provider::static_file", ?block, ?segment, "Getting static file writer.");
        self.writers.get_or_create(segment, || {
            StaticFileProviderRW::new(segment, block, Arc::downgrade(&self.0), self.metrics.clone())
        })
    }

    fn latest_writer(
        &self,
        segment: StaticFileSegment,
    ) -> ProviderResult<StaticFileProviderRWRefMut<'_, Self::Primitives>> {
        self.get_writer(self.get_highest_static_file_block(segment).unwrap_or_default(), segment)
    }

    fn commit(&self) -> ProviderResult<()> {
        self.writers.commit()
    }
}

impl<N: NodePrimitives<BlockHeader: Value>> HeaderProvider for StaticFileProvider<N> {
    type Header = N::BlockHeader;

    fn header(&self, block_hash: &BlockHash) -> ProviderResult<Option<Self::Header>> {
        self.find_static_file(StaticFileSegment::Headers, |jar_provider| {
            Ok(jar_provider
                .cursor()?
                .get_two::<HeaderWithHashMask<Self::Header>>(block_hash.into())?
                .and_then(|(header, hash)| {
                    if &hash == block_hash {
                        return Some(header)
                    }
                    None
                }))
        })
    }

    fn header_by_number(&self, num: BlockNumber) -> ProviderResult<Option<Self::Header>> {
        self.get_segment_provider_from_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.header_by_number(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn header_td(&self, block_hash: &BlockHash) -> ProviderResult<Option<U256>> {
        self.find_static_file(StaticFileSegment::Headers, |jar_provider| {
            Ok(jar_provider
                .cursor()?
                .get_two::<TDWithHashMask>(block_hash.into())?
                .and_then(|(td, hash)| (&hash == block_hash).then_some(td.0)))
        })
    }

    fn header_td_by_number(&self, num: BlockNumber) -> ProviderResult<Option<U256>> {
        self.get_segment_provider_from_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.header_td_by_number(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Self::Header>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            to_range(range),
            |cursor, number| cursor.get_one::<HeaderMask<Self::Header>>(number.into()),
            |_| true,
        )
    }

    fn sealed_header(
        &self,
        num: BlockNumber,
    ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        self.get_segment_provider_from_block(StaticFileSegment::Headers, num, None)
            .and_then(|provider| provider.sealed_header(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<BlockNumber>,
        predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            to_range(range),
            |cursor, number| {
                Ok(cursor
                    .get_two::<HeaderWithHashMask<Self::Header>>(number.into())?
                    .map(|(header, hash)| SealedHeader::new(header, hash)))
            },
            predicate,
        )
    }
}

impl<N: NodePrimitives> BlockHashReader for StaticFileProvider<N> {
    fn block_hash(&self, num: u64) -> ProviderResult<Option<B256>> {
        self.get_segment_provider_from_block(StaticFileSegment::Headers, num, None)?.block_hash(num)
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Headers,
            start..end,
            |cursor, number| cursor.get_one::<BlockHashMask>(number.into()),
            |_| true,
        )
    }
}

impl<N: NodePrimitives<SignedTx: Value + SignedTransaction, Receipt: Value>> ReceiptProvider
    for StaticFileProvider<N>
{
    type Receipt = N::Receipt;

    fn receipt(&self, num: TxNumber) -> ProviderResult<Option<Self::Receipt>> {
        self.get_segment_provider_from_transaction(StaticFileSegment::Receipts, num, None)
            .and_then(|provider| provider.receipt(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn receipt_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Receipt>> {
        if let Some(num) = self.transaction_id(hash)? {
            return self.receipt(num)
        }
        Ok(None)
    }

    fn receipts_by_block(
        &self,
        _block: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Receipt>>> {
        unreachable!()
    }

    fn receipts_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Receipt>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Receipts,
            to_range(range),
            |cursor, number| cursor.get_one::<ReceiptMask<Self::Receipt>>(number.into()),
            |_| true,
        )
    }

    fn receipts_by_block_range(
        &self,
        _block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Receipt>>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: FullNodePrimitives<SignedTx: Value, Receipt: Value, BlockHeader: Value>>
    TransactionsProviderExt for StaticFileProvider<N>
{
    fn transaction_hashes_by_range(
        &self,
        tx_range: Range<TxNumber>,
    ) -> ProviderResult<Vec<(TxHash, TxNumber)>> {
        let tx_range_size = (tx_range.end - tx_range.start) as usize;

        // Transactions are different size, so chunks will not all take the same processing time. If
        // chunks are too big, there will be idle threads waiting for work. Choosing an
        // arbitrary smaller value to make sure it doesn't happen.
        let chunk_size = 100;

        // iterator over the chunks
        let chunks = tx_range
            .clone()
            .step_by(chunk_size)
            .map(|start| start..std::cmp::min(start + chunk_size as u64, tx_range.end));
        let mut channels = Vec::with_capacity(tx_range_size.div_ceil(chunk_size));

        for chunk_range in chunks {
            let (channel_tx, channel_rx) = mpsc::channel();
            channels.push(channel_rx);

            let manager = self.clone();

            // Spawn the task onto the global rayon pool
            // This task will send the results through the channel after it has calculated
            // the hash.
            rayon::spawn(move || {
                let mut rlp_buf = Vec::with_capacity(128);
                let _ = manager.fetch_range_with_predicate(
                    StaticFileSegment::Transactions,
                    chunk_range,
                    |cursor, number| {
                        Ok(cursor
                            .get_one::<TransactionMask<Self::Transaction>>(number.into())?
                            .map(|transaction| {
                                rlp_buf.clear();
                                let _ = channel_tx
                                    .send(calculate_hash((number, transaction), &mut rlp_buf));
                            }))
                    },
                    |_| true,
                );
            });
        }

        let mut tx_list = Vec::with_capacity(tx_range_size);

        // Iterate over channels and append the tx hashes unsorted
        for channel in channels {
            while let Ok(tx) = channel.recv() {
                let (tx_hash, tx_id) = tx.map_err(|boxed| *boxed)?;
                tx_list.push((tx_hash, tx_id));
            }
        }

        Ok(tx_list)
    }
}

impl<N: NodePrimitives<SignedTx: Decompress + SignedTransaction>> TransactionsProvider
    for StaticFileProvider<N>
{
    type Transaction = N::SignedTx;

    fn transaction_id(&self, tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
        self.find_static_file(StaticFileSegment::Transactions, |jar_provider| {
            let mut cursor = jar_provider.cursor()?;
            if cursor
                .get_one::<TransactionMask<Self::Transaction>>((&tx_hash).into())?
                .and_then(|tx| (tx.trie_hash() == tx_hash).then_some(tx))
                .is_some()
            {
                Ok(cursor.number())
            } else {
                Ok(None)
            }
        })
    }

    fn transaction_by_id(&self, num: TxNumber) -> ProviderResult<Option<Self::Transaction>> {
        self.get_segment_provider_from_transaction(StaticFileSegment::Transactions, num, None)
            .and_then(|provider| provider.transaction_by_id(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn transaction_by_id_unhashed(
        &self,
        num: TxNumber,
    ) -> ProviderResult<Option<Self::Transaction>> {
        self.get_segment_provider_from_transaction(StaticFileSegment::Transactions, num, None)
            .and_then(|provider| provider.transaction_by_id_unhashed(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileTx(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn transaction_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Transaction>> {
        self.find_static_file(StaticFileSegment::Transactions, |jar_provider| {
            Ok(jar_provider
                .cursor()?
                .get_one::<TransactionMask<Self::Transaction>>((&hash).into())?
                .and_then(|tx| (tx.trie_hash() == hash).then_some(tx)))
        })
    }

    fn transaction_by_hash_with_meta(
        &self,
        _hash: TxHash,
    ) -> ProviderResult<Option<(Self::Transaction, TransactionMeta)>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transaction_block(&self, _id: TxNumber) -> ProviderResult<Option<BlockNumber>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_block(
        &self,
        _block_id: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Transaction>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_block_range(
        &self,
        _range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Transaction>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn transactions_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Transaction>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::Transactions,
            to_range(range),
            |cursor, number| cursor.get_one::<TransactionMask<Self::Transaction>>(number.into()),
            |_| true,
        )
    }

    fn senders_by_tx_range(
        &self,
        range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Address>> {
        let txes = self.transactions_by_tx_range(range)?;
        Ok(reth_primitives_traits::transaction::recover::recover_signers(&txes)?)
    }

    fn transaction_sender(&self, id: TxNumber) -> ProviderResult<Option<Address>> {
        match self.transaction_by_id_unhashed(id)? {
            Some(tx) => Ok(tx.recover_signer().ok()),
            None => Ok(None),
        }
    }
}

/* Cannot be successfully implemented but must exist for trait requirements */

impl<N: NodePrimitives> BlockNumReader for StaticFileProvider<N> {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn best_block_number(&self) -> ProviderResult<BlockNumber> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn last_block_number(&self) -> ProviderResult<BlockNumber> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_number(&self, _hash: B256) -> ProviderResult<Option<BlockNumber>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: FullNodePrimitives<SignedTx: Value, Receipt: Value, BlockHeader: Value>> BlockReader
    for StaticFileProvider<N>
{
    type Block = N::Block;

    fn find_block_by_hash(
        &self,
        _hash: B256,
        _source: BlockSource,
    ) -> ProviderResult<Option<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block(&self, _id: BlockHashOrNumber) -> ProviderResult<Option<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn pending_block(&self) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn pending_block_and_receipts(
        &self,
    ) -> ProviderResult<Option<(RecoveredBlock<Self::Block>, Vec<Self::Receipt>)>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn recovered_block(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn sealed_block_with_senders(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_range(&self, _range: RangeInclusive<BlockNumber>) -> ProviderResult<Vec<Self::Block>> {
        // Required data not present in static_files
        Err(ProviderError::UnsupportedProvider)
    }

    fn block_with_senders_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Err(ProviderError::UnsupportedProvider)
    }

    fn recovered_block_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Err(ProviderError::UnsupportedProvider)
    }
}

impl<N: NodePrimitives> BlockBodyIndicesProvider for StaticFileProvider<N> {
    fn block_body_indices(&self, num: u64) -> ProviderResult<Option<StoredBlockBodyIndices>> {
        self.get_segment_provider_from_block(StaticFileSegment::BlockMeta, num, None)
            .and_then(|provider| provider.block_body_indices(num))
            .or_else(|err| {
                if let ProviderError::MissingStaticFileBlock(_, _) = err {
                    Ok(None)
                } else {
                    Err(err)
                }
            })
    }

    fn block_body_indices_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<StoredBlockBodyIndices>> {
        self.fetch_range_with_predicate(
            StaticFileSegment::BlockMeta,
            *range.start()..*range.end() + 1,
            |cursor, number| cursor.get_one::<BodyIndicesMask>(number.into()),
            |_| true,
        )
    }
}

impl<N: NodePrimitives> StatsReader for StaticFileProvider<N> {
    fn count_entries<T: Table>(&self) -> ProviderResult<usize> {
        match T::NAME {
            tables::CanonicalHeaders::NAME |
            tables::Headers::<Header>::NAME |
            tables::HeaderTerminalDifficulties::NAME => Ok(self
                .get_highest_static_file_block(StaticFileSegment::Headers)
                .map(|block| block + 1)
                .unwrap_or_default()
                as usize),
            tables::Receipts::<Receipt>::NAME => Ok(self
                .get_highest_static_file_tx(StaticFileSegment::Receipts)
                .map(|receipts| receipts + 1)
                .unwrap_or_default() as usize),
            tables::Transactions::<TransactionSigned>::NAME => Ok(self
                .get_highest_static_file_tx(StaticFileSegment::Transactions)
                .map(|txs| txs + 1)
                .unwrap_or_default()
                as usize),
            _ => Err(ProviderError::UnsupportedProvider),
        }
    }
}

/// Calculates the tx hash for the given transaction and its id.
#[inline]
fn calculate_hash<T>(
    entry: (TxNumber, T),
    rlp_buf: &mut Vec<u8>,
) -> Result<(B256, TxNumber), Box<ProviderError>>
where
    T: Encodable2718,
{
    let (tx_id, tx) = entry;
    tx.encode_2718(rlp_buf);
    Ok((keccak256(rlp_buf), tx_id))
}
