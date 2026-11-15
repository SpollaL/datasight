//! Whole-object download for the cloud backends, as parallel ranged GETs.
//!
//! One GET streams the whole object over a single connection, so its speed is
//! whatever one stream gets — and object_store gives every request 30 s in
//! total, body included, so an object that takes longer than that to stream
//! fails outright. This fetches the object as `CHUNK`-sized byte ranges, at
//! most `STREAMS` at a time (object_store is HTTP/1.1-only by default, so each
//! in-flight range is its own connection). The ranges in flight share the
//! link, so the bytes in flight (`STREAMS * CHUNK`), not the object's size,
//! set how long each request takes — keep that product small. Every range is
//! pinned to the ETag read up front, so an object overwritten mid-download
//! fails with a precondition error instead of stitching together two versions.
//! Each finished range is added to the caller's [`Progress`], and a cancel is
//! honoured between ranges.

use crate::browser::{BrowserError, Progress};
use object_store::{path::Path, GetOptions, ObjectStore};
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::task::JoinSet;

/// Range requests in flight at once. Over a slow link to Azure, 8 x 2 MiB
/// and 16 x 1 MiB both ran at 5-6 MB/s, so more streams bought nothing.
const STREAMS: usize = 8;
/// Bytes per range request. 16 MiB in flight finishes each range inside the
/// 30 s timeout while the link manages ~0.6 MB/s in total — the same floor a
/// single GET of a 16 MiB object needs, so this is never more fragile than it.
#[cfg(not(test))]
const CHUNK: usize = 2 * 1024 * 1024;
/// Tiny under test so a round trip spans more than `STREAMS` chunks without
/// allocating hundreds of MB.
#[cfg(test)]
const CHUNK: usize = 1024;

/// Splits `0..size` into contiguous `CHUNK`-sized ranges (the last may be shorter).
fn chunk_ranges(size: usize) -> impl Iterator<Item = Range<usize>> {
    (0..size)
        .step_by(CHUNK)
        .map(move |start| start..(start + CHUNK).min(size))
}

pub(crate) async fn get_all(
    store: Arc<dyn ObjectStore>,
    path: &Path,
    progress: &Progress,
) -> Result<Vec<u8>, BrowserError> {
    let net = |e: object_store::Error| BrowserError::Network(e.to_string());
    let meta = store.head(path).await.map_err(net)?;
    progress.total.store(meta.size, Ordering::Relaxed);
    progress.done.store(0, Ordering::Relaxed);
    let mut buf = vec![0u8; meta.size];
    let spawn = |tasks: &mut JoinSet<_>, range: Range<usize>| {
        let (store, path) = (Arc::clone(&store), path.clone());
        let options = GetOptions {
            range: Some(range.clone().into()),
            if_match: meta.e_tag.clone(),
            ..Default::default()
        };
        tasks.spawn(async move {
            let bytes = store.get_opts(&path, options).await?.bytes().await?;
            Ok::<_, object_store::Error>((range, bytes))
        });
    };
    let mut pending = chunk_ranges(meta.size);
    let mut tasks = JoinSet::new();
    for range in pending.by_ref().take(STREAMS) {
        spawn(&mut tasks, range);
    }
    // Dropping `tasks` on an early return aborts the ranges still in flight.
    while let Some(joined) = tasks.join_next().await {
        let (range, bytes) = joined
            .map_err(|e| BrowserError::Network(e.to_string()))?
            .map_err(net)?;
        if bytes.len() != range.len() {
            return Err(BrowserError::Network(format!(
                "range {:?} of {} returned {} bytes",
                range,
                path,
                bytes.len()
            )));
        }
        buf[range].copy_from_slice(&bytes);
        progress.done.fetch_add(bytes.len(), Ordering::Relaxed);
        if progress.cancel.load(Ordering::Relaxed) {
            return Err(BrowserError::Other("cancelled".into()));
        }
        if let Some(next) = pending.next() {
            spawn(&mut tasks, next);
        }
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[test]
    fn test_chunk_ranges_tile_the_object_in_bounded_chunks() {
        for size in [0, 1, CHUNK - 1, CHUNK, CHUNK + 1, 1 << 34] {
            let mut next = 0;
            for r in chunk_ranges(size) {
                assert_eq!(r.start, next, "gap or overlap at size {size}");
                assert!(!r.is_empty() && r.len() <= CHUNK, "size {size}: {r:?}");
                next = r.end;
            }
            assert_eq!(next, size);
        }
        assert_eq!(chunk_ranges(CHUNK).count(), 1);
        assert_eq!(chunk_ranges(CHUNK + 1).count(), 2);
    }

    #[test]
    fn test_get_all_round_trips_a_multi_range_object_byte_for_byte() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("data/big.parquet");
        // Prime-length pattern so a misplaced range cannot line up by accident,
        // and more chunks than STREAMS so in-flight slots get refilled.
        let data: Vec<u8> = (0..(STREAMS + 2) * CHUNK + 3)
            .map(|i| (i % 251) as u8)
            .collect();
        rt.block_on(store.put(&path, data.clone().into())).unwrap();
        assert!(chunk_ranges(data.len()).count() > STREAMS);

        let progress = Progress::default();
        let got = rt.block_on(get_all(store, &path, &progress)).unwrap();
        assert!(got == data, "downloaded bytes differ from the object");
        assert_eq!(progress.total.load(Ordering::Relaxed), data.len());
        assert_eq!(progress.done.load(Ordering::Relaxed), data.len());
    }

    #[test]
    fn test_get_all_stops_once_cancelled() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let path = Path::from("big.csv");
        rt.block_on(store.put(&path, vec![7u8; 4 * STREAMS * CHUNK].into()))
            .unwrap();
        let progress = Progress::default();
        progress.cancel.store(true, Ordering::Relaxed);
        assert!(rt.block_on(get_all(store, &path, &progress)).is_err());
        // Stops after the first range lands rather than draining the rest.
        assert!(progress.done.load(Ordering::Relaxed) < 2 * STREAMS * CHUNK);
    }

    #[test]
    fn test_get_all_of_empty_and_missing_objects() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let empty = Path::from("empty.csv");
        rt.block_on(store.put(&empty, Vec::<u8>::new().into()))
            .unwrap();
        assert!(rt
            .block_on(get_all(Arc::clone(&store), &empty, &Progress::default()))
            .unwrap()
            .is_empty());
        assert!(rt
            .block_on(get_all(
                store,
                &Path::from("nope.csv"),
                &Progress::default()
            ))
            .is_err());
    }
}
