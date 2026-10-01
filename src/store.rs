use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use sha2::{Digest, Sha256};
use tokio::{
    sync::{broadcast, Notify},
    time::{self, Duration, Instant},
};

use crate::{
    geometry::{decode, Coordinates},
    stream::{Fields, Stream, StreamId},
};
use std::str::FromStr;

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OrdF64(pub f64);

impl PartialOrd for OrdF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrdF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

impl Eq for OrdF64 {}

impl Hash for OrdF64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}

#[derive(Debug)]
pub struct Store {
    pub db: Db,
}

#[derive(Debug, Default)]
pub struct ListEntry {
    values: Vec<Bytes>,
    key_count: u64,
}

#[derive(Debug, Clone)]
pub struct Db {
    shared: Arc<Shared>,
}
#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
    background_task: Notify,
}

type ZSet = BTreeMap<(OrdF64, Bytes), ()>;

#[derive(Debug)]
struct State {
    entries: HashMap<String, Entry>,
    expirations: BTreeSet<(Instant, String)>,
    stream: HashMap<String, Stream>,
    list: HashMap<String, ListEntry>,
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,
    z_set: HashMap<String, ZSet>,
    users: HashMap<String, String>,
    shutdown: bool,
}

#[derive(Debug)]
struct Entry {
    data: Bytes,
    expires_at: Option<Instant>,
}
impl Shared {
    fn pure_expire_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();
        if state.shutdown {
            return None;
        }
        let state = &mut *state;
        let now = Instant::now();
        while let Some(&(when, ref key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }

            state.entries.remove(key);
            state.expirations.remove(&(when, key.clone()));
        }

        None
    }
    pub fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}
impl Db {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                stream: HashMap::new(),
                expirations: BTreeSet::new(),
                list: HashMap::new(),
                pub_sub: HashMap::new(),
                z_set: HashMap::new(),
                users: HashMap::new(),
                shutdown: false,
            }),

            background_task: Notify::new(),
        });
        tokio::spawn(pure_expire_task(shared.clone()));

        Db { shared }
    }
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    pub fn get_keys(&self, key: &str) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let mut result_keys: Vec<Bytes> = Vec::new();

        if key == "*" {
            result_keys = state.entries.keys().cloned().map(|x| x.into()).collect();
            result_keys.extend(state.stream.keys().cloned().map(|x| x.into()));
        } else if key.ends_with('*') {
            let prefix = &key[0..key.len() - 1];
            for k in state.entries.keys() {
                if k.starts_with(prefix) {
                    result_keys.push(k.clone().into());
                }
            }
        } else if let Some(suffix) = key.strip_suffix('*') {
            for k in state.entries.keys() {
                if k.ends_with(suffix) {
                    result_keys.push(k.clone().into());
                }
            }
        }

        result_keys
    }

    pub fn is_stream(&self, key: &str) -> bool {
        let state = self.shared.state.lock().unwrap();
        state.stream.contains_key(key)
    }

    pub fn get_last_stream_id(&self, key: &str) -> Option<StreamId> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state.stream.entry(key.to_string()).or_default();
        stream.last_id()
    }

    pub fn xadd(&self, key: String, id_str: String, fields: Fields) -> Result<String, String> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state.stream.entry(key).or_default();

        let id = if id_str == "*" {
            let mut ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            if let Some(last_id) = stream.last_id() {
                if ms < last_id.ms {
                    ms = last_id.ms;
                }
            }
            stream.generate_id(ms)
        } else if id_str.ends_with("-*") {
            let mut parts = id_str.splitn(2, '-');
            let ms_str = parts.next().unwrap();
            let ms = ms_str
                .parse::<u64>()
                .map_err(|_| "ERR Invalid stream ID specified as stream key".to_string())?;

            stream.generate_id(ms)
        } else {
            StreamId::from_str(&id_str).map_err(|e| e.to_string())?
        };

        if id.is_invalid() {
            return Err("ERR The ID specified in XADD must be greater than 0-0".to_string());
        }
        if let Some(last_id) = stream.last_id() {
            if id <= last_id {
                return Err("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
            }
        }
        let stream_id = stream.xadd(id, fields);
        Ok(stream_id.to_string())
    }

    pub fn xrange(
        &self,
        key: String,
        start: StreamId,
        end: StreamId,
    ) -> Option<BTreeMap<StreamId, Fields>> {
        let state = self.shared.state.lock().unwrap();
        let stream = state.stream.get(&key)?;
        Some(stream.xrange(start, end))
    }
    pub fn xread(&self, key: String, id: StreamId) -> Option<BTreeMap<StreamId, Fields>> {
        let state = self.shared.state.lock().unwrap();
        let stream = state.stream.get(&key)?;
        Some(stream.xread(id))
    }

    pub fn rpush(&self, key: String, valuse: Vec<Bytes>) -> u64 {
        let mut state = self.shared.state.lock().unwrap();
        let list = state.list.entry(key).or_default();
        let len = valuse.len() as u64;
        list.values.extend(valuse);
        list.key_count += len;
        list.key_count
    }
    pub fn lpush(&self, key: String, valuse: Vec<Bytes>) -> u64 {
        let mut state = self.shared.state.lock().unwrap();
        let list = state.list.entry(key).or_default();
        let len = valuse.len() as u64;
        list.values.splice(..0, valuse.iter().cloned());
        list.key_count += len;
        list.key_count
    }

    pub fn lrange(&self, key: String, start: i64, end: i64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let mut v = Vec::new();

        if let Some(list) = state.list.get(&key) {
            let list_len = list.values.len() as i64;

            if list_len == 0 {
                return v;
            }

            let norm_start = if start < 0 {
                (list_len + start).max(0)
            } else {
                start.min(list_len - 1)
            };

            let norm_end = if end < 0 {
                (list_len + end).max(-1)
            } else {
                end.min(list_len - 1)
            };

            if norm_start > norm_end || norm_start >= list_len || norm_end < 0 {
                return v;
            }

            let start_idx = norm_start as usize;
            let end_idx = norm_end as usize;
            v.extend_from_slice(&list.values[start_idx..=end_idx]);
        }

        v
    }

    pub fn llen(&self, key: String) -> usize {
        let mut len = 0;
        let state = self.shared.state.lock().unwrap();
        if let Some(list) = state.list.get(&key) {
            len = list.values.len();
        }

        len
    }

    pub fn lpop(&self, key: String) -> Option<Bytes> {
        let mut state = self.shared.state.lock().unwrap();
        if let Some(list) = state.list.get_mut(&key) {
            if !list.values.is_empty() {
                return Some(list.values.remove(0));
            }
        }
        None
    }

    pub fn lpop_rang(&self, key: String, start: i64, end: i64) -> Vec<Bytes> {
        let mut state = self.shared.state.lock().unwrap();
        let mut removed = Vec::new();

        if let Some(list) = state.list.get_mut(&key) {
            let len = list.values.len() as i64;
            if len == 0 {
                return removed;
            }

            let norm_start = if start < 0 { len + start } else { start }
                .max(0)
                .min(len - 1);
            let norm_end = if end < 0 { len + end } else { end }.max(-1).min(len - 1);

            if norm_start > norm_end || norm_start >= len || norm_end < 0 {
                return removed;
            }

            let start_idx = norm_start as usize;
            let end_idx = norm_end as usize;

            removed.extend(list.values.drain(start_idx..=end_idx));
        }

        removed
    }

    pub fn bl_pop(&self, key: String) -> Vec<Bytes> {
        let mut state = self.shared.state.lock().unwrap();

        if let Some(list) = state.list.get_mut(&key) {
            if let Some(first) = list.values.first().cloned() {
                list.values.remove(0);
                return vec![Bytes::from(key), first];
            }
        }

        Vec::new()
    }

    pub fn zadd(&self, key: String, member: Bytes, score: f64) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let z_set = state.z_set.entry(key).or_default();

        let new_key = (OrdF64(score), member.clone());
        let already_exists = z_set.iter().any(|((_, m), _)| m == &member);

        if already_exists {
            let old_key = z_set
                .iter()
                .find(|((_, m), _)| m == &member)
                .map(|(k, _)| k.clone());
            if let Some(old) = old_key {
                z_set.remove(&old);
            }
        }

        z_set.insert(new_key, ());
        if already_exists {
            0
        } else {
            1
        }
    }

    pub fn zrank(&self, key: String, member: Bytes) -> Option<usize> {
        let state = self.shared.state.lock().unwrap();
        let z_set = state.z_set.get(&key)?;

        for (rank, ((_, m), _)) in z_set.iter().enumerate() {
            if m == &member {
                return Some(rank);
            }
        }
        None
    }

    pub fn zrange(&self, key: String, start: i64, end: i64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let Some(zset) = state.z_set.get(&key) else {
            return vec![];
        };

        let len = zset.len() as i64;
        if len == 0 {
            return vec![];
        }

        let start_idx = if start < 0 { len + start } else { start }.max(0) as usize;
        let end_idx = if end < 0 { len + end } else { end }.min(len - 1) as usize;

        if start_idx > end_idx {
            return vec![];
        }

        zset.iter()
            .skip(start_idx)
            .take(end_idx - start_idx + 1)
            .map(|((_, member), _)| member.clone())
            .collect()
    }
    pub fn zcard(&self, key: String) -> usize {
        let state = self.shared.state.lock().unwrap();

        if let Some(val) = state.z_set.get(&key) {
            val.len()
        } else {
            0
        }
    }

    pub fn zscore(&self, key: String, member: Bytes) -> Option<f64> {
        let state = self.shared.state.lock().unwrap();
        let z_set = state.z_set.get(&key)?;

        for (f, m) in z_set.keys() {
            if m == &member {
                return Some(f.0);
            }
        }

        None
    }

    pub fn zrem(&self, key: String, member: Bytes) -> u64 {
        let mut state = self.shared.state.lock().unwrap();

        if let Some(zset) = state.z_set.get_mut(&key) {
            let mut removed = 0u64;

            let keys_to_remove: Vec<(OrdF64, Bytes)> = zset
                .iter()
                .filter(|(&(_score, ref m), &())| m == &member)
                .map(|(k, &())| k.clone())
                .collect();

            for k in keys_to_remove {
                zset.remove(&k);
                removed += 1;
            }

            if zset.is_empty() {
                state.z_set.remove(&key);
            }

            removed
        } else {
            0
        }
    }

    pub fn gpos(&self, key: String, members: Vec<Bytes>) -> Vec<Option<f64>> {
        let state = self.shared.state.lock().unwrap();
        let z_set = state.z_set.get(&key);

        if let Some(zset) = z_set {
            members
                .into_iter()
                .map(|member| {
                    zset.iter().find_map(
                        |((OrdF64(score), m), &())| {
                            if m == &member {
                                Some(*score)
                            } else {
                                None
                            }
                        },
                    )
                })
                .collect()
        } else {
            vec![None; members.len()]
        }
    }

    pub fn gsearch(&self, key: String, center: Coordinates, radius_meters: f64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();

        let Some(zset) = state.z_set.get(&key) else {
            return vec![];
        };

        let mut results = Vec::with_capacity(zset.len().min(64));

        for (OrdF64(score), member) in zset.keys() {
            let geohash_u64 = *score as u64;
            let coord = decode(geohash_u64);

            if coord.haversine_distance(&center) <= radius_meters + 1e-6 {
                results.push(member.clone());
            }
        }

        results
    }

    pub fn insert_user(&self, name: String, password_hash: String) -> bool {
        let mut state = self.shared.state.lock().unwrap();

        if state.users.contains_key(&name) {
            return false;
        }

        state.users.insert(name, password_hash);
        true
    }

    pub fn get_user_password_hash(&self, name: &str) -> Option<String> {
        let state = self.shared.state.lock().unwrap();
        state.users.get(name).cloned()
    }

    pub fn verify_user_passowrd(&self, username: &str, password: String) -> bool {
        let state = self.shared.state.lock().unwrap();
        if let Some(user_password) = state.users.get(username) {
            let hash = Sha256::digest(password);
            let password_hash = hex::encode(hash);
            if *user_password == password_hash {
                return true;
            }
        }

        false
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();
        let mut notify = false;
        let expires_at = expire.map(|duration| {
            let when = Instant::now() + duration;
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            when
        });

        let prev = state.entries.insert(
            key.clone(),
            Entry {
                data: value,
                expires_at,
            },
        );
        if let Some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, key.clone()));
            }
        }

        if let Some(when) = expires_at {
            state.expirations.insert((when, key.clone()));
        }
        drop(state);
        if notify {
            self.shared.background_task.notify_one();
        }
    }

    pub fn subscribe(&self, channel: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();
        match state.pub_sub.entry(channel) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    pub fn publish(&self, channel: String, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();
        state
            .pub_sub
            .get(&channel)
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }

    fn shutdown_purge_task(&self) {
        let mut state = self.shared.state.lock().unwrap();
        state.shutdown = true;

        drop(state);
        self.shared.background_task.notify_one();
    }
}

impl Drop for Store {
    fn drop(&mut self) {
        self.db.shutdown_purge_task();
    }
}
impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}
impl Store {
    pub fn new() -> Self {
        Store { db: Db::new() }
    }
}

impl Default for Store {
    fn default() -> Self {
        Store::new()
    }
}

impl State {
    pub fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .iter()
            .next()
            .map(|expiration| expiration.0)
    }
}

async fn pure_expire_task(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.pure_expire_keys() {
            tokio::select! {
                _= time::sleep_until(when)=> {},
                _= shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
