use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};
use snap::raw::Encoder as SnapEncoder;
use snap::raw::Decoder as SnapDecoder;

struct StorageServer {
    partitions: Vec<Arc<RwLock<Partition>>>,
    replicas: usize,
}

#[derive(Debug)]
struct Partition {
    data: HashMap<String, Vec<u8>>,
    replicas: Vec<Arc<RwLock<Partition>>>,
}

impl StorageServer {
    fn new(num_partitions: usize, num_replicas: usize) -> Self {
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let mut replicas = Vec::with_capacity(num_replicas);
            for _ in 0..num_replicas {
                replicas.push(Arc::new(RwLock::new(Partition {
                    data: HashMap::new(),
                    replicas: Vec::with_capacity(num_replicas),
                })));
            }
            partitions.push(Arc::clone(&replicas[0]));
            for replica in replicas.iter() {
                let mut replica_guard = match replica.write() {
                    Ok(guard) => guard,
                    Err(poisoned) => poisoned.into_inner(),
                };
                replica_guard.replicas = replicas.clone();
            }
        }
        Self { partitions, replicas: num_replicas }
    }

    fn get_partition(&self, key: &str) -> Arc<RwLock<Partition>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let partition_index = hasher.finish() as usize % self.partitions.len();
        self.partitions[partition_index].clone()
    }

    /// Returns the value associated with the given key, or an error if the key is not found.
    fn get(&self, key: &str) -> Result<String, ()> {
        // Determine which partition the key belongs to.
        let partition = self.get_partition(key);

        // Acquire a read lock on the partition to ensure exclusive access.
        let partition_guard = partition.read().unwrap();

        // Look up the key in the partition data.
        let compressed_data = match partition_guard.data.get(key) {
            Some(data) => data,
            None => return Err(()),
        };
        let data = decompress(compressed_data).ok_or(())?;
        let value = String::from_utf8(data).map_err(|_| ())?;
        Ok(value)
    }

    /// Inserts a key-value pair into the partition and its replicas.
    fn put(&self, key: &str, value: &str) -> Result<(), ()> {
        let data = compress(value.as_bytes());

        // Determine which partition the key belongs to.
        let partition = self.get_partition(key);

        // Acquire a lock on the partition to ensure exclusive access.
        let mut partition_guard = partition.write().unwrap();

        // Insert the key-value pair into the primary partition.
        partition_guard.data.insert(key.to_owned(), data.to_owned());

        // Insert the key-value pair into the replica partitions.
        for replica in partition_guard.replicas.iter().skip(1) {
            let mut replica_guard = replica.write().unwrap();
            replica_guard.data.insert(key.to_owned(), data.to_owned());
        }

        // Return success.
        Ok(())
    }
}

fn compress(data: &[u8]) -> Vec<u8> {
    SnapEncoder::new().compress_vec(data).unwrap()
}

fn decompress(data: &[u8]) -> Option<Vec<u8>> {
    SnapDecoder::new().decompress_vec(data).ok()
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_storage_server() {
        let num_partitions = 3;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        assert_eq!(storage_server.partitions.len(), num_partitions);
        assert_eq!(storage_server.replicas, num_replicas);
    }

    #[test]
    fn test_get_partition() {
        let num_partitions = 3;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        let partition = storage_server.get_partition("test_key");
        assert!(partition.read().unwrap().data.is_empty());
    }

    #[test]
    fn test_get() {
        let num_partitions = 3;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        let key = "test_key";
        let value = "test_value";
        let compressed_value = compress(value.as_bytes());
        let partition = storage_server.get_partition(key);
        let mut partition_guard = partition.write().unwrap();
        partition_guard.data.insert(key.to_owned(), compressed_value.to_owned());
        drop(partition_guard); // Release the lock early.
        let result = storage_server.get(key);
        assert_eq!(result, Ok(value.to_owned()));
    }

    #[test]
    fn test_put_replicas() {
        let num_partitions = 4;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        let key = "test_key";
        let value = "test_value";
        let compressed_value = compress(value.as_bytes());
        let result = storage_server.put(key, value);
        assert_eq!(result, Ok(()));
        for i in 1..num_replicas {
            let partition = storage_server.get_partition(key);
            let replica = &partition.write().unwrap().replicas[i];
            let replica_guard = replica.read().unwrap();
            assert_eq!(replica_guard.data.get(key), Some(&compressed_value));
        }
    }

    #[test]
    fn test_put_existing_key() {
        let num_partitions = 4;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        let key = "test_key";
        let value1 = "test_value1";
        let value2 = "test_value2";
        let result = storage_server.put(key, value1);
        assert_eq!(result, Ok(()));
        let result = storage_server.put(key, value2);
        assert_eq!(result, Ok(()));
        let result = storage_server.get(key);
        assert_eq!(result, Ok(value2.to_owned()));
    }

    #[test]
    fn test_get_nonexistent_key() {
        let num_partitions = 4;
        let num_replicas = 2;
        let storage_server = StorageServer::new(num_partitions, num_replicas);
        let key = "test_key".to_string();
        let result = storage_server.get(&key.clone());
        assert_eq!(result, Err(()));
    }

    #[test]
    fn test_compress() {
        let data = b"hello world";
        let compressed_data = compress(data);
        assert_ne!(data.as_slice(), compressed_data);
    }

    #[test]
    fn test_decompress() {
        let data = b"hello world";
        let compressed_data = compress(data);
        let decompressed_data = decompress(&compressed_data).unwrap();
        assert_eq!(data, &decompressed_data[..]);
    }
}