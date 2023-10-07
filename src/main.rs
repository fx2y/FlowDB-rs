use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

struct StorageServer {
    partitions: Vec<Arc<Mutex<Partition>>>,
}

#[derive(Debug)]
struct Partition {
    data: HashMap<String, String>,
}

impl StorageServer {
    fn new(num_partitions: usize) -> Self {
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitions.push(Arc::new(Mutex::new(Partition {
                data: HashMap::new(),
            })));
        }
        Self { partitions }
    }

    fn get_partition(&self, key: &str) -> Arc<Mutex<Partition>> {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let partition_index = hasher.finish() as usize % self.partitions.len();
        self.partitions[partition_index].clone()
    }

    fn get(&self, key: &str) -> Result<String, ()> {
        let partition = self.get_partition(key);
        let partition_guard = partition.lock().unwrap();
        partition_guard.data.get(key).cloned().ok_or(())
    }

    fn put(&self, key: String, value: String) -> Result<(), ()> {
        let partition = self.get_partition(&key);
        let mut partition_guard = partition.lock().unwrap();
        partition_guard.data.insert(key, value);
        Ok(())
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_storage_server() {
        let num_partitions = 4;
        let storage_server = StorageServer::new(num_partitions);
        assert_eq!(storage_server.partitions.len(), num_partitions);
    }

    #[test]
    fn test_get_partition() {
        let num_partitions = 4;
        let storage_server = StorageServer::new(num_partitions);
        let partition1 = storage_server.get_partition("key1");
        let partition2 = storage_server.get_partition("key3");
        assert_ne!(Arc::as_ptr(&partition1), Arc::as_ptr(&partition2));
    }

    #[test]
    fn test_put_and_get() {
        let num_partitions = 4;
        let storage_server = StorageServer::new(num_partitions);
        let key = "test_key".to_string();
        let value = "test_value".to_string();
        storage_server.put(key.clone(), value.clone()).unwrap();
        let result = storage_server.get(&key).unwrap();
        assert_eq!(result, value);
    }

    #[test]
    fn test_get_nonexistent_key() {
        let num_partitions = 4;
        let storage_server = StorageServer::new(num_partitions);
        let key = "nonexistent_key".to_string();
        let result = storage_server.get(&key);
        assert_eq!(result, Err(()));
    }
}