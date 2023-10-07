use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

struct StorageServer {
    partitions: Vec<Partition>,
}

struct Partition {
    data: HashMap<String, String>,
}

impl StorageServer {
    fn new(num_partitions: usize) -> Self {
        let mut partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            partitions.push(Partition {
                data: HashMap::new(),
            });
        }
        Self { partitions }
    }

    fn get_partition(&mut self, key: &str) -> &mut Partition {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let partition_index = hasher.finish() as usize % self.partitions.len();
        &mut self.partitions[partition_index]
    }

    fn get(&mut self, key: &str) -> Option<&str> {
        self.get_partition(key).data.get(key).map(|s| s.as_str())
    }

    fn put(&mut self, key: String, value: String) {
        let partition = self.get_partition(&key);
        partition.data.insert(key, value);
    }
}

fn main() {
    println!("Hello, world!");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut server = StorageServer::new(4);
        server.put("foo".to_string(), "bar".to_string());
        assert_eq!(server.get("foo"), Some("bar"));
    }

    #[test]
    fn test_put_overwrite() {
        let mut server = StorageServer::new(4);
        server.put("foo".to_string(), "bar".to_string());
        server.put("foo".to_string(), "baz".to_string());
        assert_eq!(server.get("foo"), Some("baz"));
    }

    #[test]
    fn test_get_nonexistent() {
        let mut server = StorageServer::new(4);
        assert_eq!(server.get("foo"), None);
    }
}