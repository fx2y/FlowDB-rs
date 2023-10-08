use std::fs::File;
use std::io::{BufWriter, Result, Write};

/// A transaction log that writes data to a file.
pub struct TransactionLog {
    file: BufWriter<File>,
}

impl TransactionLog {
    /// Creates a new transaction log that writes to the specified file path.
    pub fn new(path: &str) -> Result<Self> {
        let file = BufWriter::new(File::create(path)?);
        Ok(Self { file })
    }

    /// Writes the given data to the transaction log.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        self.file.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let log = TransactionLog::new("test.log").unwrap();
        assert!(log.file.get_ref().metadata().unwrap().len() == 0);
    }

    #[test]
    fn test_write() {
        let mut log = TransactionLog::new("test.log").unwrap();
        log.write(b"Hello, world!").unwrap();
        assert!(log.file.get_ref().metadata().unwrap().len() == 13);
    }
}