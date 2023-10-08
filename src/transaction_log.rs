use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Result, Write};
use std::path::PathBuf;

/// A transaction log that writes data to a file.
pub struct TransactionLog {
    file: BufWriter<File>,
    path: PathBuf,
    max_size: u64,
    max_files: u32,
}

impl TransactionLog {
    /// Creates a new transaction log that writes to the specified file path.
    pub fn new(path: &str, max_size: u64, max_files: u32) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let file_size = file.metadata()?.len();
        let file = BufWriter::with_capacity(8192, file);
        let path = PathBuf::from(path);
        Ok(Self { file, path, max_size, max_files })
    }

    /// Writes the given data to the transaction log.
    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.file.write_all(data)?;
        if self.file.buffer().len() as u64 >= self.max_size {
            self.rotate()?;
        }
        self.file.flush()?;
        Ok(())
    }

    /// Rotates the transaction log by renaming the current log file to .1 and creating a new one.
    fn rotate(&mut self) -> Result<()> {

        // Create a backup path by cloning the current path and changing the extension to .1.
        let mut backup_path = self.path.clone();
        backup_path.set_extension("log.bak");

        // Rename the current log file to the backup path.
        fs::rename(&self.path, &backup_path)?;

        // Create a new file with the same name as the original log file and open it for appending.
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        // Write a newline character to the new file.
        file.write_all(b"\n")?;

        // Create a new buffered writer with a capacity of 8192 bytes and set it as the current file.
        self.file = BufWriter::with_capacity(8192, file);

        // Clean up any old backup files.
        self.cleanup()?;

        // Return Ok if everything succeeded.
        Ok(())
    }

    /// Cleans up old backup files by deleting any files that exceed the maximum number of allowed files.
    fn cleanup(&self) -> Result<()> {

        // Read the directory containing the log file and filter out any non-file entries.
        let mut files = fs::read_dir(self.path.parent().unwrap())?
            .filter_map(|entry| entry.ok())
            .filter(|entry| {

                // Filter out any files that don't have the .log extension.
                entry.file_type().ok().map(|ft| ft.is_file()).unwrap_or(false) &&
                    entry.path().extension().map(|ext| ext == "log").unwrap_or(false) &&

                    // Filter out any files that don't have a numeric stem.
                    entry.path().file_stem().and_then(|stem| stem.to_str().and_then(|s| s.parse::<u32>().ok())).is_some()
            })
            .collect::<Vec<_>>();

        // Sort the files by path and remove the oldest files until the number of files is within the maximum allowed.
        files.sort_by_key(|entry| entry.path());
        while files.len() > self.max_files as usize {
            let entry = files.remove(0);
            fs::remove_file(entry.path())?;
        }

        // Return Ok if everything succeeded.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let log = TransactionLog::new("logs/test_new.log", 1024, 5).unwrap();
        assert_eq!(log.max_size, 1024);
        assert_eq!(log.max_files, 5);
        assert_eq!(log.file.get_ref().metadata().unwrap().len(), 0);
    }

    #[test]
    fn test_write() {
        let mut log = TransactionLog::new("logs/test_write.log", 15, 5).unwrap();
        log.write(b"Hello, world!").unwrap();
        println!("{}", log.file.get_ref().metadata().unwrap().len());
        assert_eq!(log.file.get_ref().metadata().unwrap().len(), 13);

        // Write more than max_size bytes to trigger rotation
        let data = vec![b'x'; 20];
        log.write(&data).unwrap();
        assert_eq!(log.file.get_ref().metadata().unwrap().len(), 1);
    }
}