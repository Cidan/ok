//! File handles: which export, which file, and a keyed hash of both, so a handle is good only if
//! this server made it. A process that was never given a handle cannot make one; the kernel's NFS
//! client is given them, from the export's root down.

/// A handle's length: the export (8 bytes), the file (8), and the hash (16).
pub const LEN: usize = 32;

/// Makes and checks handles under one key.
pub struct Handles {
    key: [u8; 32],
}

impl Handles {
    pub fn new(key: [u8; 32]) -> Self {
        Self { key }
    }

    fn mac(&self, export: u64, ino: u64) -> [u8; 16] {
        let mut msg = [0u8; 16];
        msg[..8].copy_from_slice(&export.to_be_bytes());
        msg[8..].copy_from_slice(&ino.to_be_bytes());
        let h = blake3::keyed_hash(&self.key, &msg);
        h.as_bytes()[..16].try_into().unwrap()
    }

    /// The handle of file `ino` in `export`.
    pub fn make(&self, export: u64, ino: u64) -> [u8; LEN] {
        let mut fh = [0u8; LEN];
        fh[..8].copy_from_slice(&export.to_be_bytes());
        fh[8..16].copy_from_slice(&ino.to_be_bytes());
        fh[16..].copy_from_slice(&self.mac(export, ino));
        fh
    }

    /// The export and file a handle names, if this server made it.
    pub fn open(&self, fh: &[u8]) -> Option<(u64, u64)> {
        if fh.len() != LEN {
            return None;
        }
        let export = u64::from_be_bytes(fh[..8].try_into().unwrap());
        let ino = u64::from_be_bytes(fh[8..16].try_into().unwrap());
        let want = self.mac(export, ino);
        // Every byte compared, whatever differs first.
        let diff = want
            .iter()
            .zip(&fh[16..])
            .fold(0u8, |acc, (a, b)| acc | (a ^ b));
        (diff == 0).then_some((export, ino))
    }
}
