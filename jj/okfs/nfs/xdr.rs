//! XDR (RFC 4506) as ONC RPC and NFSv3 use it: big-endian four-byte units, variable-length data
//! preceded by its length and padded to a multiple of four bytes.

/// The data ended early, or held a value XDR does not allow there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Malformed;

pub type Result<T> = std::result::Result<T, Malformed>;

/// Reads XDR from a buffer.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or(Malformed)?;
        let s = self.buf.get(self.pos..end).ok_or(Malformed)?;
        self.pos = end;
        Ok(s)
    }

    pub fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    pub fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    pub fn bool(&mut self) -> Result<bool> {
        match self.u32()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Malformed),
        }
    }

    /// Fixed-length opaque data of `n` bytes, and its padding.
    pub fn fixed(&mut self, n: usize) -> Result<&'a [u8]> {
        let data = self.take(n)?;
        self.take(pad(n))?;
        Ok(data)
    }

    /// Variable-length opaque data (or a string) of at most `max` bytes.
    pub fn opaque(&mut self, max: usize) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(Malformed);
        }
        self.fixed(n)
    }

    /// What is left unread.
    pub fn rest(&self) -> &'a [u8] {
        &self.buf[self.pos..]
    }
}

fn pad(n: usize) -> usize {
    (4 - n % 4) % 4
}

/// Writes XDR to a buffer.
#[derive(Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.buf.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u32(v as u32)
    }

    /// Fixed-length opaque data, and its padding.
    pub fn fixed(&mut self, data: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(data);
        self.buf.extend_from_slice(&[0; 3][..pad(data.len())]);
        self
    }

    /// Variable-length opaque data (or a string): its length, then the data.
    pub fn opaque(&mut self, data: &[u8]) -> &mut Self {
        self.u32(data.len() as u32);
        self.fixed(data)
    }

    /// Bytes already encoded, as they are.
    pub fn raw(&mut self, encoded: &[u8]) -> &mut Self {
        self.buf.extend_from_slice(encoded);
        self
    }

    /// Bytes written so far.
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.buf
    }
}

/// The bytes `data` takes as variable-length opaque data.
pub fn opaque_len(data: &[u8]) -> usize {
    4 + data.len() + pad(data.len())
}
