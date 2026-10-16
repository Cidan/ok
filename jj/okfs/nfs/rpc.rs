//! ONC RPC version 2 (RFC 5531) over TCP: record marking, calls, and replies. Only the
//! credentials NFS clients send are understood — AUTH_SYS, and AUTH_NONE for NULL pings.

use std::io;
use std::io::Read;
use std::io::Write;

use crate::xdr;
use crate::xdr::Reader;
use crate::xdr::Writer;

const CALL: u32 = 0;
const REPLY: u32 = 1;
const RPC_VERSION: u32 = 2;

const AUTH_NONE: u32 = 0;
const AUTH_SYS: u32 = 1;

const MSG_ACCEPTED: u32 = 0;
const MSG_DENIED: u32 = 1;

/// An accepted call's outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Accept {
    Success = 0,
    ProgUnavail = 1,
    ProgMismatch = 2,
    ProcUnavail = 3,
    GarbageArgs = 4,
    SystemErr = 5,
}

/// Why a call's credential was refused (`auth_stat`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthStat {
    BadCred = 1,
    TooWeak = 5,
}

/// Who a call says it is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cred {
    None,
    Sys {
        uid: u32,
        gid: u32,
        gids: Vec<u32>,
    },
    /// A flavor this server does not understand.
    Other(u32),
}

/// A call, its arguments still encoded.
pub struct Call<'a> {
    pub xid: u32,
    pub prog: u32,
    pub vers: u32,
    pub proc_: u32,
    pub cred: Cred,
    pub args: &'a [u8],
}

/// Why a record is no call this server can answer as one.
pub enum Unanswerable {
    /// Not even a transaction ID: the record is dropped.
    Garbage,
    /// A call of another RPC version, which is answered as such.
    RpcMismatch(u32),
    /// A call whose credential does not parse.
    BadCred(u32),
}

/// Parses a call from a record.
pub fn parse_call(record: &[u8]) -> Result<Call<'_>, Unanswerable> {
    let mut r = Reader::new(record);
    let xid = r.u32().map_err(|_| Unanswerable::Garbage)?;
    let header = (|| -> xdr::Result<(u32, u32)> { Ok((r.u32()?, r.u32()?)) })();
    let (mtype, rpcvers) = header.map_err(|_| Unanswerable::Garbage)?;
    if mtype != CALL {
        return Err(Unanswerable::Garbage);
    }
    if rpcvers != RPC_VERSION {
        return Err(Unanswerable::RpcMismatch(xid));
    }
    let (prog, vers, proc_) =
        (|| -> xdr::Result<(u32, u32, u32)> { Ok((r.u32()?, r.u32()?, r.u32()?)) })()
            .map_err(|_| Unanswerable::Garbage)?;
    let cred = (|| -> xdr::Result<Cred> {
        let flavor = r.u32()?;
        let body = r.opaque(400)?;
        // The verifier, which neither flavor understood here uses.
        r.u32()?;
        r.opaque(400)?;
        Ok(match flavor {
            AUTH_NONE => Cred::None,
            AUTH_SYS => {
                let mut b = Reader::new(body);
                b.u32()?; // stamp
                b.opaque(255)?; // machine name
                let uid = b.u32()?;
                let gid = b.u32()?;
                let n = b.u32()?;
                if n > 16 {
                    return Err(xdr::Malformed);
                }
                let gids = (0..n).map(|_| b.u32()).collect::<xdr::Result<_>>()?;
                Cred::Sys { uid, gid, gids }
            }
            other => Cred::Other(other),
        })
    })()
    .map_err(|_| Unanswerable::BadCred(xid))?;
    Ok(Call {
        xid,
        prog,
        vers,
        proc_,
        cred,
        args: r.rest(),
    })
}

fn reply_header(xid: u32) -> Writer {
    let mut w = Writer::new();
    w.u32(xid).u32(REPLY);
    w
}

/// An accepted call's reply: `stat`, then `body` (the results, or a mismatch's version range).
pub fn accepted(xid: u32, stat: Accept, body: &[u8]) -> Vec<u8> {
    let mut w = reply_header(xid);
    w.u32(MSG_ACCEPTED)
        .u32(AUTH_NONE)
        .opaque(&[])
        .u32(stat as u32);
    let mut out = w.into_vec();
    out.extend_from_slice(body);
    out
}

/// A program's version range, for `ProgMismatch`.
pub fn versions(low: u32, high: u32) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(low).u32(high);
    w.into_vec()
}

/// A refused call's reply: its credential.
pub fn auth_error(xid: u32, stat: AuthStat) -> Vec<u8> {
    let mut w = reply_header(xid);
    w.u32(MSG_DENIED).u32(1).u32(stat as u32);
    w.into_vec()
}

/// A refused call's reply: an RPC version other than 2.
pub fn rpc_mismatch(xid: u32) -> Vec<u8> {
    let mut w = reply_header(xid);
    w.u32(MSG_DENIED).u32(0).u32(RPC_VERSION).u32(RPC_VERSION);
    w.into_vec()
}

/// Reads one record — its fragments, joined — of at most `max` bytes. None at a clean end of the
/// stream, before a record began.
pub fn read_record(r: &mut impl Read, max: usize) -> io::Result<Option<Vec<u8>>> {
    let mut record = Vec::new();
    let mut first = true;
    loop {
        let mut header = [0u8; 4];
        if first {
            let mut got = 0;
            while got < 4 {
                match r.read(&mut header[got..])? {
                    0 if got == 0 => return Ok(None),
                    0 => return Err(io::ErrorKind::UnexpectedEof.into()),
                    n => got += n,
                }
            }
        } else {
            r.read_exact(&mut header)?;
        }
        first = false;
        let h = u32::from_be_bytes(header);
        let last = h & 0x8000_0000 != 0;
        let len = (h & 0x7fff_ffff) as usize;
        if record.len() + len > max {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("an RPC record over {max} bytes"),
            ));
        }
        let at = record.len();
        record.resize(at + len, 0);
        r.read_exact(&mut record[at..])?;
        if last {
            return Ok(Some(record));
        }
    }
}

/// Writes one record, as a single fragment.
pub fn write_record(w: &mut impl Write, record: &[u8]) -> io::Result<()> {
    let header = (record.len() as u32 | 0x8000_0000).to_be_bytes();
    let mut out = Vec::with_capacity(4 + record.len());
    out.extend_from_slice(&header);
    out.extend_from_slice(record);
    w.write_all(&out)
}

/// A call's record, as a client sends it (the test client's).
pub fn call(xid: u32, prog: u32, vers: u32, proc_: u32, cred: &Cred, args: &[u8]) -> Vec<u8> {
    let mut w = Writer::new();
    w.u32(xid)
        .u32(CALL)
        .u32(RPC_VERSION)
        .u32(prog)
        .u32(vers)
        .u32(proc_);
    match cred {
        Cred::Sys { uid, gid, gids } => {
            let mut b = Writer::new();
            b.u32(0).opaque(b"localhost").u32(*uid).u32(*gid);
            b.u32(gids.len() as u32);
            for g in gids {
                b.u32(*g);
            }
            w.u32(AUTH_SYS).opaque(&b.into_vec());
        }
        Cred::None => {
            w.u32(AUTH_NONE).opaque(&[]);
        }
        Cred::Other(f) => {
            w.u32(*f).opaque(&[]);
        }
    }
    w.u32(AUTH_NONE).opaque(&[]);
    let mut out = w.into_vec();
    out.extend_from_slice(args);
    out
}

/// A reply's outcome, as a client reads it: the accepted call's status and its results, or
/// why it was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum Reply {
    Accepted(Accept, Vec<u8>),
    Denied,
}

/// Parses a reply's transaction ID and outcome (the test client's).
pub fn parse_reply(record: &[u8]) -> xdr::Result<(u32, Reply)> {
    let mut r = Reader::new(record);
    let xid = r.u32()?;
    if r.u32()? != REPLY {
        return Err(xdr::Malformed);
    }
    match r.u32()? {
        MSG_ACCEPTED => {
            r.u32()?;
            r.opaque(400)?;
            let stat = match r.u32()? {
                0 => Accept::Success,
                1 => Accept::ProgUnavail,
                2 => Accept::ProgMismatch,
                3 => Accept::ProcUnavail,
                4 => Accept::GarbageArgs,
                5 => Accept::SystemErr,
                _ => return Err(xdr::Malformed),
            };
            Ok((xid, Reply::Accepted(stat, r.rest().to_vec())))
        }
        MSG_DENIED => Ok((xid, Reply::Denied)),
        _ => Err(xdr::Malformed),
    }
}
