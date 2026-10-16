//! A minimal NFSv3 and MOUNT client over one connection: how tests call a server as the kernel's
//! NFS client would, without mounting anything (which needs a kernel client, and on Linux root).

use std::io;
use std::net::SocketAddr;
use std::net::TcpStream;

use crate::Attr;
use crate::MOUNT_PROGRAM;
use crate::NFS_PROGRAM;
use crate::SetAttr;
use crate::nfs3;
use crate::rpc;
use crate::rpc::Accept;
use crate::rpc::Cred;
use crate::rpc::Reply;
use crate::xdr;
use crate::xdr::Reader;
use crate::xdr::Writer;

/// An NFS status other than OK, as a call failed with it.
pub type Status = u32;

/// What LOOKUP found: its handle, and its attributes if they came.
pub type Found = (Vec<u8>, Option<Attr>);

/// A directory's entries as READDIR lists them: each name and file.
pub type Names = Vec<(Vec<u8>, u64)>;

/// An entry READDIRPLUS lists.
#[derive(Clone, Debug)]
pub struct PlusEntry {
    pub name: Vec<u8>,
    pub fileid: u64,
    pub attr: Option<Attr>,
    pub fh: Option<Vec<u8>>,
}

pub struct Client {
    conn: TcpStream,
    xid: u32,
    cred: Cred,
}

fn bad(_: xdr::Malformed) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "a reply that does not parse")
}

impl Client {
    /// Connects as user `uid` (AUTH_SYS).
    pub fn connect(addr: SocketAddr, uid: u32, gid: u32) -> io::Result<Self> {
        Self::with_cred(
            addr,
            Cred::Sys {
                uid,
                gid,
                gids: Vec::new(),
            },
        )
    }

    pub fn with_cred(addr: SocketAddr, cred: Cred) -> io::Result<Self> {
        let conn = TcpStream::connect(addr)?;
        conn.set_nodelay(true)?;
        Ok(Self { conn, xid: 1, cred })
    }

    /// Calls a procedure and reads its reply.
    pub fn call(&mut self, prog: u32, vers: u32, proc_: u32, args: &[u8]) -> io::Result<Reply> {
        self.xid += 1;
        let record = rpc::call(self.xid, prog, vers, proc_, &self.cred, args);
        rpc::write_record(&mut self.conn, &record)?;
        let reply = rpc::read_record(&mut self.conn, 64 << 20)?
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        let (xid, reply) = rpc::parse_reply(&reply).map_err(bad)?;
        if xid != self.xid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "a reply to another call",
            ));
        }
        Ok(reply)
    }

    /// An NFS procedure's results, and the status they begin with.
    fn nfs(&mut self, proc_: u32, args: Writer) -> io::Result<(u32, Vec<u8>)> {
        match self.call(NFS_PROGRAM, 3, proc_, &args.into_vec())? {
            Reply::Accepted(Accept::Success, body) => {
                let mut r = Reader::new(&body);
                let status = r.u32().map_err(bad)?;
                Ok((status, r.rest().to_vec()))
            }
            other => Err(io::Error::other(format!(
                "procedure {proc_} answered {other:?}"
            ))),
        }
    }

    /// MNT: the root handle of the export `path` mounts.
    pub fn mnt(&mut self, path: &[u8]) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(path);
        match self.call(MOUNT_PROGRAM, 3, nfs3::MNT, &w.into_vec())? {
            Reply::Accepted(Accept::Success, body) => {
                let mut r = Reader::new(&body);
                match r.u32().map_err(bad)? {
                    0 => Ok(Ok(r.opaque(64).map_err(bad)?.to_vec())),
                    s => Ok(Err(s)),
                }
            }
            other => Err(io::Error::other(format!("MNT answered {other:?}"))),
        }
    }

    pub fn getattr(&mut self, fh: &[u8]) -> io::Result<Result<Attr, Status>> {
        let mut w = Writer::new();
        w.opaque(fh);
        let (st, body) = self.nfs(nfs3::GETATTR, w)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        Ok(Ok(nfs3::read_fattr(&mut Reader::new(&body)).map_err(bad)?))
    }

    pub fn setattr(&mut self, fh: &[u8], set: &SetAttr) -> io::Result<Result<nfs3::Wcc, Status>> {
        let mut w = Writer::new();
        w.opaque(fh);
        nfs3::write_sattr(&mut w, set);
        w.bool(false);
        let (st, body) = self.nfs(nfs3::SETATTR, w)?;
        let wcc = nfs3::read_wcc(&mut Reader::new(&body)).map_err(bad)?;
        Ok(if st == nfs3::OK { Ok(wcc) } else { Err(st) })
    }

    pub fn lookup(&mut self, dir: &[u8], name: &[u8]) -> io::Result<Result<Found, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name);
        let (st, body) = self.nfs(nfs3::LOOKUP, w)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        let mut r = Reader::new(&body);
        let fh = r.opaque(64).map_err(bad)?.to_vec();
        let attr = nfs3::read_post_attr(&mut r).map_err(bad)?;
        Ok(Ok((fh, attr)))
    }

    pub fn access(&mut self, fh: &[u8], mask: u32) -> io::Result<Result<u32, Status>> {
        let mut w = Writer::new();
        w.opaque(fh).u32(mask);
        let (st, body) = self.nfs(nfs3::ACCESS, w)?;
        let mut r = Reader::new(&body);
        nfs3::read_post_attr(&mut r).map_err(bad)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        Ok(Ok(r.u32().map_err(bad)?))
    }

    pub fn readlink(&mut self, fh: &[u8]) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(fh);
        let (st, body) = self.nfs(nfs3::READLINK, w)?;
        let mut r = Reader::new(&body);
        nfs3::read_post_attr(&mut r).map_err(bad)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        Ok(Ok(r.opaque(4096).map_err(bad)?.to_vec()))
    }

    /// READ: the data, and whether it reached the end.
    pub fn read(
        &mut self,
        fh: &[u8],
        offset: u64,
        count: u32,
    ) -> io::Result<Result<(Vec<u8>, bool), Status>> {
        let mut w = Writer::new();
        w.opaque(fh).u64(offset).u32(count);
        let (st, body) = self.nfs(nfs3::READ, w)?;
        let mut r = Reader::new(&body);
        nfs3::read_post_attr(&mut r).map_err(bad)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        r.u32().map_err(bad)?;
        let eof = r.bool().map_err(bad)?;
        Ok(Ok((
            r.opaque(crate::MAX_IO as usize).map_err(bad)?.to_vec(),
            eof,
        )))
    }

    /// WRITE, FILE_SYNC: the bytes written, and the change's before and after.
    pub fn write(
        &mut self,
        fh: &[u8],
        offset: u64,
        data: &[u8],
    ) -> io::Result<Result<(u32, nfs3::Wcc), Status>> {
        let mut w = Writer::new();
        w.opaque(fh)
            .u64(offset)
            .u32(data.len() as u32)
            .u32(2)
            .opaque(data);
        let (st, body) = self.nfs(nfs3::WRITE, w)?;
        let mut r = Reader::new(&body);
        let wcc = nfs3::read_wcc(&mut r).map_err(bad)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        Ok(Ok((r.u32().map_err(bad)?, wcc)))
    }

    /// The handle a creating procedure returned.
    fn made(&mut self, proc_: u32, w: Writer) -> io::Result<Result<Vec<u8>, Status>> {
        let (st, body) = self.nfs(proc_, w)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        let mut r = Reader::new(&body);
        if !r.bool().map_err(bad)? {
            return Err(io::Error::other("a create that returned no handle"));
        }
        Ok(Ok(r.opaque(64).map_err(bad)?.to_vec()))
    }

    /// CREATE, guarded (refused when the name exists).
    pub fn create(
        &mut self,
        dir: &[u8],
        name: &[u8],
        mode: u32,
    ) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name).u32(1);
        nfs3::write_sattr(
            &mut w,
            &SetAttr {
                mode: Some(mode),
                ..SetAttr::default()
            },
        );
        self.made(nfs3::CREATE, w)
    }

    /// CREATE, unchecked: an existing file is opened, the attributes applied.
    pub fn create_unchecked(
        &mut self,
        dir: &[u8],
        name: &[u8],
        set: &SetAttr,
    ) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name).u32(0);
        nfs3::write_sattr(&mut w, set);
        self.made(nfs3::CREATE, w)
    }

    /// CREATE, exclusive, with a verifier.
    pub fn create_exclusive(
        &mut self,
        dir: &[u8],
        name: &[u8],
        verf: [u8; 8],
    ) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name).u32(2).fixed(&verf);
        self.made(nfs3::CREATE, w)
    }

    pub fn mkdir(
        &mut self,
        dir: &[u8],
        name: &[u8],
        mode: u32,
    ) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name);
        nfs3::write_sattr(
            &mut w,
            &SetAttr {
                mode: Some(mode),
                ..SetAttr::default()
            },
        );
        self.made(nfs3::MKDIR, w)
    }

    pub fn symlink(
        &mut self,
        dir: &[u8],
        name: &[u8],
        target: &[u8],
    ) -> io::Result<Result<Vec<u8>, Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name);
        nfs3::write_sattr(&mut w, &SetAttr::default());
        w.opaque(target);
        self.made(nfs3::SYMLINK, w)
    }

    fn dir_op(&mut self, proc_: u32, w: Writer) -> io::Result<Result<(), Status>> {
        let (st, _) = self.nfs(proc_, w)?;
        Ok(if st == nfs3::OK { Ok(()) } else { Err(st) })
    }

    pub fn remove(&mut self, dir: &[u8], name: &[u8]) -> io::Result<Result<(), Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name);
        self.dir_op(nfs3::REMOVE, w)
    }

    pub fn rmdir(&mut self, dir: &[u8], name: &[u8]) -> io::Result<Result<(), Status>> {
        let mut w = Writer::new();
        w.opaque(dir).opaque(name);
        self.dir_op(nfs3::RMDIR, w)
    }

    pub fn rename(
        &mut self,
        from: &[u8],
        name: &[u8],
        to: &[u8],
        to_name: &[u8],
    ) -> io::Result<Result<(), Status>> {
        let mut w = Writer::new();
        w.opaque(from).opaque(name).opaque(to).opaque(to_name);
        self.dir_op(nfs3::RENAME, w)
    }

    pub fn link(&mut self, fh: &[u8], dir: &[u8], name: &[u8]) -> io::Result<Result<(), Status>> {
        let mut w = Writer::new();
        w.opaque(fh).opaque(dir).opaque(name);
        self.dir_op(nfs3::LINK, w)
    }

    /// Every entry of a directory, READDIR after READDIR of `count` bytes.
    pub fn readdir(&mut self, dir: &[u8], count: u32) -> io::Result<Result<Names, Status>> {
        let mut out = Vec::new();
        let mut cookie = 0;
        loop {
            let mut w = Writer::new();
            w.opaque(dir).u64(cookie).fixed(&[0; 8]).u32(count);
            let (st, body) = self.nfs(nfs3::READDIR, w)?;
            let mut r = Reader::new(&body);
            nfs3::read_post_attr(&mut r).map_err(bad)?;
            if st != nfs3::OK {
                return Ok(Err(st));
            }
            r.fixed(8).map_err(bad)?;
            while r.bool().map_err(bad)? {
                let fileid = r.u64().map_err(bad)?;
                let name = r.opaque(4096).map_err(bad)?.to_vec();
                cookie = r.u64().map_err(bad)?;
                out.push((name, fileid));
            }
            if r.bool().map_err(bad)? {
                return Ok(Ok(out));
            }
        }
    }

    /// Every entry of a directory, READDIRPLUS after READDIRPLUS of `maxcount` bytes.
    pub fn readdirplus(
        &mut self,
        dir: &[u8],
        maxcount: u32,
    ) -> io::Result<Result<Vec<PlusEntry>, Status>> {
        let mut out = Vec::new();
        let mut cookie = 0;
        loop {
            let mut w = Writer::new();
            w.opaque(dir)
                .u64(cookie)
                .fixed(&[0; 8])
                .u32(maxcount)
                .u32(maxcount);
            let (st, body) = self.nfs(nfs3::READDIRPLUS, w)?;
            let mut r = Reader::new(&body);
            nfs3::read_post_attr(&mut r).map_err(bad)?;
            if st != nfs3::OK {
                return Ok(Err(st));
            }
            r.fixed(8).map_err(bad)?;
            while r.bool().map_err(bad)? {
                let fileid = r.u64().map_err(bad)?;
                let name = r.opaque(4096).map_err(bad)?.to_vec();
                cookie = r.u64().map_err(bad)?;
                let attr = nfs3::read_post_attr(&mut r).map_err(bad)?;
                let fh = if r.bool().map_err(bad)? {
                    Some(r.opaque(64).map_err(bad)?.to_vec())
                } else {
                    None
                };
                out.push(PlusEntry {
                    name,
                    fileid,
                    attr,
                    fh,
                });
            }
            if r.bool().map_err(bad)? {
                return Ok(Ok(out));
            }
        }
    }

    /// FSINFO: rtmax and wtmax.
    pub fn fsinfo(&mut self, fh: &[u8]) -> io::Result<Result<(u32, u32), Status>> {
        let mut w = Writer::new();
        w.opaque(fh);
        let (st, body) = self.nfs(nfs3::FSINFO, w)?;
        let mut r = Reader::new(&body);
        nfs3::read_post_attr(&mut r).map_err(bad)?;
        if st != nfs3::OK {
            return Ok(Err(st));
        }
        let rtmax = r.u32().map_err(bad)?;
        r.u32().map_err(bad)?;
        r.u32().map_err(bad)?;
        Ok(Ok((rtmax, r.u32().map_err(bad)?)))
    }
}
