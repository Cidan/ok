// Package membuf is bytes held in memory, grown a block at a time: written to, read back at any
// offset, and read whole, without the copies a growing slice makes as it doubles.
package membuf

import (
	"errors"
	"io"
)

const (
	// firstBlock is the first block's size; each next is twice the last, up to maxBlock, so a few
	// bytes take a few kilobytes and a large buffer whole blocks.
	firstBlock = 4 << 10
	maxBlock   = 1 << 20
)

// Buffer is bytes written in order. The zero value is empty and ready.
type Buffer struct {
	blocks [][]byte
	starts []int64 // the offset of each block's first byte
	n      int64
}

// Len is how many bytes were written.
func (b *Buffer) Len() int64 { return b.n }

// Write appends p.
func (b *Buffer) Write(p []byte) (int, error) {
	written := len(p)
	for len(p) > 0 {
		last := len(b.blocks) - 1
		if last < 0 || len(b.blocks[last]) == cap(b.blocks[last]) {
			size := firstBlock
			if last >= 0 {
				size = min(2*cap(b.blocks[last]), maxBlock)
			}
			b.blocks = append(b.blocks, make([]byte, 0, size))
			b.starts = append(b.starts, b.n)
			last++
		}
		blk := b.blocks[last]
		n := min(len(p), cap(blk)-len(blk))
		b.blocks[last] = append(blk, p[:n]...)
		b.n += int64(n)
		p = p[n:]
	}
	return written, nil
}

// ReadAt reads len(p) bytes from off; past the end it reads what there is, and io.EOF.
func (b *Buffer) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("membuf: a negative offset")
	}
	if off >= b.n {
		if len(p) == 0 {
			return 0, nil
		}
		return 0, io.EOF
	}
	// The block holding off: the last whose start is at or before it.
	lo, hi := 0, len(b.starts)
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if b.starts[mid] <= off {
			lo = mid
		} else {
			hi = mid
		}
	}
	read := 0
	for i := lo; i < len(b.blocks) && read < len(p); i++ {
		from := int(off - b.starts[i])
		if from < 0 {
			from = 0
		}
		n := copy(p[read:], b.blocks[i][from:])
		read += n
		off += int64(n)
	}
	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
}

// Reader reads the buffer from its start, as it is when Reader is called.
func (b *Buffer) Reader() io.Reader { return io.NewSectionReader(b, 0, b.n) }

// Reset empties the buffer, letting its memory go.
func (b *Buffer) Reset() { *b = Buffer{} }
