package server

import "sync"

const (
	defaultPooledBufferCap = 256
	maxPooledBufferCap     = 1 << 20 // 1MiB cap before resetting to default
)

// pooledBuffer is a grow-only byte accumulator backed by a sync.Pool.
// It implements io.Writer so it can be used wherever a bytes.Buffer would.
type pooledBuffer struct {
	buf []byte
}

func (b *pooledBuffer) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

func (b *pooledBuffer) WriteString(s string) (int, error) {
	b.buf = append(b.buf, s...)
	return len(s), nil
}

func (b *pooledBuffer) Bytes() []byte { return b.buf }

func (b *pooledBuffer) Reset() {
	if len(b.buf) > 0 {
		b.buf = b.buf[:0]
	}
}

func (b *pooledBuffer) Len() int { return len(b.buf) }

func (b *pooledBuffer) Truncate(n int) {
	if n < 0 || n > len(b.buf) {
		panic("pooledBuffer: truncate out of bounds")
	}
	b.buf = b.buf[:n]
}

func (b *pooledBuffer) WriteSlice(n int) []byte {
	if n < 0 {
		panic("pooledBuffer: negative slice size")
	}
	l := len(b.buf)
	b.ensureCapacity(l + n)
	b.buf = b.buf[:l+n]
	return b.buf[l:]
}

func (b *pooledBuffer) ensureCapacity(total int) {
	if total < 0 {
		panic("pooledBuffer: negative capacity request")
	}
	if cap(b.buf) >= total {
		return
	}
	newCap := growPooledCapacity(cap(b.buf), total)
	newBuf := make([]byte, len(b.buf), newCap)
	copy(newBuf, b.buf)
	b.buf = newBuf
}

func growPooledCapacity(current, required int) int {
	if required < 0 {
		panic("pooledBuffer: negative capacity request")
	}
	if required <= current {
		return current
	}
	if current == 0 {
		current = 1
	}
	newCap := current
	for newCap < required {
		newCap *= 2
		if newCap <= 0 {
			return required
		}
	}
	return newCap
}

var pooledBufferPool = sync.Pool{
	New: func() any {
		return &pooledBuffer{buf: make([]byte, 0, defaultPooledBufferCap)}
	},
}

func getPooledBuffer() *pooledBuffer {
	return pooledBufferPool.Get().(*pooledBuffer)
}

func putPooledBuffer(buf *pooledBuffer) {
	if buf == nil {
		return
	}
	if cap(buf.buf) > maxPooledBufferCap {
		buf.buf = make([]byte, 0, defaultPooledBufferCap)
	} else {
		buf.Reset()
	}
	pooledBufferPool.Put(buf)
}
