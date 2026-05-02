package mqtt

import (
	"bytes"
	"sync"
)

// pooled bytes.Buffer for encoding to reduce allocations on the hot path.

var bufPool = sync.Pool{
	New: func() any { return &pooledBuf{b: new(bytes.Buffer)} },
}

type pooledBuf struct {
	b *bytes.Buffer
}

func newBuf() *pooledBuf {
	return bufPool.Get().(*pooledBuf)
}

func (p *pooledBuf) Reset() {
	p.b.Reset()
	bufPool.Put(p)
}
